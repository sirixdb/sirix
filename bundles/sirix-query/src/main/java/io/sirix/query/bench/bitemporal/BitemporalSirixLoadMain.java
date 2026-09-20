package io.sirix.query.bench.bitemporal;

import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.bench.bitemporal.BitemporalSchema.Event;
import io.sirix.query.bench.bitemporal.BitemporalSchema.Tier;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Loads an SH1 stream with sparse interval edits into five revisioned Sirix resources. */
public final class BitemporalSirixLoadMain {

  private static final long DEFAULT_SIRIX_CAP = 6L << 30;
  private static final long DEFAULT_CAMPAIGN_CAP = 12L << 30;
  private static final long MIN_FREE_BYTES = 20L << 30;
  private static final Path REQUIRED_ROOT = Path.of("/var/tmp/sirix-bitemporal");

  private BitemporalSirixLoadMain() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: BitemporalSirixLoadMain <tier> <events.jsonl> <database-root>");
      System.exit(2);
      return;
    }
    final Tier tier = Tier.parse(args[0]);
    final Path eventsPath = requireCampaignPath(Path.of(args[1]), "event stream");
    final Path databaseRoot = requireCampaignPath(Path.of(args[2]), "database root");
    if (!Files.isRegularFile(eventsPath)) {
      throw new IllegalArgumentException("event stream does not exist: " + eventsPath);
    }
    final Path databasePath = databaseRoot.resolve(BitemporalSchema.DATABASE);
    if (Files.exists(databasePath)) {
      throw new IllegalArgumentException("refusing to overwrite existing Sirix database: " + databasePath);
    }
    Files.createDirectories(databaseRoot);
    requireCapacity(databaseRoot);

    final List<Event>[] epochs = readEvents(eventsPath, tier);
    final long started = System.nanoTime();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      createResources(database);
      final RelationState contracts = new RelationState(BitemporalSchema.CONTRACTS, tier.contracts());
      final RelationState products = new RelationState(BitemporalSchema.PRODUCTS, tier.products());
      final RelationState suppliers = new RelationState(BitemporalSchema.SUPPLIERS, tier.suppliers());

      loadInitial(database, contracts, filter(epochs[0], BitemporalSchema.CONTRACTS));
      loadInitial(database, products, filter(epochs[0], BitemporalSchema.PRODUCTS));
      loadInitial(database, suppliers, filter(epochs[0], BitemporalSchema.SUPPLIERS));
      loadMetadata(database, BitemporalSchema.EPOCHS, epochJson());
      loadMetadata(database, BitemporalSchema.DAYS, dayJson());
      reportPublication(databaseRoot, 0);

      for (int epoch = 1; epoch < BitemporalSchema.PUBLICATIONS; epoch++) {
        applyPublication(database, contracts, filter(epochs[epoch], BitemporalSchema.CONTRACTS), epoch);
        applyPublication(database, products, filter(epochs[epoch], BitemporalSchema.PRODUCTS), epoch);
        applyPublication(database, suppliers, filter(epochs[epoch], BitemporalSchema.SUPPLIERS), epoch);
        reportPublication(databaseRoot, epoch);
      }
    }

    sync();
    final Footprint footprint = footprint(databaseRoot);
    final double seconds = (System.nanoTime() - started) / 1e9;
    System.out.printf("SIRIX_LOAD tier=%s seconds=%.3f logical_bytes=%d allocated_bytes=%d db=%s%n", tier.label(),
        seconds, footprint.logicalBytes(), footprint.allocatedBytes(), databaseRoot);
  }

  private static Path requireCampaignPath(final Path path, final String label) throws IOException {
    final Path normalized = path.toAbsolutePath().normalize();
    if (!normalized.startsWith(REQUIRED_ROOT)) {
      throw new IllegalArgumentException(label + " must be under " + REQUIRED_ROOT + ": " + normalized);
    }
    return normalized;
  }

  @SuppressWarnings("unchecked")
  private static List<Event>[] readEvents(final Path path, final Tier tier) throws IOException {
    final List<Event>[] epochs = new List[BitemporalSchema.PUBLICATIONS];
    final int perEpoch = Math.max(16, tier.contracts() / 18);
    for (int epoch = 0; epoch < epochs.length; epoch++) {
      epochs[epoch] = new ArrayList<>(epoch == 0
          ? tier.contracts() + tier.products() + tier.suppliers()
          : perEpoch);
    }
    int priorEpoch = -1;
    String priorTable = "";
    int priorId = 0;
    long count = 0;
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        final Event event = Event.fromJson(JsonParser.parseString(line).getAsJsonObject());
        if (event.epoch() < priorEpoch || event.epoch() == priorEpoch && event.table().compareTo(priorTable) < 0
            || event.epoch() == priorEpoch && event.table().equals(priorTable) && event.id() <= priorId) {
          throw new IllegalArgumentException("event stream is not strictly ordered at line " + (count + 1));
        }
        epochs[event.epoch()].add(event);
        priorEpoch = event.epoch();
        priorTable = event.table();
        priorId = event.id();
        count++;
      }
    }
    final long expected =
        (long) tier.contracts() + tier.products() + tier.suppliers() + 24L * ((tier.contracts() + 19L) / 20L)
            + 24L * ((tier.products() + 99L) / 100L) + 24L * ((tier.suppliers() + 99L) / 100L) + 4L;
    if (count != expected) {
      throw new IllegalArgumentException("event count " + count + " != tier expectation " + expected);
    }
    return epochs;
  }

  private static List<Event> filter(final List<Event> events, final String table) {
    final List<Event> filtered = new ArrayList<>();
    for (final Event event : events) {
      if (event.table().equals(table)) {
        filtered.add(event);
      }
    }
    return filtered;
  }

  private static void createResources(final Database<JsonResourceSession> database) {
    createBusinessResource(database, BitemporalSchema.CONTRACTS);
    createBusinessResource(database, BitemporalSchema.PRODUCTS);
    createBusinessResource(database, BitemporalSchema.SUPPLIERS);
    database.createResource(ResourceConfiguration.newBuilder(BitemporalSchema.EPOCHS)
                                                 .customCommitTimestamps(true)
                                                 .buildPathSummary(true)
                                                 .build());
    database.createResource(ResourceConfiguration.newBuilder(BitemporalSchema.DAYS)
                                                 .customCommitTimestamps(true)
                                                 .buildPathSummary(true)
                                                 .build());
  }

  private static void createBusinessResource(final Database<JsonResourceSession> database, final String name) {
    database.createResource(ResourceConfiguration.newBuilder(name)
                                                 .validTimePaths("vf", "vt")
                                                 .customCommitTimestamps(true)
                                                 .buildPathSummary(true)
                                                 .build());
  }

  private static void loadInitial(final Database<JsonResourceSession> database, final RelationState state,
      final List<Event> events) {
    if (events.size() != state.identityCount) {
      throw new IllegalArgumentException(state.name + " E0 count " + events.size() + " != " + state.identityCount);
    }
    final StringBuilder json = new StringBuilder(Math.max(32, events.size() * 96));
    json.append('[');
    for (int i = 0; i < events.size(); i++) {
      final Event event = events.get(i);
      if (!event.put() || event.id() != i + 1 || event.fromDay() != 0
          || event.toDay() != BitemporalSchema.HORIZON_DAYS) {
        throw new IllegalArgumentException("invalid " + state.name + " base event for id " + event.id());
      }
      if (i > 0) {
        json.append(',');
      }
      json.append(segmentJson(Segment.from(event)));
    }
    json.append(']');

    try (JsonResourceSession session = database.beginResourceSession(state.name);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        throw new IllegalStateException("inserted " + state.name + " resource has no array root");
      }
      state.arrayNodeKey = wtx.getNodeKey();
      ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, BitemporalSchema.DATABASE);
      if (!wtx.moveTo(state.arrayNodeKey) || !wtx.moveToFirstChild()) {
        throw new IllegalStateException("cannot traverse inserted " + state.name + " array");
      }
      for (int i = 0; i < events.size(); i++) {
        final Segment segment = Segment.from(events.get(i));
        segment.nodeKey = wtx.getNodeKey();
        final ArrayList<Segment> identity = new ArrayList<>(2);
        identity.add(segment);
        state.segments[segment.id] = identity;
        if (i + 1 < events.size() && !wtx.moveToRightSibling()) {
          throw new IllegalStateException("short " + state.name + " array at id " + segment.id);
        }
      }
      wtx.commit("SH1 E0", BitemporalSchema.systemTime(0));
      assertRevision(wtx, 0, state.name);
    }
  }

  private static void loadMetadata(final Database<JsonResourceSession> database, final String resource,
      final String json) {
    try (JsonResourceSession session = database.beginResourceSession(resource);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
      wtx.commit("SH1 immutable metadata", BitemporalSchema.systemTime(0));
      assertRevision(wtx, 0, resource);
    }
  }

  private static void applyPublication(final Database<JsonResourceSession> database, final RelationState state,
      final List<Event> events, final int epoch) {
    try (JsonResourceSession session = database.beginResourceSession(state.name);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      for (final Event event : events) {
        state.apply(wtx, event);
      }
      wtx.commit("SH1 E" + epoch, BitemporalSchema.systemTime(epoch));
      assertRevision(wtx, epoch, state.name);
    }
  }

  private static void assertRevision(final JsonNodeTrx wtx, final int epoch, final String resource) {
    // Resource creation persists an empty custom-timestamp bootstrap revision. E0 is therefore
    // revision 2 and E24 revision 26; query code selects by commit timestamp, never by arithmetic.
    final int expected = epoch + 2;
    if (wtx.getRevisionNumber() != expected) {
      throw new IllegalStateException(resource + " revision " + wtx.getRevisionNumber() + " != " + expected);
    }
  }

  private static String epochJson() {
    final StringBuilder json = new StringBuilder(2_000).append('[');
    for (int epoch = 0; epoch < BitemporalSchema.PUBLICATIONS; epoch++) {
      if (epoch > 0) {
        json.append(',');
      }
      json.append("{\"id\":")
          .append(epoch)
          .append(",\"epoch\":")
          .append(epoch)
          .append(",\"ts\":\"")
          .append(BitemporalSchema.systemTime(epoch))
          .append("\"}");
    }
    return json.append(']').toString();
  }

  private static String dayJson() {
    final StringBuilder json = new StringBuilder(28_000).append('[');
    for (int day = 0; day < BitemporalSchema.HORIZON_DAYS; day++) {
      if (day > 0) {
        json.append(',');
      }
      json.append("{\"id\":")
          .append(day)
          .append(",\"day_no\":")
          .append(day)
          .append(",\"ts\":\"")
          .append(BitemporalSchema.day(day))
          .append("\"}");
    }
    return json.append(']').toString();
  }

  private static void reportPublication(final Path root, final int epoch) throws IOException {
    final Footprint footprint = footprint(root);
    final Footprint campaignFootprint = footprint(REQUIRED_ROOT);
    final long cap = Long.getLong("bitemporal.sirixCapBytes", DEFAULT_SIRIX_CAP);
    final long campaignCap = Long.getLong("bitemporal.campaignCapBytes", DEFAULT_CAMPAIGN_CAP);
    System.out.printf(
        "SIRIX_PUBLICATION epoch=%d logical_bytes=%d allocated_bytes=%d " + "campaign_allocated_bytes=%d%n", epoch,
        footprint.logicalBytes(), footprint.allocatedBytes(), campaignFootprint.allocatedBytes());
    if (footprint.allocatedBytes() > cap) {
      throw new IllegalStateException(
          "Sirix allocated footprint " + footprint.allocatedBytes() + " exceeds cap " + cap + " after epoch " + epoch);
    }
    if (campaignFootprint.allocatedBytes() > campaignCap) {
      throw new IllegalStateException("campaign allocated footprint " + campaignFootprint.allocatedBytes()
          + " exceeds cap " + campaignCap + " after epoch " + epoch);
    }
    requireCapacity(root);
  }

  private static void requireCapacity(final Path path) throws IOException {
    final Path existing = Files.exists(path)
        ? path
        : path.getParent();
    final FileStore store = Files.getFileStore(existing);
    if (store.getUsableSpace() < MIN_FREE_BYTES) {
      throw new IllegalStateException("free space below 20 GiB at " + existing + ": " + store.getUsableSpace());
    }
  }

  private static Footprint footprint(final Path root) throws IOException {
    if (!Files.exists(root)) {
      return new Footprint(0, 0);
    }
    final long[] sizes = new long[2];
    try (var paths = Files.walk(root)) {
      paths.filter(Files::isRegularFile).forEach(path -> {
        try {
          final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
          sizes[0] = Math.addExact(sizes[0], attributes.size());
        } catch (final IOException e) {
          throw new FootprintException(e);
        }
      });
    } catch (final FootprintException e) {
      throw e.cause;
    }
    sizes[1] = allocatedBytes(root);
    return new Footprint(sizes[0], sizes[1]);
  }

  private static long allocatedBytes(final Path root) throws IOException {
    final Process process = new ProcessBuilder("du", "-B1", "-s", root.toString()).redirectErrorStream(true).start();
    final String output;
    try (BufferedReader reader = process.inputReader(StandardCharsets.UTF_8)) {
      output = reader.readLine();
    }
    try {
      if (process.waitFor() != 0 || output == null) {
        throw new IOException("du failed for " + root + ": " + output);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while measuring " + root, e);
    }
    final int separator = output.indexOf('\t');
    final int whitespace = separator >= 0
        ? separator
        : output.indexOf(' ');
    if (whitespace < 1) {
      throw new IOException("unexpected du output: " + output);
    }
    return Long.parseLong(output.substring(0, whitespace));
  }

  private static void sync() throws IOException, InterruptedException {
    final Process process = new ProcessBuilder("sync").inheritIO().start();
    if (process.waitFor() != 0) {
      throw new IOException("sync failed with exit code " + process.exitValue());
    }
  }

  private record Footprint(long logicalBytes, long allocatedBytes) {
  }

  private static final class FootprintException extends RuntimeException {
    private final IOException cause;

    FootprintException(final IOException cause) {
      super(cause);
      this.cause = cause;
    }
  }

  private static final class RelationState {
    private final String name;
    private final int identityCount;
    private final ArrayList<Segment>[] segments;
    private long arrayNodeKey;

    @SuppressWarnings("unchecked")
    RelationState(final String name, final int identityCount) {
      this.name = name;
      this.identityCount = identityCount;
      segments = new ArrayList[identityCount + 1];
    }

    void apply(final JsonNodeTrx wtx, final Event event) {
      if (!name.equals(event.table()) || event.id() > identityCount) {
        throw new IllegalArgumentException("event does not belong to " + name + ": " + event);
      }
      final ArrayList<Segment> old = segments[event.id()];
      if (old == null) {
        throw new IllegalStateException("missing identity state: " + name + '/' + event.id());
      }
      final ArrayList<Segment> next = new ArrayList<>(old.size() + 2);
      for (final Segment segment : old) {
        if (segment.toDay <= event.fromDay() || event.toDay() <= segment.fromDay) {
          next.add(segment.copy());
          continue;
        }
        if (segment.fromDay < event.fromDay()) {
          next.add(segment.withBounds(segment.fromDay, event.fromDay()));
        }
        if (event.toDay() < segment.toDay) {
          next.add(segment.withBounds(event.toDay(), segment.toDay));
        }
      }
      if (event.put()) {
        next.add(Segment.from(event));
      }
      next.sort(Comparator.comparingInt(segment -> segment.fromDay));
      coalesce(next);
      validate(next, event.id());
      reconcile(wtx, old, next);
      segments[event.id()] = next;
    }

    private void reconcile(final JsonNodeTrx wtx, final ArrayList<Segment> old, final ArrayList<Segment> next) {
      for (final Segment target : next) {
        final Segment reusable = findByStart(old, target.fromDay);
        if (reusable != null) {
          target.nodeKey = reusable.nodeKey;
          updateChangedFields(wtx, reusable, target);
        }
      }
      for (final Segment prior : old) {
        if (findByStart(next, prior.fromDay) == null) {
          if (!wtx.moveTo(prior.nodeKey)) {
            throw new IllegalStateException("cannot remove missing node " + prior.nodeKey);
          }
          wtx.remove();
        }
      }
      for (final Segment target : next) {
        if (target.nodeKey < 0) {
          if (!wtx.moveTo(arrayNodeKey)) {
            throw new IllegalStateException("cannot move to " + name + " array " + arrayNodeKey);
          }
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(segmentJson(target)), JsonNodeTrx.Commit.NO);
          target.nodeKey = wtx.getNodeKey();
        }
      }
    }

    private static void coalesce(final ArrayList<Segment> values) {
      int write = 0;
      for (int read = 0; read < values.size(); read++) {
        final Segment current = values.get(read);
        if (write > 0) {
          final Segment previous = values.get(write - 1);
          if (previous.toDay == current.fromDay && previous.samePayload(current)) {
            values.set(write - 1, previous.withBounds(previous.fromDay, current.toDay));
            continue;
          }
        }
        values.set(write++, current);
      }
      while (values.size() > write) {
        values.remove(values.size() - 1);
      }
    }

    private static void validate(final ArrayList<Segment> values, final int id) {
      int priorEnd = -1;
      for (final Segment value : values) {
        if (value.id != id || value.fromDay < 0 || value.fromDay >= value.toDay
            || value.toDay > BitemporalSchema.HORIZON_DAYS || value.fromDay < priorEnd) {
          throw new IllegalStateException("invalid logical segments for id " + id);
        }
        priorEnd = value.toDay;
      }
    }
  }

  private static Segment findByStart(final List<Segment> segments, final int fromDay) {
    for (final Segment segment : segments) {
      if (segment.fromDay == fromDay) {
        return segment;
      }
    }
    return null;
  }

  private static void updateChangedFields(final JsonNodeTrx wtx, final Segment old, final Segment replacement) {
    if (old.toDay != replacement.toDay) {
      setStringField(wtx, replacement.nodeKey, "vt", BitemporalSchema.day(replacement.toDay).toString());
    }
    if (old.value1 != replacement.value1) {
      setNumberField(wtx, replacement.nodeKey, firstPayloadField(replacement.table), replacement.value1);
    }
    if (old.value2 != replacement.value2) {
      setNumberField(wtx, replacement.nodeKey, secondPayloadField(replacement.table), replacement.value2);
    }
    if (BitemporalSchema.CONTRACTS.equals(replacement.table)) {
      if (old.value3 != replacement.value3) {
        setNumberField(wtx, replacement.nodeKey, "cost", replacement.value3);
      }
      if (old.value4 != replacement.value4) {
        setNumberField(wtx, replacement.nodeKey, "qty", replacement.value4);
      }
      if (old.value5 != replacement.value5) {
        setNumberField(wtx, replacement.nodeKey, "grade", replacement.value5);
      }
    }
  }

  private static String firstPayloadField(final String table) {
    return switch (table) {
      case BitemporalSchema.CONTRACTS -> "pid";
      case BitemporalSchema.PRODUCTS -> "category";
      case BitemporalSchema.SUPPLIERS -> "region";
      default -> throw new AssertionError("validated table: " + table);
    };
  }

  private static String secondPayloadField(final String table) {
    return switch (table) {
      case BitemporalSchema.CONTRACTS -> "sid";
      case BitemporalSchema.PRODUCTS -> "retail";
      case BitemporalSchema.SUPPLIERS -> "tier";
      default -> throw new AssertionError("validated table: " + table);
    };
  }

  private static void setNumberField(final JsonNodeTrx wtx, final long objectKey, final String field, final int value) {
    moveToFieldValue(wtx, objectKey, field);
    wtx.setNumberValue(value);
  }

  private static void setStringField(final JsonNodeTrx wtx, final long objectKey, final String field,
      final String value) {
    moveToFieldValue(wtx, objectKey, field);
    wtx.setStringValue(value);
  }

  private static void moveToFieldValue(final JsonNodeTrx wtx, final long objectKey, final String field) {
    if (!wtx.moveTo(objectKey) || !wtx.moveToFirstChild()) {
      throw new IllegalStateException("cannot inspect object node " + objectKey);
    }
    do {
      if (wtx.getName() != null && field.equals(wtx.getName().getLocalName())) {
        // Primitive object records may be stored either as OBJECT_KEY + child or as one fused
        // OBJECT_NAMED_* node. Both report a field name; only the former has a child to enter.
        if (wtx.hasFirstChild()) {
          wtx.moveToFirstChild();
        }
        return;
      }
    } while (wtx.moveToRightSibling());
    throw new IllegalStateException("field not found: " + field + " on node " + objectKey);
  }

  private static String segmentJson(final Segment segment) {
    final String common = "{\"id\":" + segment.id;
    final String payload = switch (segment.table) {
      case BitemporalSchema.CONTRACTS -> ",\"pid\":" + segment.value1 + ",\"sid\":" + segment.value2 + ",\"cost\":"
          + segment.value3 + ",\"qty\":" + segment.value4 + ",\"grade\":" + segment.value5;
      case BitemporalSchema.PRODUCTS -> ",\"category\":" + segment.value1 + ",\"retail\":" + segment.value2;
      case BitemporalSchema.SUPPLIERS -> ",\"region\":" + segment.value1 + ",\"tier\":" + segment.value2;
      default -> throw new AssertionError("validated table: " + segment.table);
    };
    return common + payload + ",\"vf\":\"" + BitemporalSchema.day(segment.fromDay) + "\",\"vt\":\""
        + BitemporalSchema.day(segment.toDay) + "\"}";
  }

  private static final class Segment {
    private final String table;
    private final int id;
    private final int fromDay;
    private final int toDay;
    private final int value1;
    private final int value2;
    private final int value3;
    private final int value4;
    private final int value5;
    private long nodeKey = -1;

    Segment(final String table, final int id, final int fromDay, final int toDay, final int value1, final int value2,
        final int value3, final int value4, final int value5) {
      this.table = table;
      this.id = id;
      this.fromDay = fromDay;
      this.toDay = toDay;
      this.value1 = value1;
      this.value2 = value2;
      this.value3 = value3;
      this.value4 = value4;
      this.value5 = value5;
    }

    static Segment from(final Event event) {
      return new Segment(event.table(), event.id(), event.fromDay(), event.toDay(), event.value1(), event.value2(),
          event.value3(), event.value4(), event.value5());
    }

    Segment copy() {
      return withBounds(fromDay, toDay);
    }

    Segment withBounds(final int from, final int to) {
      final Segment copy = new Segment(table, id, from, to, value1, value2, value3, value4, value5);
      if (from == fromDay) {
        copy.nodeKey = nodeKey;
      }
      return copy;
    }

    boolean samePayload(final Segment other) {
      return value1 == other.value1 && value2 == other.value2 && value3 == other.value3 && value4 == other.value4
          && value5 == other.value5;
    }
  }
}
