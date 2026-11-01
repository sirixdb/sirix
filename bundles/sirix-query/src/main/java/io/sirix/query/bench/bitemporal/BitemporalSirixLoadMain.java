package io.sirix.query.bench.bitemporal;

import com.google.gson.stream.JsonReader;
import io.brackit.query.jdm.Type;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import java.util.Set;
import io.brackit.query.util.path.PathParser;
import static io.brackit.query.util.path.Path.parse;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.bench.bitemporal.BitemporalIntervalStore.Intervals;
import io.sirix.query.bench.bitemporal.BitemporalSchema.Tier;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

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

    final long started = System.nanoTime();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    long parseNanos;
    try (BitemporalEventReader events = new BitemporalEventReader(eventsPath, tier);
        BitemporalIntervalStore intervals = new BitemporalIntervalStore(512L << 20);
        Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      events.advance();
      createResources(database);
      final RelationState contracts = new RelationState(BitemporalSchema.CONTRACTS, tier.contracts(), intervals);
      final RelationState products = new RelationState(BitemporalSchema.PRODUCTS, tier.products(), intervals);
      final RelationState suppliers = new RelationState(BitemporalSchema.SUPPLIERS, tier.suppliers(), intervals);

      loadInitial(database, contracts, events);
      loadInitial(database, products, events);
      loadInitial(database, suppliers, events);
      loadMetadata(database, BitemporalSchema.EPOCHS, epochJson());
      loadMetadata(database, BitemporalSchema.DAYS, dayJson());
      reportPublication(databaseRoot, 0);

      for (int epoch = 1; epoch < BitemporalSchema.PUBLICATIONS; epoch++) {
        applyPublication(database, contracts, events, epoch);
        applyPublication(database, products, events, epoch);
        applyPublication(database, suppliers, events, epoch);
        reportPublication(databaseRoot, epoch);
      }
      events.requireEnd();
      parseNanos = events.parseNanos();
      System.out.printf("SIRIX_INPUT events=%d parse_io_seconds=%.6f native_state_bytes=%d%n", events.count(),
          parseNanos / 1e9, intervals.reservedBytes());
    }

    sync();
    final Footprint footprint = footprint(databaseRoot);
    final double seconds = (System.nanoTime() - started) / 1e9;
    System.out.printf(
        "SIRIX_LOAD tier=%s seconds=%.3f parse_io_seconds=%.6f load_excluding_parse_seconds=%.6f logical_bytes=%d allocated_bytes=%d db=%s%n",
        tier.label(), seconds, parseNanos / 1e9, seconds - parseNanos / 1e9, footprint.logicalBytes(),
        footprint.allocatedBytes(), databaseRoot);
  }

  private static Path requireCampaignPath(final Path path, final String label) throws IOException {
    final Path normalized = path.toAbsolutePath().normalize();
    if (!normalized.startsWith(REQUIRED_ROOT)) {
      throw new IllegalArgumentException(label + " must be under " + REQUIRED_ROOT + ": " + normalized);
    }
    return normalized;
  }

  private static void createResources(final Database<JsonResourceSession> database) {
    createBusinessResource(database, BitemporalSchema.CONTRACTS);
    createBusinessResource(database, BitemporalSchema.PRODUCTS);
    createBusinessResource(database, BitemporalSchema.SUPPLIERS);
    database.createResource(ResourceConfiguration.newBuilder(BitemporalSchema.EPOCHS)
                                                 .storeDiffs(false)
                                                 .customCommitTimestamps(true)
                                                 .buildPathSummary(true)
                                                 .build());
    database.createResource(ResourceConfiguration.newBuilder(BitemporalSchema.DAYS)
                                                 .storeDiffs(false)
                                                 .customCommitTimestamps(true)
                                                 .buildPathSummary(true)
                                                 .build());
  }

  private static void createBusinessResource(final Database<JsonResourceSession> database, final String name) {
    final ResourceConfiguration.Builder builder = ResourceConfiguration.newBuilder(name)
                                                                       .validTimePaths("vf", "vt")
                                                                       .storeDiffs(false)
                                                                       .customCommitTimestamps(true)
                                                                       .buildPathSummary(true);
    // What a commit writes for a touched page, and what reading it back costs, is decided by the
    // versioning algorithm, so commit-latency campaigns run the same load under each of them.
    final String versioning = System.getProperty("sirix.bitemporal.versioning");
    if (versioning != null) {
      builder.versioningApproach(VersioningType.valueOf(versioning));
    }
    database.createResource(builder.build());
  }

  static void createBusinessKeyIndex(final JsonResourceSession session, final JsonNodeTrx writer,
      final String resource) {
    if (!BitemporalSchema.CONTRACTS.equals(resource) && !BitemporalSchema.PRODUCTS.equals(resource)
        || Boolean.getBoolean("sirix.bitemporal.disableBusinessKeyIndex")) {
      return;
    }
    final var storage = writer.getStorageEngineWriter();
    final int indexId = storage.getCASPage(storage.getActualRevisionRootPage()).nextUnallocatedIndex();
    final var id = parse("/[]/id", PathParser.Type.JSON);
    final IndexDef definition = IndexDefs.createCASIdxDef(false, Type.INR, Set.of(id), indexId, IndexDef.DbType.JSON);
    session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(definition), writer);
  }

  private static void loadInitial(final Database<JsonResourceSession> database, final RelationState state,
      final BitemporalEventReader events) throws IOException {
    try (JsonResourceSession session = database.beginResourceSession(state.name);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      try (JsonReader input = new JsonReader(new InitialReader(events, state))) {
        wtx.insertSubtreeAsFirstChild(input, JsonNodeTrx.Commit.NO);
      }
      wtx.moveToDocumentRoot();
      if (!wtx.moveToFirstChild()) {
        throw new IllegalStateException("inserted " + state.name + " resource has no array root");
      }
      state.arrayNodeKey = wtx.getNodeKey();
      // Commit-latency campaigns measure the maintenance cost of each index family by declaring
      // fewer of them; the query kit needs every family, so the defaults declare them all.
      if (!Boolean.getBoolean("sirix.bitemporal.disableValidTimeIndexes")) {
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, BitemporalSchema.DATABASE);
      }
      createBusinessKeyIndex(session, wtx, state.name);
      // The projection the grouped queries read their columns from, built from the same E0 records
      // and maintained by every later publication's commit.
      BitemporalProjections.declare(session, wtx, state.name);
      BitemporalTemporalAggregates.declare(session, wtx, state.name);
      if (!wtx.moveTo(state.arrayNodeKey) || !wtx.moveToFirstChild()) {
        throw new IllegalStateException("cannot traverse inserted " + state.name + " array");
      }
      for (int id = 1; id <= state.identityCount; id++) {
        state.segments.read(id, state.old);
        state.old.keys[0] = wtx.getNodeKey();
        state.segments.write(id, state.old);
        if (id < state.identityCount && !wtx.moveToRightSibling()) {
          throw new IllegalStateException("short " + state.name + " array at id " + id);
        }
      }
      final long commitStart = System.nanoTime();
      wtx.commit("SH1 E0", BitemporalSchema.systemTime(0));
      System.out.printf("SIRIX_COMMIT epoch=0 resource=%s events=%d apply_ms=0.0 commit_ms=%.1f%n", state.name,
          state.identityCount, (System.nanoTime() - commitStart) / 1e6);
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
      final BitemporalEventReader events, final int epoch) throws IOException {
    try (JsonResourceSession session = database.beginResourceSession(state.name);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final long applyStart = System.nanoTime();
      int count = 0;
      final long parseStart = events.parseNanos();
      while (events.matches(epoch, state.name)) {
        state.apply(wtx, events);
        events.advance();
        count++;
      }
      final long parseElapsed = events.parseNanos() - parseStart;
      final long commitStart = System.nanoTime();
      wtx.commit("SH1 E" + epoch, BitemporalSchema.systemTime(epoch));
      final long commitEnd = System.nanoTime();
      System.out.printf("SIRIX_COMMIT epoch=%d resource=%s events=%d apply_ms=%.1f commit_ms=%.1f parse_io_ms=%.3f%n",
          epoch, state.name, count, (commitStart - applyStart - parseElapsed) / 1e6, (commitEnd - commitStart) / 1e6,
          parseElapsed / 1e6);
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
    private final BitemporalIntervalStore.Relation segments;
    private final Intervals old = new Intervals();
    private final Intervals next = new Intervals();
    private final StringBuilder json = new StringBuilder(256);
    private long arrayNodeKey;

    RelationState(final String name, final int identityCount, final BitemporalIntervalStore owner) {
      this.name = name;
      this.identityCount = identityCount;
      segments = owner.relation(identityCount);
    }

    void apply(final JsonNodeTrx wtx, final BitemporalEventReader event) {
      if (!name.equals(event.table)) {
        throw new IllegalArgumentException("event does not belong to " + name);
      }
      segments.read(event.id, old);
      old.replace(event, next);
      for (int target = 0; target < next.size; target++) {
        final int prior = old.find(next.get(target, 0));
        if (prior >= 0) {
          next.keys[target] = old.keys[prior];
          updateChangedFields(wtx, prior, target);
        }
      }
      for (int prior = 0; prior < old.size; prior++) {
        if (next.find(old.get(prior, 0)) < 0) {
          if (!wtx.moveTo(old.keys[prior])) {
            throw new IllegalStateException("cannot remove missing node " + old.keys[prior]);
          }
          wtx.remove();
        }
      }
      for (int target = 0; target < next.size; target++) {
        if (next.keys[target] < 0) {
          if (!wtx.moveTo(arrayNodeKey)) {
            throw new IllegalStateException("cannot move to " + name + " array " + arrayNodeKey);
          }
          json.setLength(0);
          appendSegment(json, name, event.id, next, target);
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          next.keys[target] = wtx.getNodeKey();
        }
      }
      segments.write(event.id, next);
    }

    private void updateChangedFields(final JsonNodeTrx wtx, final int prior, final int target) {
      final long key = next.keys[target];
      if (old.get(prior, 1) != next.get(target, 1)) {
        setStringField(wtx, key, "vt", DAYS[next.get(target, 1)]);
      }
      for (int column = 2; column < Intervals.COLUMNS; column++) {
        if (old.get(prior, column) != next.get(target, column)) {
          final String field = switch (column) {
            case 2 -> firstPayloadField(name);
            case 3 -> secondPayloadField(name);
            case 4 -> "cost";
            case 5 -> "qty";
            case 6 -> "grade";
            default -> throw new AssertionError(column);
          };
          setNumberField(wtx, key, field, next.get(target, column));
        }
      }
    }
  }

  /** Supplies the original single E0 subtree insertion without materializing its JSON or events. */
  private static final class InitialReader extends Reader {
    private final BitemporalEventReader events;
    private final RelationState state;
    private final StringBuilder buffer = new StringBuilder(256);
    private int position;
    private int count;
    private boolean ended;

    InitialReader(final BitemporalEventReader events, final RelationState state) {
      this.events = events;
      this.state = state;
      buffer.append('[');
    }

    @Override
    public int read(final char[] destination, final int offset, final int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, destination.length);
      if (length == 0) {
        return 0;
      }
      if (position == buffer.length()) {
        if (ended) {
          return -1;
        }
        buffer.setLength(0);
        position = 0;
        if (events.matches(0, state.name)) {
          if (!events.put || events.id != count + 1 || events.from != 0 || events.to != BitemporalSchema.HORIZON_DAYS) {
            throw new IllegalArgumentException("invalid " + state.name + " base event for id " + events.id);
          }
          if (count != 0) {
            buffer.append(',');
          }
          state.old.size = 0;
          state.old.append(events);
          state.segments.write(events.id, state.old);
          appendSegment(buffer, state.name, events.id, state.old, 0);
          count++;
          events.advance();
        } else {
          if (count != state.identityCount) {
            throw new IllegalArgumentException(state.name + " E0 count " + count + " != " + state.identityCount);
          }
          buffer.append(']');
          ended = true;
        }
      }
      final int copied = Math.min(length, buffer.length() - position);
      buffer.getChars(position, position + copied, destination, offset);
      position += copied;
      return copied;
    }

    @Override
    public void close() {
      // The load owns the event stream; the resource reader must not close the next window.
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

  private static final String[] DAYS = dayStrings();

  private static String[] dayStrings() {
    final String[] days = new String[BitemporalSchema.HORIZON_DAYS + 1];
    for (int day = 0; day < days.length; day++) {
      days[day] = BitemporalSchema.day(day).toString();
    }
    return days;
  }

  private static void appendSegment(final StringBuilder json, final String table, final int id, final Intervals values,
      final int row) {
    json.append("{\"id\":")
        .append(id)
        .append(",\"")
        .append(firstPayloadField(table))
        .append("\":")
        .append(values.get(row, 2))
        .append(",\"")
        .append(secondPayloadField(table))
        .append("\":")
        .append(values.get(row, 3));
    if (BitemporalSchema.CONTRACTS.equals(table)) {
      json.append(",\"cost\":")
          .append(values.get(row, 4))
          .append(",\"qty\":")
          .append(values.get(row, 5))
          .append(",\"grade\":")
          .append(values.get(row, 6));
    }
    json.append(",\"vf\":\"")
        .append(DAYS[values.get(row, 0)])
        .append("\",\"vt\":\"")
        .append(DAYS[values.get(row, 1)])
        .append("\"}");
  }
}
