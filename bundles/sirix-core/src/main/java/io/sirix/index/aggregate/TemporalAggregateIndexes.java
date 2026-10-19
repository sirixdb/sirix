package io.sirix.index.aggregate;

import io.sirix.index.changelog.ChangeLogSlab;
import io.sirix.index.changelog.ChangeLogGroupKey;
import io.sirix.index.changelog.TemporalAggregateCatchUpBudget;
import io.sirix.index.changelog.TemporalAggregateSummaryLog;
import io.sirix.index.changelog.TemporalAggregateSummaryLog.WriteWork;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.path.summary.PathSummaryReader;
import java.util.function.LongFunction;
import java.util.function.ObjIntConsumer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.sirix.index.aggregate.TemporalAggregateStorage.GroupHead;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.index.aggregate.TemporalAggregateSpec.Lane;
import io.sirix.index.projection.ProjectionAggregateRows;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Map;

/**
 * Declaration validation, committed-projection builds, and durable-slab maintenance. Ordinary
 * sources reserve declarations and log changes; the derived resource runs the aggregate kernel.
 */
public final class TemporalAggregateIndexes {
  private static final int FORMAT = 5;
  private static final TemporalAggregateDomain DOMAIN = new TemporalAggregateDomain();
  private static volatile MaintenanceWork lastMaintenance = new MaintenanceWork(0, 0, 0, 0, 0, 0, 0);

  private static volatile WriteWork lastSummaryWrite = new WriteWork(0, 0, 0);

  /**
   * All declarations of the last logged source projection publication, including coverage headers.
   */
  public static WriteWork lastSummaryWrite() {
    return lastSummaryWrite;
  }

  public record MaintenanceWork(long changedRecords, long nodesRead, long nodesWritten, long endpointsRead,
      long endpointsWritten, long summaryNodesRead, long summaryNodesWritten) {
  }

  public static MaintenanceWork lastMaintenance() {
    return lastMaintenance;
  }

  private TemporalAggregateIndexes() {}

  public static void validate(final IndexDef aggregate, final IndexDef projection,
      final @Nullable ValidTimeConfig config) {
    Objects.requireNonNull(config, "Temporal aggregate indexes require valid-time configuration");
    if (!aggregate.isTemporalAggregateIndex() || !projection.isProjectionIndex()
        || aggregate.getTemporalAggregateSpec().projectionId() != projection.getID()
        || !aggregate.getPaths().iterator().next().toString().equals(projection.getProjectionRootPath().toString())) {
      throw new IllegalArgumentException("Temporal aggregate source must be its declared covering projection");
    }
    aggregate.getTemporalAggregateSpec().validateColumnCount(projection.getProjectionFields().size());
    endpointColumn(projection, config.getValidFromPath());
    endpointColumn(projection, config.getValidToPath());
  }

  public static void build(final StorageEngineWriter writer, final IndexDef definition, final IndexDef projection) {
    final JsonResourceSession session = (JsonResourceSession) writer.getResourceSession();
    final ValidTimeConfig config = Objects.requireNonNull(session.getResourceConfig().getValidTimeConfig());
    validate(definition, projection, config);
    final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.requireReady(writer, projection.getID());
    try {
      listener(writer, projection.getID()).initializeChangeLog();
      if (DerivedIndexMaintainer.enabled(writer.getResourceSession().getResourceConfig())) {
        ChangeLogStorage.reserveAggregateId(writer, definition.getID());
        ((AbstractResourceSession<?, ?>) session).enableDerivedIndexes();
      } else {
        buildColumns(writer, definition, projection, config, metadata);
      }
    } catch (final RuntimeException | Error failure) {
      writer.markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  /** Called only after the projection has installed its final rows for this maintenance batch. */
  public static void maintain(final StorageEngineWriter writer, final IndexDef projection, final ChangeLogSlab slab) {
    if (DerivedIndexMaintainer.enabled(writer.getResourceSession().getResourceConfig())
        || !(writer.getResourceSession() instanceof JsonResourceSession session)) {
      return;
    }
    final ValidTimeConfig config = Objects.requireNonNull(session.getResourceConfig().getValidTimeConfig());
    final int from = endpointColumn(projection, config.getValidFromPath());
    final int to = endpointColumn(projection, config.getValidToPath());
    final byte[] kinds = slab.kinds();
    // One endpoint parse per distinct value across both old/new rows and all declarations.
    final Map<String, Long> instants = new HashMap<>();
    for (final IndexDef definition : session.getWtxIndexController(writer.getRevisionNumber())
                                            .getIndexes()
                                            .getIndexDefs()) {
      if (!definition.isTemporalAggregateIndex()
          || definition.getTemporalAggregateSpec().projectionId() != projection.getID()) {
        continue;
      }
      final TemporalAggregateSpec spec = definition.getTemporalAggregateSpec();
      final long[] dependencies = new long[(kinds.length + 63) >>> 6];
      dependOn(dependencies, from);
      dependOn(dependencies, to);
      for (final int column : spec.groupColumns()) {
        dependOn(dependencies, column);
      }
      for (final Lane lane : spec.lanes()) {
        dependOn(dependencies, lane.left());
        if (lane.right() >= 0) {
          dependOn(dependencies, lane.right());
        }
      }
      long changed = 0;
      for (final ChangeLogSlab.Entry entry : slab.entries()) {
        if (affected(dependencies, entry.changed())) {
          changed++;
        }
      }
      if (changed == 0) {
        lastMaintenance = new MaintenanceWork(0, 0, 0, 0, 0, 0, 0);
        continue;
      }
      final TransactionState state = listener(writer, projection.getID()).aggregateState(definition.getID());
      state.atRevision(writer.getRevisionNumber());
      try (TemporalAggregateStorage storage = new TemporalAggregateStorage(writer, definition.getID())) {
        final Header header = header(Objects.requireNonNull(storage.reader().get(TemporalAggregateStorage.HEADER)));
        final Maintenance maintenance = new Maintenance(storage, spec, header);
        for (final ChangeLogSlab.Entry entry : slab.entries()) {
          if (!affected(dependencies, entry.changed())) {
            continue;
          }
          final Row previous = state.rows.containsKey(entry.record())
              ? state.rows.get(entry.record())
              : state.built
                  ? null
                  : fromLog(entry.before(), kinds, from, to, spec, instants);
          final Row next = fromLog(entry.after(), kinds, from, to, spec, instants);
          maintenance.change(previous, next);
          state.rows.put(entry.record(), next);
        }
        maintenance.finish();
        lastMaintenance = new MaintenanceWork(changed, storage.reader().nodesRead(), storage.nodesWritten(),
            maintenance.endpointsRead, maintenance.endpointsWritten, maintenance.summaryNodesRead,
            maintenance.summaryNodesWritten);
      }
    }
  }

  /**
   * Builds from a published source projection; the destination has no document or projection tree.
   */
  public static void buildDerived(final StorageEngineWriter destination, final JsonNodeReadOnlyTrx source,
      final IndexDef definition, final IndexDef projection) {
    final StorageEngineReader reader = source.getStorageEngineReader();
    final ValidTimeConfig config =
        Objects.requireNonNull(source.getResourceSession().getResourceConfig().getValidTimeConfig());
    validate(definition, projection, config);
    final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.requireReady(reader, projection.getID());
    TemporalAggregateStorage.resetForBuild(destination, definition.getID());
    try (TemporalAggregateStorage storage = new TemporalAggregateStorage(destination, definition.getID());
        PathSummaryReader paths = source.getResourceSession().openPathSummary(source.getRevisionNumber())) {
      final Maintenance maintenance = new Maintenance(storage, definition.getTemporalAggregateSpec(), new Header(1, 0));
      final Source rows = new Source(reader, metadata, projection, config,
          record -> ProjectionAggregateRows.extract(projection, paths, source, record));
      ProjectionAggregateRows.visit(reader, projection.getID(), metadata,
          (record, page, row) -> maintenance.change(null, rows.row(record, page, row, maintenance.spec)));
      maintenance.finish();
    }
  }

  /**
   * Applies one durable slab exactly once, without opening any source projection or document pages.
   */
  public static MaintenanceWork applyDerived(final StorageEngineWriter destination, final IndexDef definition,
      final IndexDef projection, final ValidTimeConfig config, final ChangeLogSlab slab,
      final ObjIntConsumer<byte[]> foldCandidate) {
    validate(definition, projection, config);
    if (slab.projection() != projection.getID() || slab.kinds().length != projection.getProjectionFields().size()) {
      throw new IllegalArgumentException("Change-log schema does not match aggregate source");
    }
    final int from = endpointColumn(projection, config.getValidFromPath());
    final int to = endpointColumn(projection, config.getValidToPath());
    final byte[] kinds = slab.kinds();
    final TemporalAggregateSpec spec = definition.getTemporalAggregateSpec();
    final long[] dependencies = new long[(kinds.length + 63) >>> 6];
    dependOn(dependencies, from);
    dependOn(dependencies, to);
    for (final int column : spec.groupColumns()) {
      dependOn(dependencies, column);
    }
    for (final Lane lane : spec.lanes()) {
      dependOn(dependencies, lane.left());
      if (lane.right() >= 0) {
        dependOn(dependencies, lane.right());
      }
    }
    final Map<String, Long> instants = new HashMap<>();
    try (TemporalAggregateStorage storage = new TemporalAggregateStorage(destination, definition.getID())) {
      final Header header = header(Objects.requireNonNull(storage.reader().get(TemporalAggregateStorage.HEADER)));
      final Maintenance maintenance = new Maintenance(storage, spec, header);
      long changed = 0;
      for (final ChangeLogSlab.Entry entry : slab.entries()) {
        if (affected(dependencies, entry.changed())) {
          maintenance.change(fromLog(entry.before(), kinds, from, to, spec, instants),
              fromLog(entry.after(), kinds, from, to, spec, instants));
          changed++;
        }
      }
      if (changed != 0) {
        maintenance.finish(foldCandidate);
      }
      return new MaintenanceWork(changed, storage.reader().nodesRead(), storage.nodesWritten(),
          maintenance.endpointsRead, maintenance.endpointsWritten, maintenance.summaryNodesRead,
          maintenance.summaryNodesWritten);
    }
  }

  /** Persist small signed operands from rows already captured by the projection listener. */
  public static void writeSummaries(final StorageEngineWriter writer, final IndexDef projection,
      final ChangeLogSlab slab, final ChangeLogStorage log) {
    final JsonResourceSession session = (JsonResourceSession) writer.getResourceSession();
    final ValidTimeConfig config = Objects.requireNonNull(session.getResourceConfig().getValidTimeConfig());
    final int from = endpointColumn(projection, config.getValidFromPath());
    final int to = endpointColumn(projection, config.getValidToPath());
    final byte[] kinds = slab.kinds();
    final Map<String, Long> instants = new HashMap<>();
    int groups = 0;
    int bytes = 0;
    long slots = 0;
    for (final IndexDef definition : session.getWtxIndexController(writer.getRevisionNumber())
                                            .getIndexes()
                                            .getIndexDefs()) {
      if (definition.isTemporalAggregateIndex()
          && definition.getTemporalAggregateSpec().projectionId() == projection.getID()) {
        final WriteWork work = writeSummary(log, slab, definition, kinds, from, to, instants);
        groups = Math.addExact(groups, work.groups());
        bytes = Math.addExact(bytes, work.bytes());
        slots = Math.addExact(slots, work.slots());
      }
    }
    lastSummaryWrite = new WriteWork(groups, bytes, slots);
  }

  private static WriteWork writeSummary(final ChangeLogStorage log, final ChangeLogSlab slab, final IndexDef definition,
      final byte[] kinds, final int from, final int to, final Map<String, Long> instants) {
    final TemporalAggregateSpec spec = definition.getTemporalAggregateSpec();
    final long[] dependencies = dependencies(spec, kinds.length, from, to);
    final TemporalAggregateParts parts = new TemporalAggregateParts(spec, DOMAIN.height());
    final TreeMap<byte[], TemporalAggregateParts.SummaryBuilder> groups = new TreeMap<>(Arrays::compareUnsigned);
    final TreeMap<byte[], byte[]> encoded = new TreeMap<>(Arrays::compareUnsigned);
    long unsupported = 0;
    boolean unservable = false;
    try {
      for (final ChangeLogSlab.Entry entry : slab.entries()) {
        if (!affected(dependencies, entry.changed())) {
          continue;
        }
        final Row previous = fromLog(entry.before(), kinds, from, to, spec, instants);
        final Row next = fromLog(entry.after(), kinds, from, to, spec, instants);
        if (previous != null && previous.same(next)) {
          continue;
        }
        if (previous != null && previous.unsupported) {
          unsupported = Math.decrementExact(unsupported);
          unservable = true;
        }
        if (next != null && next.unsupported) {
          unsupported = Math.incrementExact(unsupported);
          unservable = true;
        }
        summarize(groups, parts, previous, -1);
        summarize(groups, parts, next, 1);
      }
      for (final var group : groups.entrySet()) {
        final byte[] operand = group.getValue().encode(TemporalAggregateSummaryLog.operandLimit(group.getKey()));
        if (operand == null || operand.length != 0) {
          encoded.put(group.getKey(), operand);
        }
      }
    } catch (final ArithmeticException overflow) {
      // The durable source commit remains valid. No partial summary may be used by a reader.
      unservable = true;
      encoded.clear();
    }
    return TemporalAggregateSummaryLog.write(log, slab, definition.getID(), encoded, unsupported, unservable);
  }

  private static void summarize(final TreeMap<byte[], TemporalAggregateParts.SummaryBuilder> groups,
      final TemporalAggregateParts parts, final @Nullable Row row, final int sign) {
    if (row != null && !row.unsupported && row.group != null) {
      groups.computeIfAbsent(row.group, ignored -> parts.summaryBuilder())
            .add(row.from, row.to, row.values, row.present, sign);
    }
  }

  private static long[] dependencies(final TemporalAggregateSpec spec, final int columns, final int from,
      final int to) {
    final long[] dependencies = new long[(columns + 63) >>> 6];
    dependOn(dependencies, from);
    dependOn(dependencies, to);
    for (final int column : spec.groupColumns()) {
      dependOn(dependencies, column);
    }
    for (final Lane lane : spec.lanes()) {
      dependOn(dependencies, lane.left());
      if (lane.right() >= 0) {
        dependOn(dependencies, lane.right());
      }
    }
    return dependencies;
  }

  /** A query-owned tail, reusable across all instants of an immutable source view. */
  public static final class CatchUp {
    private final TemporalAggregateParts parts;
    private final TreeMap<byte[], TemporalAggregateParts.Delta> groups = new TreeMap<>(Arrays::compareUnsigned);

    private CatchUp(final TemporalAggregateParts parts) {
      this.parts = parts;
    }

    public Iterable<byte[]> keys() {
      return groups.navigableKeySet();
    }

    public boolean contains(final byte[] key) {
      return groups.containsKey(key);
    }

    public AggregateSummary point(final TemporalAggregateParts.Store store, final long[] stored, final byte[] key,
        final long coordinate) {
      return parts.point(store, stored, coordinate, groups.get(key));
    }
  }

  /**
   * Fold only addressed changes. Any missing coverage, discontinuous base, unsupported cell or
   * exhausted budget aborts the entire route. Nothing is written to either resource.
   */
  public static CatchUp catchUp(final StorageEngineReader source, final int maintained, final IndexDef definition,
      final IndexDef projection, final TemporalAggregateCatchUpBudget.Budget budget) {
    final int target = source.getRevisionNumber();
    if (maintained < 1 || maintained > target || target - maintained > DerivedIndexMaintainer.MAX_QUERY_LAG) {
      throw new TemporalAggregateCatchUpBudget.LimitExceeded();
    }
    final ValidTimeConfig config =
        Objects.requireNonNull(source.getResourceSession().getResourceConfig().getValidTimeConfig());
    validate(definition, projection, config);
    final TemporalAggregateParts parts =
        new TemporalAggregateParts(definition.getTemporalAggregateSpec(), DOMAIN.height());
    final TreeMap<byte[], TemporalAggregateParts.SummaryBuilder> builders = new TreeMap<>(Arrays::compareUnsigned);
    for (int offset = 1; offset <= target - maintained; offset++) {
      final int revision = maintained + offset;
      try (var log =
          new TemporalAggregateSummaryLog.Reader(source, projection.getID(), revision, definition.getID(), budget)) {
        log.visit((key, operand) -> {
          if (!builders.containsKey(key) && builders.size() >= 4096) {
            throw new TemporalAggregateCatchUpBudget.LimitExceeded();
          }
          builders.computeIfAbsent(key, ignored -> parts.summaryBuilder()).merge(operand, budget);
        });
      }
    }
    final CatchUp result = new CatchUp(parts);
    for (final var group : builders.entrySet()) {
      result.groups.put(group.getKey(), group.getValue().snapshot());
    }
    return result;
  }

  /** Folds only the supplied touched groups, in a separate derived transaction. */
  public static void foldDerived(final StorageEngineWriter writer, final IndexDef definition,
      final Iterable<byte[]> groups) {
    try (TemporalAggregateStorage storage = new TemporalAggregateStorage(writer, definition.getID())) {
      final Header header = header(Objects.requireNonNull(storage.reader().get(TemporalAggregateStorage.HEADER)));
      final TemporalAggregateParts parts =
          new TemporalAggregateParts(definition.getTemporalAggregateSpec(), DOMAIN.height());
      long next = header.nextPart();
      for (final byte[] key : groups) {
        final GroupHead head = GroupHead.decode(Objects.requireNonNull(storage.reader().get(key)));
        if (head.parts().length < 2) {
          continue;
        }
        final TemporalAggregateParts.Builder checkpoint = parts.builder();
        for (final long part : head.parts()) {
          checkpoint.merge(storage.reader().parts(), part);
        }
        final byte[] encoded = checkpoint.encode(true);
        final int tier = Maintenance.tierForBytes(encoded.length) + 1;
        storage.putPart(next, encoded);
        storage.put(key,
            new GroupHead(head.rows(), tier, new long[] {next}, new byte[] {(byte) tier}, encoded.length, 0).encode());
        next = Math.incrementExact(next);
      }
      storage.put(TemporalAggregateStorage.HEADER,
          ByteBuffer.allocate(20).putInt(FORMAT).putLong(next).putLong(header.unsupportedRows()).array());
    }
  }

  private static boolean affected(final long[] dependencies, final long[] changed) {
    for (int word = 0; word < dependencies.length; word++) {
      if ((dependencies[word] & changed[word]) != 0) {
        return true;
      }
    }
    return false;
  }

  private static @Nullable Row fromLog(final ChangeLogSlab.@Nullable Row row, final byte[] kinds, final int fromColumn,
      final int toColumn, final TemporalAggregateSpec spec, final Map<String, Long> instants) {
    if (row == null) {
      return null;
    }
    if (!row.object) {
      return Row.UNSUPPORTED;
    }
    if (!row.present(fromColumn) && !row.present(toColumn)) {
      return Row.EMPTY;
    }
    try {
      final long from = endpoint(row, kinds, fromColumn, false, instants);
      final long to = endpoint(row, kinds, toColumn, true, instants);
      if (from >= to) {
        return Row.EMPTY;
      }
      final byte[] group = ChangeLogGroupKey.encode(row, kinds, spec.groupColumns());
      if (group == null) {
        return Row.UNSUPPORTED;
      }
      final long[] values = new long[spec.lanes().size()];
      long present = 0;
      for (int at = 0; at < values.length; at++) {
        final Lane lane = spec.lanes().get(at);
        requireSupported(row, lane.left());
        if (lane.right() >= 0) {
          requireSupported(row, lane.right());
        }
        if (row.present(lane.left()) && (lane.right() < 0 || row.present(lane.right()))) {
          if (kinds[lane.left()] != ChangeLogSlab.LONG
              || (lane.right() >= 0 && kinds[lane.right()] != ChangeLogSlab.LONG)) {
            throw new UnsupportedCell();
          }
          values[at] = lane.right() < 0
              ? row.numbers[lane.left()]
              : Math.multiplyExact(row.numbers[lane.left()], row.numbers[lane.right()]);
          present |= 1L << at;
        }
      }
      return new Row(group, from, to, values, present, false);
    } catch (final DateTimeException | ArithmeticException | UnsupportedCell decline) {
      return Row.UNSUPPORTED;
    }
  }

  private static long endpoint(final ChangeLogSlab.Row row, final byte[] kinds, final int column, final boolean upper,
      final Map<String, Long> instants) {
    if (!row.present(column)) {
      return upper
          ? DOMAIN.maxValue() + 1
          : DOMAIN.lowerBound(null);
    }
    if (kinds[column] == ChangeLogSlab.TIMESTAMP) {
      if ((row.flags[column] & (ChangeLogSlab.NON_DOUBLE | ChangeLogSlab.NON_INTEGRAL)) != 0) {
        throw new UnsupportedCell();
      }
      final Instant instant = Instant.ofEpochSecond(row.numbers[column]);
      if (!DOMAIN.isExact(instant)) {
        throw new UnsupportedCell();
      }
      return DOMAIN.point(instant);
    }
    requireSupported(row, column);
    if (kinds[column] != ChangeLogSlab.STRING) {
      throw new UnsupportedCell();
    }
    return instants.computeIfAbsent((String) row.text[column], value -> {
      final Instant instant = Instant.parse(value);
      if (!DOMAIN.isExact(instant)) {
        throw new UnsupportedCell();
      }
      return DOMAIN.point(instant);
    });
  }

  private static void requireSupported(final ChangeLogSlab.Row row, final int column) {
    if (!row.supported(column)) {
      throw new UnsupportedCell();
    }
  }

  private static final class UnsupportedCell extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private UnsupportedCell() {
      super(null, null, false, false);
    }
  }

  private static void dependOn(final long[] dependencies, final int column) {
    dependencies[column >>> 6] |= 1L << (column & 63);
  }

  private static void buildColumns(final StorageEngineWriter writer, final IndexDef definition,
      final IndexDef projection, final ValidTimeConfig config, final ProjectionIndexMetadata metadata) {
    try (TemporalAggregateStorage storage = new TemporalAggregateStorage(writer, definition.getID())) {
      final byte[] prior = storage.reader().get(TemporalAggregateStorage.HEADER);
      if (prior != null) {
        throw new IllegalStateException("Temporal aggregate build/maintenance lifecycle mismatch");
      }
      final Header header = prior == null
          ? new Header(1, 0)
          : header(prior);
      final ProjectionIndexChangeListener listener = listener(writer, projection.getID());
      final TransactionState state = listener.aggregateState(definition.getID());
      state.atRevision(writer.getRevisionNumber());
      state.built = true;
      final Maintenance maintenance = new Maintenance(storage, definition.getTemporalAggregateSpec(), header);
      final Source current = new Source(writer, metadata, projection, config, listener::aggregateSourceRow);
      ProjectionAggregateRows.visit(writer, projection.getID(), metadata, (record, page, row) -> {
        final Row next = current.row(record, page, row, maintenance.spec);
        maintenance.change(null, next);
        state.rows.put(record, next);
      });
      maintenance.finish();
    }
  }

  /**
   * Header binds availability to this revision, including exactness and unsupported source values.
   */
  public record Header(long nextPart, long unsupportedRows) {
  }

  public static Header header(final byte[] bytes) {
    if (bytes.length != 20) {
      throw new IllegalStateException("Invalid temporal aggregate header length");
    }
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    if (buffer.getInt() != FORMAT) {
      throw new IllegalStateException("Unsupported temporal aggregate format");
    }
    final Header header = new Header(buffer.getLong(), buffer.getLong());
    if (header.nextPart() < 1 || header.unsupportedRows() < 0) {
      throw new IllegalStateException("Invalid temporal aggregate header");
    }
    return header;
  }

  public static byte[] headerKey() {
    return TemporalAggregateStorage.HEADER.clone();
  }

  private static int endpointColumn(final IndexDef projection, final String configuredPath) {
    final String path = projection.getProjectionRootPath() + "/" + ValidTimeConfig.toSlashSeparatedPath(configuredPath);
    for (int column = 0; column < projection.getProjectionFields().size(); column++) {
      if (path.equals(projection.getProjectionFields().get(column).toString())) {
        return column;
      }
    }
    throw new IllegalArgumentException("Covering projection omits valid-time endpoint " + path);
  }

  /** Last applied rows belong to one writer revision, including explicit pre-commit barriers. */
  public static final class TransactionState {
    private int revision = -1;
    private boolean built;
    private final Long2ObjectOpenHashMap<Row> rows = new Long2ObjectOpenHashMap<>();

    private void atRevision(final int current) {
      if (revision != current) {
        rows.clear();
        built = false;
        revision = current;
      }
    }
  }

  private static ProjectionIndexChangeListener listener(final StorageEngineWriter writer, final int projection) {
    final JsonResourceSession session = (JsonResourceSession) writer.getResourceSession();
    for (final var listener : session.getWtxIndexController(writer.getRevisionNumber()).getChangeListenerSnapshot()) {
      if (listener instanceof ProjectionIndexChangeListener source && source.indexDefId() == projection) {
        return source;
      }
    }
    throw new IllegalStateException("Missing temporal aggregate source listener");
  }

  // Rows compare primitive lanes explicitly through same(), avoiding record array identity.
  @SuppressWarnings("ArrayRecordComponent")
  private record Row(byte @Nullable [] group, long from, long to, long[] values, long present, boolean unsupported) {
    private static final Row UNSUPPORTED = new Row(null, 0, 0, new long[0], 0, true);
    private static final Row EMPTY = new Row(null, 0, 0, new long[0], 0, false);

    private boolean same(final @Nullable Row other) {
      return other != null && from == other.from && to == other.to && present == other.present
          && unsupported == other.unsupported && Arrays.equals(group, other.group)
          && Arrays.equals(values, other.values);
    }
  }

  private static final class Source {
    private final StorageEngineReader reader;
    private final TemporalAggregateColumns columns;
    private final boolean directObjectFields;
    private final int fromColumn;
    private final int toColumn;
    private final LongFunction<ProjectionIndexRowGroupPage> recoverRow;

    private Source(final StorageEngineReader reader, final ProjectionIndexMetadata metadata, final IndexDef projection,
        final ValidTimeConfig config, final LongFunction<ProjectionIndexRowGroupPage> recoverRow) {
      this.reader = reader;
      columns = new TemporalAggregateColumns(reader, metadata);
      directObjectFields = projection.getProjectionRootPath().toString().equals("/[]")
          && projection.getProjectionFields().stream().allMatch(path -> path.toString().matches("/\\[\\]/[^/]+"));
      fromColumn = endpointColumn(projection, config.getValidFromPath());
      toColumn = endpointColumn(projection, config.getValidToPath());
      this.recoverRow = recoverRow;
    }

    private static boolean poisoned(final ProjectionIndexRowGroupPage page, final int column) {
      return page.columnUnrepresentable(column) || page.columnNumericNonIntegral(column);
    }

    private Row row(final long record, final ProjectionIndexRowGroupPage original, final int offset,
        final TemporalAggregateSpec spec) {
      boolean hasField = false;
      for (int column = 0; column < original.getColumnCount(); column++) {
        hasField |= TemporalAggregateColumns.present(original, column, offset);
      }
      if ((!directObjectFields || !hasField)
          && reader.getRecord(record, IndexType.DOCUMENT, -1).getKind() != NodeKind.OBJECT) {
        return Row.UNSUPPORTED;
      }
      if (!TemporalAggregateColumns.present(original, fromColumn, offset)
          && !TemporalAggregateColumns.present(original, toColumn, offset)) {
        return Row.EMPTY;
      }
      ProjectionIndexRowGroupPage page = original;
      int row = offset;
      boolean recover = TemporalAggregateColumns.endpointNeedsRecovery(page, fromColumn)
          || TemporalAggregateColumns.endpointNeedsRecovery(page, toColumn);
      for (final int column : spec.groupColumns()) {
        recover |= poisoned(page, column);
      }
      for (final Lane lane : spec.lanes()) {
        recover |= poisoned(page, lane.left()) || (lane.right() >= 0 && poisoned(page, lane.right()));
      }
      if (recover) {
        // Leaf-wide flags are not row identity: recover exact row-local provenance before subtracting.
        page = recoverRow.apply(record);
        row = 0;
      }
      try {
        final Instant fromInstant = columns.endpoint(page, fromColumn, row);
        final Instant toInstant = columns.endpoint(page, toColumn, row);
        if ((fromInstant != null && !DOMAIN.isExact(fromInstant))
            || (toInstant != null && !DOMAIN.isExact(toInstant))) {
          return Row.UNSUPPORTED;
        }
        final long from = DOMAIN.lowerBound(fromInstant);
        final long to = toInstant == null
            ? DOMAIN.maxValue() + 1
            : DOMAIN.point(toInstant);
        if (from >= to) {
          return Row.EMPTY;
        }
        final byte[] group = columns.group(page, row, spec.groupColumns());
        final long[] values = new long[spec.lanes().size()];
        long present = 0;
        for (int at = 0; at < values.length; at++) {
          final Lane lane = spec.lanes().get(at);
          TemporalAggregateColumns.requireRepresentable(page, lane.left());
          if (lane.right() >= 0) {
            TemporalAggregateColumns.requireRepresentable(page, lane.right());
          }
          if (TemporalAggregateColumns.present(page, lane.left(), row)
              && (lane.right() < 0 || TemporalAggregateColumns.present(page, lane.right(), row))) {
            final long left = TemporalAggregateColumns.number(page, lane.left(), row);
            values[at] = lane.right() < 0
                ? left
                : Math.multiplyExact(left, TemporalAggregateColumns.number(page, lane.right(), row));
            present |= 1L << at;
          }
        }
        return new Row(group, from, to, values, present, false);
      } catch (final TemporalAggregateColumns.Unservable | DateTimeException | ArithmeticException decline) {
        return Row.UNSUPPORTED;
      }
    }

  }

  private static final class Maintenance {
    private final TemporalAggregateStorage storage;
    private final TemporalAggregateSpec spec;
    private final TemporalAggregateParts parts;
    private final TreeMap<byte[], Group> groups = new TreeMap<>(Arrays::compareUnsigned);
    private long endpointsRead;
    private long endpointsWritten;
    private long summaryNodesRead;
    private long summaryNodesWritten;
    private long nextPart;
    private long unsupported;

    private final class Group {
      private final @Nullable GroupHead head;
      private final TemporalAggregateParts.Builder delta = parts.builder();
      private long rows;

      private Group(final byte[] key) {
        final byte[] bytes = storage.reader().get(key);
        head = bytes == null
            ? null
            : GroupHead.decode(bytes);
        rows = head == null
            ? 0
            : head.rows();
      }
    }

    private Maintenance(final TemporalAggregateStorage storage, final TemporalAggregateSpec spec, final Header header) {
      this.storage = storage;
      this.spec = spec;
      parts = new TemporalAggregateParts(spec, DOMAIN.height());
      nextPart = header.nextPart();
      unsupported = header.unsupportedRows();
    }

    // Identity is the fast path before comparing the primitive lanes.
    @SuppressWarnings("ReferenceEquality")
    private void change(final @Nullable Row previous, final @Nullable Row next) {
      if (previous == next || (previous != null && previous.same(next))) {
        return;
      }
      add(previous, -1);
      add(next, 1);
    }

    private void add(final @Nullable Row row, final int sign) {
      if (row == null) {
        return;
      }
      if (row.unsupported) {
        unsupported = Math.addExact(unsupported, sign);
      } else if (row.group != null) {
        final Group group = groups.computeIfAbsent(row.group, Group::new);
        group.rows = Math.addExact(group.rows, sign);
        group.delta.add(row.from, row.to, row.values, row.present, sign);
      }
    }

    private static int tierForBytes(final int bytes) {
      final long chunks = (bytes + (long) TemporalAggregateParts.CHUNK_BYTES - 1) / TemporalAggregateParts.CHUNK_BYTES;
      return (Long.SIZE - 1 - Long.numberOfLeadingZeros(chunks)) / 4;
    }

    private void finish() {
      finish(null);
    }

    private void finish(final @Nullable ObjIntConsumer<byte[]> foldCandidate) {
      for (final Map.Entry<byte[], Group> entry : groups.entrySet()) {
        final Group group = entry.getValue();
        final GroupHead prior = group.head;
        if (group.delta.isEmpty()) {
          if (prior != null && group.rows != prior.rows()) {
            storage.put(entry.getKey(), new GroupHead(group.rows, prior.checkpointTier(), prior.parts(), prior.tiers(),
                prior.checkpointBytes(), prior.deltaBytes()).encode());
          }
          continue;
        }
        long[] ids = prior == null
            ? new long[0]
            : prior.parts();
        byte[] tiers = prior == null
            ? new byte[0]
            : prior.tiers();
        int checkpointTier = prior == null
            ? 0
            : prior.checkpointTier();
        byte[] encoded = group.delta.encode(prior == null);
        int tier = tierForBytes(encoded.length);
        boolean checkpoint = prior == null;
        while (!checkpoint && foldCandidate == null) {
          if (tier >= checkpointTier) {
            for (final long part : ids) {
              group.delta.merge(storage.reader().parts(), part);
            }
            encoded = group.delta.encode(true);
            checkpoint = true;
            break;
          }
          int count = 1;
          for (int index = 1; index < tiers.length; index++) {
            if (tiers[index] == tier) {
              count++;
            }
          }
          if (count < TemporalAggregateParts.FAN_IN) {
            break;
          }
          int retained = 1;
          for (int index = 1; index < ids.length; index++) {
            if (tiers[index] == tier) {
              group.delta.merge(storage.reader().parts(), ids[index]);
            } else {
              ids[retained] = ids[index];
              tiers[retained++] = tiers[index];
            }
          }
          ids = Arrays.copyOf(ids, retained);
          tiers = Arrays.copyOf(tiers, retained);
          encoded = group.delta.encode(false);
          tier = Math.max(tier + 1, tierForBytes(encoded.length));
        }
        final long id = nextPart;
        nextPart = Math.incrementExact(nextPart);
        if (checkpoint) {
          checkpointTier = tierForBytes(encoded.length) + 1;
          ids = new long[] {id};
          tiers = new byte[] {(byte) checkpointTier};
        } else {
          if (foldCandidate != null) {
            // Assign a free directory tier without reading or folding old parts. The separate
            // fold commit restores size tiers; point reads depend only on the part ids.
            final int[] counts = new int[TemporalAggregateParts.MAX_TIERS];
            for (int at = 1; at < tiers.length; at++) {
              counts[tiers[at]]++;
            }
            tier = 0;
            while (tier < counts.length && counts[tier] >= TemporalAggregateParts.FAN_IN - 1) {
              tier++;
            }
            if (tier == counts.length) {
              throw new IllegalStateException("Derived group must be folded before appending another part");
            }
            checkpointTier = Math.max(checkpointTier, tier + 1);
            tiers[0] = (byte) checkpointTier;
          }
          ids = Arrays.copyOf(ids, ids.length + 1);
          tiers = Arrays.copyOf(tiers, tiers.length + 1);
          ids[ids.length - 1] = id;
          tiers[tiers.length - 1] = (byte) tier;
        }
        storage.putPart(id, encoded);
        final long checkpointBytes = checkpoint
            ? encoded.length
            : Objects.requireNonNull(prior).checkpointBytes();
        final long deltaBytes = checkpoint
            ? 0
            : Math.addExact(Objects.requireNonNull(prior).deltaBytes(), encoded.length);
        storage.put(entry.getKey(),
            new GroupHead(group.rows, checkpointTier, ids, tiers, checkpointBytes, deltaBytes).encode());
        if (foldCandidate != null && (ids.length > TemporalAggregateParts.FAN_IN
            || ids.length > TemporalAggregateParts.MAX_PARTS - 24 || deltaBytes > checkpointBytes / 2)) {
          foldCandidate.accept(entry.getKey(), ids.length);
        }
        endpointsRead += group.delta.endpointsRead();
        endpointsWritten += group.delta.endpointsWritten();
        summaryNodesRead += group.delta.summaryNodesRead();
        summaryNodesWritten += group.delta.summaryNodesWritten();
      }
      if (unsupported < 0 || nextPart < 1) {
        throw new IllegalStateException("Invalid temporal aggregate maintenance counters");
      }
      storage.put(TemporalAggregateStorage.HEADER,
          ByteBuffer.allocate(20).putInt(FORMAT).putLong(nextPart).putLong(unsupported).array());
    }
  }
}
