package io.sirix.index.aggregate;

import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.sirix.index.aggregate.TemporalAggregateStorage.GroupHead;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.index.aggregate.TemporalAggregateSpec.Lane;
import io.sirix.index.projection.ProjectionAggregateRows;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Map;

/** Declaration validation, initial column build, and maintenance driven by the projection's dirty rows. */
public final class TemporalAggregateIndexes {
  private static final int FORMAT = 2;
  private static final TemporalAggregateDomain DOMAIN = new TemporalAggregateDomain();
  private static volatile MaintenanceWork lastMaintenance = new MaintenanceWork(0, 0, 0);

  public record MaintenanceWork(long changedRecords, long nodesRead, long nodesWritten) {
  }

  public static MaintenanceWork lastMaintenance() {
    return lastMaintenance;
  }

  private TemporalAggregateIndexes() {
  }

  public static void validate(final IndexDef aggregate, final IndexDef projection, final ValidTimeConfig config) {
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
    try {
      apply(writer, definition, projection, config, null, true);
    } catch (final RuntimeException | Error failure) {
      writer.markTransactionRollbackOnly(failure);
      throw failure;
    }
  }

  /** Whether a projection needs to retain dirty identities for commit-time aggregate maintenance. */
  public static boolean hasDependents(final StorageEngineWriter writer, final IndexDef projection) {
    if (!(writer.getResourceSession() instanceof JsonResourceSession session)) {
      return false;
    }
    for (final IndexDef definition : session.getWtxIndexController(writer.getRevisionNumber()).getIndexes().getIndexDefs()) {
      if (definition.isTemporalAggregateIndex()
          && definition.getTemporalAggregateSpec().projectionId() == projection.getID()) {
        return true;
      }
    }
    return false;
  }

  /** Called only after the projection has installed its final rows for this maintenance batch. */
  public static void maintain(final StorageEngineWriter writer, final IndexDef projection, final LongSet dirty,
      final Long2ObjectOpenHashMap<long[]> changedColumns) {
    if (!(writer.getResourceSession() instanceof JsonResourceSession session)) {
      return;
    }
    final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
    for (final IndexDef definition : controller.getIndexes().getIndexDefs()) {
      if (definition.isTemporalAggregateIndex()
          && definition.getTemporalAggregateSpec().projectionId() == projection.getID()) {
        final ValidTimeConfig config = Objects.requireNonNull(session.getResourceConfig().getValidTimeConfig());
        final TemporalAggregateSpec spec = definition.getTemporalAggregateSpec();
        final long[] dependencies = new long[(projection.getProjectionFields().size() + 63) >>> 6];
        dependOn(dependencies, endpointColumn(projection, config.getValidFromPath()));
        dependOn(dependencies, endpointColumn(projection, config.getValidToPath()));
        for (final int column : spec.groupColumns()) {
          dependOn(dependencies, column);
        }
        for (final Lane lane : spec.lanes()) {
          dependOn(dependencies, lane.left());
          if (lane.right() >= 0) {
            dependOn(dependencies, lane.right());
          }
        }
        final LongOpenHashSet affected = new LongOpenHashSet(dirty.size());
        final var records = dirty.iterator();
        while (records.hasNext()) {
          final long record = records.nextLong();
          final long[] changed = changedColumns.get(record);
          // Unknown provenance and structural events must never be filtered out.
          if (changed == null) {
            affected.add(record);
          } else {
            for (int word = 0; word < dependencies.length; word++) {
              if (word >= changed.length || (dependencies[word] & changed[word]) != 0) {
                affected.add(record);
                break;
              }
            }
          }
        }
        if (!affected.isEmpty()) {
          apply(writer, definition, projection, config, affected, false);
        } else {
          lastMaintenance = new MaintenanceWork(0, 0, 0);
        }
      }
    }
  }

  private static void dependOn(final long[] dependencies, final int column) {
    dependencies[column >>> 6] |= 1L << (column & 63);
  }

  private static void apply(final StorageEngineWriter writer, final IndexDef definition, final IndexDef projection,
      final ValidTimeConfig config, final @Nullable LongSet dirty, final boolean build) {
    final ProjectionIndexMetadata metadata = Objects.requireNonNull(ProjectionIndexMetadata.parse(
        ProjectionIndexHOTStorage.readMetadataBlob(writer, projection.getID())), "Missing covering projection");
    if (metadata.isStale()) {
      throw new IllegalStateException("Cannot maintain a temporal aggregate from a stale projection");
    }
    try (TemporalAggregateStorage storage = new TemporalAggregateStorage(writer, definition.getID())) {
      final byte[] prior = storage.reader().get(TemporalAggregateStorage.HEADER);
      if (build != (prior == null)) {
        throw new IllegalStateException("Temporal aggregate build/maintenance lifecycle mismatch");
      }
      final Header header = prior == null ? new Header(1, 0) : header(prior);
      final ProjectionIndexChangeListener listener = listener(writer, projection.getID());
      final TransactionState state = listener.aggregateState(definition.getID());
      state.atRevision(writer.getRevisionNumber());
      final Maintenance maintenance = new Maintenance(storage, definition.getTemporalAggregateSpec(), header,
          writer.getRevisionNumber());
      try (Source current = new Source(writer, metadata, projection, config, listener)) {
        if (!build) {
          final LongOpenHashSet uncached = new LongOpenHashSet(Objects.requireNonNull(dirty));
          uncached.removeAll(state.rows.keySet());
          if (!uncached.isEmpty()) {
            final JsonResourceSession session = (JsonResourceSession) writer.getResourceSession();
            try (StorageEngineReader previous = session.createStorageEngineReader(writer.getRevisionToRepresent())) {
              final ProjectionIndexMetadata oldMetadata = ProjectionIndexMetadata.parse(
                  ProjectionIndexHOTStorage.readMetadataBlob(previous, projection.getID()));
              if (oldMetadata != null) {
                if (oldMetadata.isStale()) {
                  throw new IllegalStateException("Previous aggregate projection is stale");
                }
                try (Source old = new Source(previous, oldMetadata, projection, config, null)) {
                  ProjectionAggregateRows.visit(previous, projection.getID(), oldMetadata, uncached,
                      (record, page, row) -> state.rows.put(record, old.row(record, page, row, maintenance.spec)));
                }
              }
            }
          }
        }
        ProjectionAggregateRows.visit(writer, projection.getID(), metadata, dirty, (record, page, row) -> {
          final Row next = current.row(record, page, row, maintenance.spec);
          maintenance.change(build ? null : state.rows.get(record), next);
          state.rows.put(record, next);
        });
      }
      maintenance.finish();
      if (!build) {
        lastMaintenance = new MaintenanceWork(Objects.requireNonNull(dirty).size(), storage.reader().nodesRead(),
            storage.nodesWritten());
      }
    }
  }

  /** Header binds availability to this revision, including exactness and unsupported source values. */
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
    private final Long2ObjectOpenHashMap<Row> rows = new Long2ObjectOpenHashMap<>();

    private void atRevision(final int current) {
      if (revision != current) {
        rows.clear();
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

  private record Row(byte @Nullable [] group, long from, long to, long[] values, long present, boolean unsupported) {
    private static final Row UNSUPPORTED = new Row(null, 0, 0, new long[0], 0, true);
    private static final Row EMPTY = new Row(null, 0, 0, new long[0], 0, false);

    private boolean same(final @Nullable Row other) {
      return other != null && from == other.from && to == other.to && present == other.present
          && unsupported == other.unsupported && Arrays.equals(group, other.group) && Arrays.equals(values, other.values);
    }
  }

  private static final class Source implements AutoCloseable {
    private final StorageEngineReader reader;
    private final TemporalAggregateColumns columns;
    private final IndexDef projection;
    private final int fromColumn;
    private final int toColumn;
    private final @Nullable ProjectionIndexChangeListener listener;
    private @Nullable JsonNodeReadOnlyTrx transaction;
    private @Nullable PathSummaryReader paths;

    private Source(final StorageEngineReader reader, final ProjectionIndexMetadata metadata,
        final IndexDef projection, final ValidTimeConfig config, final @Nullable ProjectionIndexChangeListener listener) {
      this.reader = reader;
      columns = new TemporalAggregateColumns(reader, metadata);
      this.projection = projection;
      fromColumn = endpointColumn(projection, config.getValidFromPath());
      toColumn = endpointColumn(projection, config.getValidToPath());
      this.listener = listener;
    }

    private static boolean poisoned(final ProjectionIndexRowGroupPage page, final int column) {
      return page.columnUnrepresentable(column) || page.columnNumericNonIntegral(column);
    }

    private @Nullable Row row(final long record, final @Nullable ProjectionIndexRowGroupPage original, final int offset,
        final TemporalAggregateSpec spec) {
      if (original == null) {
        return null;
      }
      if (reader.getRecord(record, IndexType.DOCUMENT, -1).getKind() != NodeKind.OBJECT) {
        return Row.UNSUPPORTED;
      }
      ProjectionIndexRowGroupPage page = original;
      int row = offset;
      boolean recover = poisoned(page, fromColumn) || poisoned(page, toColumn);
      for (final int column : spec.groupColumns()) {
        recover |= poisoned(page, column);
      }
      for (final Lane lane : spec.lanes()) {
        recover |= poisoned(page, lane.left()) || (lane.right() >= 0 && poisoned(page, lane.right()));
      }
      if (recover) {
        // Leaf-wide flags are not row identity: recover exact row-local provenance before subtracting.
        if (listener != null) {
          page = listener.aggregateSourceRow(record);
        } else {
          if (transaction == null) {
            final JsonResourceSession session = (JsonResourceSession) reader.getResourceSession();
            transaction = session.beginNodeReadOnlyTrx(reader.getRevisionNumber());
            paths = session.openPathSummary(reader.getRevisionNumber());
          }
          page = ProjectionAggregateRows.extract(projection, Objects.requireNonNull(paths), transaction, record);
        }
        row = 0;
      }
      try {
        final String fromText = columns.string(page, fromColumn, row);
        final String toText = columns.string(page, toColumn, row);
        final Instant fromInstant = fromText == null ? null : Instant.parse(fromText);
        final Instant toInstant = toText == null ? null : Instant.parse(toText);
        if ((fromInstant != null && !DOMAIN.isExact(fromInstant)) || (toInstant != null && !DOMAIN.isExact(toInstant))) {
          return Row.UNSUPPORTED;
        }
        final long from = DOMAIN.lowerBound(fromInstant);
        final long to = toInstant == null ? DOMAIN.maxValue() + 1 : DOMAIN.point(toInstant);
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
            values[at] = lane.right() < 0 ? left
                : Math.multiplyExact(left, TemporalAggregateColumns.number(page, lane.right(), row));
            present |= 1L << at;
          }
        }
        return new Row(group, from, to, values, present, false);
      } catch (final TemporalAggregateColumns.Unservable | DateTimeException | ArithmeticException decline) {
        return Row.UNSUPPORTED;
      }
    }

    @Override
    public void close() {
      try {
        if (paths != null) {
          paths.close();
        }
      } finally {
        if (transaction != null) {
          transaction.close();
        }
      }
    }
  }

  private static final class Maintenance {
    private final TemporalAggregateStorage storage;
    private final TemporalAggregateSpec spec;
    private final TemporalAggregateParts parts;
    private final TreeMap<byte[], Group> groups = new TreeMap<>(Arrays::compareUnsigned);
    private final int revision;
    private long nextPart;
    private long unsupported;

    private final class Group {
      private final @Nullable GroupHead head;
      private final TemporalAggregateParts.Builder delta = parts.builder();
      private long rows;

      private Group(final byte[] key) {
        final byte[] bytes = storage.reader().get(key);
        head = bytes == null ? null : GroupHead.decode(bytes);
        rows = head == null ? 0 : head.rows();
      }
    }

    private Maintenance(final TemporalAggregateStorage storage, final TemporalAggregateSpec spec,
        final Header header, final int revision) {
      this.storage = storage;
      this.spec = spec;
      parts = new TemporalAggregateParts(spec, DOMAIN.height());
      nextPart = header.nextPart();
      unsupported = header.unsupportedRows();
      this.revision = revision;
    }

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

    private void finish() {
      for (final Map.Entry<byte[], Group> entry : groups.entrySet()) {
        final Group group = entry.getValue();
        if (group.delta.isEmpty()) {
          continue;
        }
        final GroupHead prior = group.head;
        final long[] previous = prior == null ? new long[0] : prior.parts();
        final boolean sameRevision = prior != null && prior.revision() == revision;
        final boolean checkpoint = previous.length == 0 || previous.length == TemporalAggregateParts.MAX_PARTS;
        final long[] ids;
        final long id;
        if (checkpoint) {
          for (final long part : previous) {
            group.delta.merge(storage.reader().parts(), part);
          }
          id = nextPart;
          nextPart = Math.incrementExact(nextPart);
          ids = new long[] {id};
        } else if (sameRevision) {
          // A second explicit barrier replaces this revision's part, never growing its read fan-out.
          id = previous[previous.length - 1];
          group.delta.merge(storage.reader().parts(), id);
          ids = previous;
        } else {
          id = nextPart;
          nextPart = Math.incrementExact(nextPart);
          ids = Arrays.copyOf(previous, previous.length + 1);
          ids[previous.length] = id;
        }
        storage.putPart(id, group.delta.encode(checkpoint));
        storage.put(entry.getKey(), new GroupHead(group.rows, revision, ids).encode());
      }
      if (unsupported < 0 || nextPart < 1) {
        throw new IllegalStateException("Invalid temporal aggregate maintenance counters");
      }
      storage.put(TemporalAggregateStorage.HEADER,
          ByteBuffer.allocate(20).putInt(FORMAT).putLong(nextPart).putLong(unsupported).array());
    }
  }
}
