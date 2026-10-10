package io.sirix.index.aggregate;

import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.StorageEngineWriter;
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

/** Declaration validation, initial column build, and maintenance driven by the projection's dirty rows. */
public final class TemporalAggregateIndexes {
  private static final int FORMAT = 1;
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
      final Maintenance maintenance = new Maintenance(storage, definition.getTemporalAggregateSpec(),
          new TemporalAggregateColumns(writer, metadata), header, endpointColumn(projection, config.getValidFromPath()),
          endpointColumn(projection, config.getValidToPath()));
      ProjectionAggregateRows.visit(writer, projection.getID(), metadata, dirty, maintenance::replace);
      maintenance.finish();
      if (!build) {
        lastMaintenance = new MaintenanceWork(Objects.requireNonNull(dirty).size(), storage.reader().nodesRead(),
            storage.nodesWritten());
      }
    }
  }

  /** Header binds availability to this revision, including exactness and unsupported source values. */
  public record Header(long nextGroup, long unsupportedRows) {
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
    if (header.nextGroup() < 1 || header.unsupportedRows() < 0) {
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

  private static final class Maintenance {
    private final TemporalAggregateStorage storage;
    private final TemporalAggregateSpec spec;
    private final TemporalAggregateColumns columns;
    private final int fromColumn;
    private final int toColumn;
    private final Long2ObjectOpenHashMap<TemporalAggregateTree.Batch> batches = new Long2ObjectOpenHashMap<>();
    private long nextGroup;
    private long unsupported;

    private Maintenance(final TemporalAggregateStorage storage, final TemporalAggregateSpec spec,
        final TemporalAggregateColumns columns, final Header header, final int fromColumn, final int toColumn) {
      this.storage = storage;
      this.spec = spec;
      this.columns = columns;
      this.nextGroup = header.nextGroup();
      this.unsupported = header.unsupportedRows();
      this.fromColumn = fromColumn;
      this.toColumn = toColumn;
    }

    private TemporalAggregateTree.Batch batch(final long group) {
      return batches.computeIfAbsent(group,
          id -> new TemporalAggregateTree(storage.tree(id), DOMAIN.height(), spec.lanes()).batch());
    }

    private void replace(final long record, final @Nullable ProjectionIndexRowGroupPage page, final int row) {
      final byte[] key = TemporalAggregateStorage.recordKey(record);
      final byte[] prior = storage.reader().get(key);
      if (prior != null) {
        if (prior.length != 24) {
          throw new IllegalStateException("Malformed temporal aggregate row membership");
        }
        final ByteBuffer buffer = ByteBuffer.wrap(prior);
        final long group = buffer.getLong();
        final long from = buffer.getLong();
        final long to = buffer.getLong();
        if (group == 0) {
          unsupported--;
        } else if (group > 0) {
          batch(group).replace(record, from, to, null);
        }
      }
      if (page == null) {
        storage.put(key, new byte[0]);
        return;
      }
      if (storage.getStorageEngineReader().getRecord(record, IndexType.DOCUMENT, -1).getKind() != NodeKind.OBJECT) {
        unsupported(key);
        return;
      }
      try {
        final String fromText = columns.string(page, fromColumn, row);
        final String toText = columns.string(page, toColumn, row);
        final Instant fromInstant = fromText == null ? null : Instant.parse(fromText);
        final Instant toInstant = toText == null ? null : Instant.parse(toText);
        if ((fromInstant != null && !DOMAIN.isExact(fromInstant)) || (toInstant != null && !DOMAIN.isExact(toInstant))) {
          unsupported(key);
          return;
        }
        final long from = DOMAIN.lowerBound(fromInstant);
        final long to = toInstant == null ? DOMAIN.maxValue() + 1 : DOMAIN.point(toInstant);
        if (from >= to) {
          storage.put(key, ByteBuffer.allocate(24).putLong(-1).putLong(from).putLong(to).array());
          return;
        }
        final byte[] groupKey = columns.group(page, row, spec.groupColumns());
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
        final byte[] groupBytes = storage.reader().get(groupKey);
        final long group;
        if (groupBytes == null) {
          group = nextGroup;
          nextGroup = Math.incrementExact(nextGroup);
          storage.put(groupKey, ByteBuffer.allocate(8).putLong(group).array());
        } else {
          group = ByteBuffer.wrap(groupBytes).getLong();
        }
        batch(group).replace(record, from, to, AggregateSummary.row(values, present));
        storage.put(key, ByteBuffer.allocate(24).putLong(group).putLong(from).putLong(to).array());
      } catch (final TemporalAggregateColumns.Unservable | DateTimeException | ArithmeticException decline) {
        unsupported(key);
      }
    }

    private void unsupported(final byte[] key) {
      unsupported = Math.incrementExact(unsupported);
      storage.put(key, new byte[24]);
    }

    private void finish() {
      final long[] groups = batches.keySet().toLongArray();
      Arrays.sort(groups);
      for (final long group : groups) {
        batches.get(group).apply();
      }
      if (unsupported < 0) {
        throw new IllegalStateException("Negative unsupported temporal row count");
      }
      storage.put(TemporalAggregateStorage.HEADER,
          ByteBuffer.allocate(20).putInt(FORMAT).putLong(nextGroup).putLong(unsupported).array());
    }
  }
}
