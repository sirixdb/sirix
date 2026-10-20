package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Int;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.aggregate.AggregateSummary;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.TemporalAggregateIndexes;
import io.sirix.index.aggregate.TemporalAggregateSpec;
import io.sirix.index.aggregate.TemporalAggregateSpec.Lane;
import io.sirix.index.aggregate.TemporalAggregateStorage;
import io.sirix.index.aggregate.TemporalAggregateParts;
import io.sirix.index.aggregate.TemporalAggregateDomain;
import io.sirix.index.projection.ProjectionIndexByteScan;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.node.json.ArrayNode;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.ColumnarRecordSequence;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/** Exact point admission before the valid-time stab and masked grouped kernel. */
public final class TemporalAggregateRoute {
  private static final Logger LOGGER = LoggerFactory.getLogger(TemporalAggregateRoute.class);

  private static final class DomainHolder {
    private static final TemporalAggregateDomain DOMAIN = new TemporalAggregateDomain();
  }

  private static final DateTimeToInstant INSTANT = new DateTimeToInstant();
  private static final LongAdder SERVED = new LongAdder();

  private TemporalAggregateRoute() {}

  public static long servedCount() {
    return SERVED.sum();
  }

  record Point(JsonDBItem document, Instant instant) {
  }

  static SirixVectorizedExecutor.@Nullable ServedGroups serve(final QueryContext ctx, final Tuple tuple,
      final @Nullable Point point, final RoutedGroupRequest request) {
    return serve(ctx, tuple, point, request, null);
  }

  static final class OutputOrder {
    private final int[] indexes;
    private final boolean[] ascending;
    private final boolean[] emptyLeast;

    OutputOrder(final int[] indexes, final boolean[] ascending, final boolean[] emptyLeast) {
      this.indexes = indexes;
      this.ascending = ascending;
      this.emptyLeast = emptyLeast;
    }
  }

  static SirixVectorizedExecutor.@Nullable ServedGroups serve(final QueryContext ctx, final Tuple tuple,
      final @Nullable Point point, final RoutedGroupRequest request, final @Nullable OutputOrder order) {
    if (request.predicate() != null || request.sourcePath().length != 1 || !"[]".equals(request.sourcePath()[0])) {
      return null;
    }
    if (point == null) {
      return null;
    }
    final JsonDBItem document = point.document();
    final StorageEngineReader reader = document.getTrx().getStorageEngineReader();
    if (reader instanceof StorageEngineWriter || reader.hasTrxIntentLog()) {
      return null;
    }
    final Indexes indexes =
        document.getResourceSession().getRtxIndexController(reader.getRevisionNumber()).getIndexes();
    if (indexes.getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE) == 0) {
      return null;
    }
    if (!(document instanceof JsonDBArray)
        || !(reader.getRecord(document.getNodeKey(), IndexType.DOCUMENT, -1) instanceof ArrayNode array)
        || array.getParentKey() != 0) {
      return null;
    }
    if (!DomainHolder.DOMAIN.isExact(point.instant())) {
      return null;
    }
    if (!DerivedIndexMaintainer.enabled(reader.getResourceSession().getResourceConfig())) {
      return serveSnapshot(reader, reader, indexes, array, point, request, order);
    }
    try {
      final StorageEngineReader derived = ctx instanceof SirixQueryContext context
          ? context.borrowDerivedAggregateReader(reader)
          : reader.getDerivedIndexReader();
      return derived == null
          ? null
          : serveSnapshot(reader, derived, indexes, array, point, request, order);
    } catch (final RuntimeException unavailable) {
      LOGGER.warn("Derived aggregate snapshot unavailable at source revision {}; using exact fallback",
          reader.getRevisionNumber(), unavailable);
      return null;
    }
  }

  private static SirixVectorizedExecutor.@Nullable ServedGroups serveSnapshot(final StorageEngineReader reader,
      final StorageEngineReader aggregateReader, final Indexes indexes, final ArrayNode array, final Point point,
      final RoutedGroupRequest request, final @Nullable OutputOrder order) {
    final List<IndexDef> candidates = new ArrayList<>();
    for (final IndexDef definition : indexes.getIndexDefs()) {
      if (definition.isTemporalAggregateIndex()) {
        candidates.add(definition);
      }
    }
    candidates.sort(Comparator.comparingInt(definition -> definition.getTemporalAggregateSpec().groupColumns().size()));
    for (final IndexDef definition : candidates) {
      final TemporalAggregateSpec spec = definition.getTemporalAggregateSpec();
      final ProjectionIndexMetadata metadata = projectionMetadata(reader, indexes, spec);
      if (metadata == null) {
        continue;
      }
      final String[] columns = metadata.fieldChains();
      final int[] groups = groupMapping(request.groupFields(), spec, columns);
      final int[] lanes = laneMapping(request, spec, columns);
      if (groups == null || lanes == null) {
        continue;
      }
      final SirixVectorizedExecutor.ServedGroups served = serveGroups(aggregateReader, definition, array, point,
          request, groups, lanes, primitiveGroups(metadata, spec, groups, request)
              ? order
              : null);
      if (served != null) {
        return served;
      }
    }
    return null;
  }

  private static @Nullable ProjectionIndexMetadata projectionMetadata(final StorageEngineReader reader,
      final Indexes indexes, final TemporalAggregateSpec spec) {
    final IndexDef projection = indexes.getIndexDef(spec.projectionId(), IndexType.PROJECTION);
    if (projection == null || !"/[]".equals(projection.getProjectionRootPath().toString())) {
      return null;
    }
    final ProjectionIndexMetadata metadata =
        ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readMetadataBlob(reader, spec.projectionId()));
    if (metadata == null || metadata.isStale()) {
      return null;
    }
    return metadata;
  }

  private static SirixVectorizedExecutor.@Nullable ServedGroups serveGroups(final StorageEngineReader aggregateReader,
      final IndexDef definition, final ArrayNode array, final Point point, final RoutedGroupRequest request,
      final int[] groups, final int[] lanes, final @Nullable OutputOrder order) {
    final TemporalAggregateSpec spec = definition.getTemporalAggregateSpec();
    try (TemporalAggregateStorage.Reader storage =
        new TemporalAggregateStorage.Reader(aggregateReader, definition.getID())) {
      final byte[] header = storage.get(TemporalAggregateIndexes.headerKey());
      if (header == null || TemporalAggregateIndexes.header(header).unsupportedRows() != 0) {
        return null;
      }
      final long sourceRows = point.document().getResourceSession().getResourceConfig().storeChildCount()
          ? array.getChildCount()
          : ((JsonDBArray) point.document()).length().longValue();
      return order != null
          ? primitiveGroups(storage, spec, sourceRows, point, request, groups, lanes, order)
          : objectGroups(storage, spec, sourceRows, point, request, groups, lanes);
    }
  }

  private static SirixVectorizedExecutor.@Nullable ServedGroups objectGroups(
      final TemporalAggregateStorage.Reader storage, final TemporalAggregateSpec spec, final long sourceRows,
      final Point point, final RoutedGroupRequest request, final int[] groups, final int[] lanes) {
    final Map<List<Sequence>, AggregateSummary> results = new LinkedHashMap<>();
    final long coordinate = DomainHolder.DOMAIN.point(point.instant());
    final TemporalAggregateParts parts = new TemporalAggregateParts(spec, DomainHolder.DOMAIN.height());
    final boolean admitted = storage.groups(sourceRows, (key, group) -> {
      if (group.rows() == 0) {
        return;
      }
      final AggregateSummary summary = parts.point(storage.parts(), group.parts(), coordinate);
      if (summary.count() == 0) {
        return;
      }
      final Sequence[] allKeys = groupValues(key, spec.groupColumns().size());
      final Sequence[] selected = new Sequence[groups.length];
      for (int i = 0; i < groups.length; i++) {
        selected[i] = allKeys[groups[i]];
      }
      final List<Sequence> groupKey = Arrays.asList(selected);
      final AggregateSummary previous = results.putIfAbsent(groupKey, summary);
      if (previous != null) {
        previous.add(summary);
      }
    });
    if (!admitted) {
      return null;
    }
    return servedGroups(request, groups, lanes, results);
  }

  /** A full permutation of integral maintained keys needs neither key objects nor coalescing. */
  private static boolean primitiveGroups(final ProjectionIndexMetadata metadata, final TemporalAggregateSpec spec,
      final int[] groups, final RoutedGroupRequest request) {
    for (final String function : request.funcs()) {
      if (!"count".equals(function) && !"min".equals(function) && !"max".equals(function)) {
        return false;
      }
    }
    if (groups.length != spec.groupColumns().size()) {
      return false;
    }
    final boolean[] seen = new boolean[groups.length];
    final byte[] kinds = metadata.columnKinds();
    for (final int group : groups) {
      if (seen[group]
          || kinds[spec.groupColumns().get(group)] != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        return false;
      }
      seen[group] = true;
    }
    return true;
  }

  private static SirixVectorizedExecutor.@Nullable ServedGroups primitiveGroups(
      final TemporalAggregateStorage.Reader storage, final TemporalAggregateSpec spec, final long sourceRows,
      final Point point, final RoutedGroupRequest request, final int[] groups, final int[] lanes,
      final OutputOrder order) {
    final QNm[] names = outputNames(request);
    final ColumnarRecordSequence.Builder columns =
        new ColumnarRecordSequence.Builder(names, new boolean[names.length], 256);
    final long coordinate = DomainHolder.DOMAIN.point(point.instant());
    final TemporalAggregateParts parts = new TemporalAggregateParts(spec, DomainHolder.DOMAIN.height());
    final long[] keys = new long[groups.length];
    final boolean[] present = new boolean[groups.length];
    final boolean[] supported = {true};
    final boolean admitted = storage.groups(sourceRows, (key, group) -> {
      if (!supported[0] || group.rows() == 0) {
        return;
      }
      final AggregateSummary summary = parts.point(storage.parts(), group.parts(), coordinate);
      if (summary.count() == 0) {
        return;
      }
      final ByteBuffer buffer = ByteBuffer.wrap(key);
      if (buffer.get() != 1) {
        throw new IllegalStateException("Invalid temporal group key");
      }
      for (int index = 0; index < keys.length; index++) {
        final byte kind = buffer.get();
        if (kind != 0 && kind != 1) {
          supported[0] = false;
          return;
        }
        present[index] = kind == 1;
        keys[index] = kind == 1
            ? buffer.getLong()
            : 0;
      }
      if (buffer.hasRemaining()) {
        throw new IllegalStateException("Trailing temporal group key bytes");
      }
      final int row = columns.addRow();
      for (int index = 0; index < groups.length; index++) {
        if (present[groups[index]]) {
          columns.setLong(index, row, keys[groups[index]]);
        }
      }
      for (int index = 0; index < lanes.length; index++) {
        final int lane = lanes[index];
        final String function = request.funcs()[index];
        if (("min".equals(function) || "max".equals(function)) && summary.present(lane) == 0) {
          continue;
        }
        final long value = switch (function) {
          case "count" -> lane < 0
              ? summary.count()
              : summary.present(lane);
          case "min" -> summary.min(lane);
          case "max" -> summary.max(lane);
          default -> throw new IllegalStateException("Unadmitted aggregate function");
        };
        columns.setLong(groups.length + index, row, value);
      }
    });
    if (!admitted) {
      return null;
    }
    if (!supported[0]) {
      return objectGroups(storage, spec, sourceRows, point, request, groups, lanes);
    }
    final int[] rows = columns.orderByLongs(order.indexes, order.ascending, order.emptyLeast);
    SERVED.increment();
    return new SirixVectorizedExecutor.ServedGroups(columns.build(rows), true);
  }

  private static QNm[] outputNames(final RoutedGroupRequest request) {
    final QNm[] names = new QNm[request.keyNames().length + request.outNames().length];
    for (int i = 0; i < request.keyNames().length; i++) {
      names[i] = new QNm(request.keyNames()[i]);
    }
    for (int i = 0; i < request.outNames().length; i++) {
      names[request.keyNames().length + i] = new QNm(request.outNames()[i]);
    }
    return names;
  }

  private static SirixVectorizedExecutor.ServedGroups servedGroups(final RoutedGroupRequest request, final int[] groups,
      final int[] lanes, final Map<List<Sequence>, AggregateSummary> results) {
    final QNm[] names = outputNames(request);
    final List<Item> items = new ArrayList<>(results.size());
    for (final Map.Entry<List<Sequence>, AggregateSummary> entry : results.entrySet()) {
      final Sequence[] values = new Sequence[names.length];
      for (int i = 0; i < groups.length; i++) {
        values[i] = entry.getKey().get(i);
      }
      for (int i = 0; i < lanes.length; i++) {
        values[groups.length + i] = aggregate(entry.getValue(), request.funcs()[i], lanes[i]);
      }
      items.add(new ArrayObject(names, values));
    }
    SERVED.increment();
    return new SirixVectorizedExecutor.ServedGroups(new ItemSequence(items.toArray(Item[]::new)), false);
  }

  static @Nullable Point point(final QueryContext ctx, final Tuple tuple,
      final SirixGroupAggregateExpr.RoutedSource routed) {
    final Item tx = Objects.requireNonNull(routed.txTime()).evaluateToItem(ctx, tuple);
    final Item valid = routed.validTime().evaluateToItem(ctx, tuple);
    if (!(tx instanceof DateTime txTime) || !(valid instanceof DateTime validTime) || txTime.getTimezone() == null
        || validTime.getTimezone() == null) {
      return null;
    }
    try {
      final Instant transaction = INSTANT.convert(txTime);
      if (!DomainHolder.DOMAIN.isExact(transaction)
          || !(ctx.getJsonItemStore().lookup(routed.database()) instanceof JsonDBCollection collection)) {
        return null;
      }
      final JsonDBItem document = collection.getDocument(routed.resource(), transaction);
      return document == null || document.getResourceSession().getResourceConfig().getValidTimeConfig() == null
          ? null
          : new Point(document, INSTANT.convert(validTime));
    } catch (final RuntimeException unservable) {
      // Preserve the routed opener's normal fallback and error semantics.
      return null;
    }
  }

  private static int @Nullable [] groupMapping(final String[] fields, final TemporalAggregateSpec spec,
      final String[] columns) {
    final int[] result = new int[fields.length];
    for (int i = 0; i < fields.length; i++) {
      result[i] = -1;
      for (int j = 0; j < spec.groupColumns().size(); j++) {
        if (fields[i].equals(columns[spec.groupColumns().get(j)])) {
          result[i] = j;
          break;
        }
      }
      if (result[i] < 0) {
        return null;
      }
    }
    return result;
  }

  private static int @Nullable [] laneMapping(final RoutedGroupRequest request, final TemporalAggregateSpec spec,
      final String[] columns) {
    final int[] result = new int[request.funcs().length];
    for (int i = 0; i < result.length; i++) {
      final String function = request.funcs()[i];
      if (!"count".equals(function) && !"sum".equals(function) && !"min".equals(function) && !"max".equals(function)) {
        return null;
      }
      final String field = request.aggFields()[i];
      if (field == null) {
        if (!"count".equals(function)) {
          return null;
        }
        result[i] = -1;
        continue;
      }
      final Lane lane = requestedLane(field, request.computedLanes(), columns);
      result[i] = -1;
      if (lane != null) {
        for (int j = 0; j < spec.lanes().size(); j++) {
          final Lane declared = spec.lanes().get(j);
          if (declared.sameOperand(lane) && (declared.operations() & TemporalAggregateSpec.operation(function)) != 0) {
            result[i] = j;
            break;
          }
        }
      }
      if (result[i] < 0) {
        return null;
      }
    }
    return result;
  }

  private static @Nullable Lane requestedLane(final String field,
      final SirixVectorizedExecutor.ComputedLane @Nullable [] computed, final String[] columns) {
    if (!field.startsWith(SirixVectorizedExecutor.COMPUTED_LANE_PREFIX)) {
      final int column = Arrays.asList(columns).indexOf(field);
      return column < 0
          ? null
          : new Lane(column);
    }
    final int id = Integer.parseInt(field.substring(SirixVectorizedExecutor.COMPUTED_LANE_PREFIX.length()));
    if (computed == null || id < 0 || id >= computed.length) {
      return null;
    }
    final SirixVectorizedExecutor.ComputedLane lane = computed[id];
    final int[] code = lane.code();
    if (code.length != 3 || code[2] != ProjectionIndexByteScan.COMPUTED_OP_MUL || code[0] < 0
        || code[0] >= lane.fields().length || code[1] < 0 || code[1] >= lane.fields().length) {
      return null;
    }
    final int left = Arrays.asList(columns).indexOf(lane.fields()[code[0]]);
    final int right = Arrays.asList(columns).indexOf(lane.fields()[code[1]]);
    return left < 0 || right < 0
        ? null
        : new Lane(left, right);
  }

  private static @Nullable Sequence aggregate(final AggregateSummary summary, final String function, final int lane) {
    return switch (function) {
      case "count" -> new Int64(lane < 0
          ? summary.count()
          : summary.present(lane));
      case "sum" -> summary.sumFitsLong(lane)
          ? new Int64(summary.sumLongExact(lane))
          : new Int(new BigDecimal(summary.sum(lane)));
      case "min" -> summary.present(lane) == 0
          ? null
          : new Int64(summary.min(lane));
      case "max" -> summary.present(lane) == 0
          ? null
          : new Int64(summary.max(lane));
      default -> throw new IllegalStateException("Unadmitted aggregate function");
    };
  }

  private static Sequence[] groupValues(final byte[] key, final int columns) {
    final ByteBuffer buffer = ByteBuffer.wrap(key);
    if (buffer.get() != 1) {
      throw new IllegalStateException("Invalid temporal group key");
    }
    final Sequence[] values = new Sequence[columns];
    for (int i = 0; i < columns; i++) {
      values[i] = switch (buffer.get()) {
        case 0 -> null;
        case 1 -> new Int64(buffer.getLong());
        case 2 -> {
          final int length = buffer.getInt();
          if (length < 0 || length > buffer.remaining()) {
            throw new IllegalStateException("Invalid temporal group string");
          }
          final byte[] utf8 = new byte[length];
          buffer.get(utf8);
          yield new Str(new String(utf8, StandardCharsets.UTF_8));
        }
        case 3 -> buffer.get() == 0
            ? Bool.FALSE
            : Bool.TRUE;
        default -> throw new IllegalStateException("Invalid temporal group value kind");
      };
    }
    if (buffer.hasRemaining()) {
      throw new IllegalStateException("Trailing temporal group key bytes");
    }
    return values;
  }
}
