package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.compiler.translator.Reference;
import io.brackit.query.expr.Variable;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.ExprUtil;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.RoutedKeys;
import io.sirix.query.json.JsonDBItem;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

final class SirixValidTimeScanExpr implements Expr, Reference {
  private final StaticContext context;
  private final Expr document;
  private final Expr point;
  private final String from;
  private final String to;
  private final int mode;
  private int pointPosition = -1;

  SirixValidTimeScanExpr(final StaticContext context, final Expr document, final Expr point, final String from,
      final String to, final int mode) {
    this.context = Objects.requireNonNull(context);
    this.document = Objects.requireNonNull(document);
    this.point = Objects.requireNonNull(point);
    this.from = Objects.requireNonNull(from);
    this.to = Objects.requireNonNull(to);
    this.mode = mode;
  }

  @Override
  public void setPos(final int position) {
    pointPosition = position;
  }

  @Nullable Item document(final QueryContext ctx, final Tuple tuple) {
    return document.evaluateToItem(ctx, tuple);
  }

  TemporalAggregateRoute.@Nullable Point aggregatePoint(final QueryContext ctx, final Tuple tuple,
      final @Nullable Item item) {
    if (mode != 2) {
      return null;
    }
    if (!(item instanceof JsonDBArray source) || source.len() == 0) {
      return null;
    }
    final JsonIndexController controller =
        source.getResourceSession().getRtxIndexController(source.getTrx().getRevisionNumber());
    if (controller.getIndexes().getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE) == 0) {
      return null;
    }
    IndexDef interval = null;
    for (final IndexDef definition : controller.getIndexes().getIndexDefs()) {
      if (definition.isValidTimeIndex()) {
        interval = definition;
        break;
      }
    }
    if (interval == null || !controller.isExactValidTimeArray(source.getTrx().getStorageEngineReader(), interval,
        source.getNodeKey(), source.len())) {
      return null;
    }
    final ValidTimeConfig config = source.getResourceSession().getResourceConfig().getValidTimeConfig();
    if (config == null || !from.equals(config.getNormalizedValidFromPath())
        || !to.equals(config.getNormalizedValidToPath())) {
      return null;
    }
    final long saved = source.getTrx().getNodeKey();
    try {
      if (!source.getTrx().moveTo(source.getNodeKey()) || source.getTrx().getParentKey() != 0) {
        return null;
      }
    } finally {
      source.getTrx().moveTo(saved);
    }
    final Item value = point.evaluateToItem(ctx, tuple);
    return value instanceof DateTime dateTime && dateTime.getTimezone() != null
        ? new TemporalAggregateRoute.Point(source, new DateTimeToInstant().convert(dateTime))
        : null;
  }

  @Nullable
  RoutedKeys routedKeys(final QueryContext ctx, final Tuple tuple, final @Nullable Item item) {
    final JsonDBItem source = (JsonDBItem) item;
    if (!(source instanceof Array array)) {
      return null;
    }
    if (array.len() == 0) {
      return new RoutedKeys(new long[0], source.getTrx().getRevisionNumber());
    }
    final ValidTimeConfig config = source.getResourceSession().getResourceConfig().getValidTimeConfig();
    if (config == null || !from.equals(config.getNormalizedValidFromPath())
        || !to.equals(config.getNormalizedValidToPath())) {
      return null;
    }
    return ValidTimeIntervalIndex.routedKeys(ValidTimeIntervalIndex.comparisonSequence(source,
        () -> point.evaluate(ctx, tuple), config, (mode & 1) != 0, (mode & 2) != 0));
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) {
    final JsonDBItem source = (JsonDBItem) document(ctx, tuple);
    final Sequence captured = pointPosition >= 0
        ? tuple.get(pointPosition)
        : point instanceof DateTime dateTime
            ? dateTime
            : null;
    final DateTime value =
        captured instanceof DateTime dateTime && (!(point instanceof Variable variable) || variable.getType() == null
            || (variable.getType().getCardinality() != Cardinality.Zero
                && variable.getType().getItemType().matches(dateTime)))
                    ? dateTime
                    : null;
    return ScanValidTimeIndex.comparisonScan(context, ctx, source, () -> point.evaluate(ctx, tuple), value, from, to,
        mode);
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) {
    return ExprUtil.asItem(evaluate(ctx, tuple));
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }
}
