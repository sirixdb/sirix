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
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.sirix.query.json.JsonDBItem;

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

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) {
    final JsonDBItem source = (JsonDBItem) document.evaluateToItem(ctx, tuple);
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
