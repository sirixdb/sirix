package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.ExprUtil;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.sirix.query.json.JsonDBItem;

import java.util.Objects;

final class SirixValidTimeScanExpr implements Expr {
  private final StaticContext context;
  private final Expr document;
  private final Expr point;
  private final String from;
  private final String to;
  private final int mode;

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
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) {
    final JsonDBItem source = (JsonDBItem) document.evaluateToItem(ctx, tuple);
    return ScanValidTimeIndex.comparisonScan(context, ctx, source, () -> point.evaluate(ctx, tuple), from, to, mode);
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
