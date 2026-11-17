package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.ExprUtil;
import io.sirix.query.scan.VectorizedRouteRefusedException;

import java.util.Objects;

/** Retains the generic evaluator behind legacy SPI expressions that cannot decline via null. */
final class VectorizedFallbackExpr implements Expr {
  private final Expr vectorized;
  private final Expr generic;

  VectorizedFallbackExpr(final Expr vectorized, final Expr generic) {
    this.vectorized = Objects.requireNonNull(vectorized);
    this.generic = Objects.requireNonNull(generic);
  }

  @Override
  public Sequence evaluate(final QueryContext context, final Tuple tuple) throws QueryException {
    try {
      return vectorized.evaluate(context, tuple);
    } catch (final VectorizedRouteRefusedException _) {
      return generic.evaluate(context, tuple);
    }
  }

  @Override
  public Item evaluateToItem(final QueryContext context, final Tuple tuple) throws QueryException {
    return ExprUtil.asItem(evaluate(context, tuple));
  }

  @Override
  public boolean isUpdating() {
    return generic.isUpdating();
  }

  @Override
  public boolean isVacuous() {
    return generic.isVacuous();
  }
}
