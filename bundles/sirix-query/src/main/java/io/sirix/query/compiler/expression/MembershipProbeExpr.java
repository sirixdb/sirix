package io.sirix.query.compiler.expression;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Bool;
import io.brackit.query.jdm.Expr;
import io.sirix.query.compiler.expression.MembershipIndexExpr.Lookup;

import java.util.Objects;

/** Boolean semi/anti-join probe; matching duplicates do not multiply the outer row. */
public final class MembershipProbeExpr implements Expr {
  private final Expr index;
  private final Expr key;
  private final boolean anti;
  private final Expr fallback;

  public MembershipProbeExpr(final Expr index, final Expr key, final boolean anti, final Expr fallback) {
    this.index = Objects.requireNonNull(index);
    this.key = Objects.requireNonNull(key);
    this.anti = anti;
    this.fallback = Objects.requireNonNull(fallback);
  }

  @Override
  public Bool evaluate(final QueryContext ctx, final Tuple tuple) {
    final Lookup lookup = (Lookup) index.evaluateToItem(ctx, tuple);
    final int result = lookup.probe(ctx, tuple, key);
    if (result < 0) {
      return (Bool) fallback.evaluateToItem(ctx, tuple);
    }
    return (result != 0) != anti
        ? Bool.TRUE
        : Bool.FALSE;
  }

  @Override
  public Bool evaluateToItem(final QueryContext ctx, final Tuple tuple) {
    return evaluate(ctx, tuple);
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
