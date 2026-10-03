package io.sirix.query.compiler.expression;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import java.util.Objects;

public final class GuardedConjunctExpr implements Expr {
  private final Expr ordered;
  private final Expr original;
  private final ConjunctInputs inputs;

  public GuardedConjunctExpr(final Expr ordered, final Expr original, final ConjunctInputs inputs) {
    this.ordered = Objects.requireNonNull(ordered);
    this.original = Objects.requireNonNull(original);
    this.inputs = Objects.requireNonNull(inputs);
  }

  @Override
  public Sequence evaluate(final QueryContext context, final Tuple tuple) {
    return (inputs.admit(context, tuple)
        ? ordered
        : original).evaluate(context, tuple);
  }

  @Override
  public Item evaluateToItem(final QueryContext context, final Tuple tuple) {
    return (inputs.admit(context, tuple)
        ? ordered
        : original).evaluateToItem(context, tuple);
  }

  @Override
  public boolean isUpdating() {
    return ordered.isUpdating() || original.isUpdating();
  }

  @Override
  public boolean isVacuous() {
    return ordered.isVacuous() && original.isVacuous();
  }
}
