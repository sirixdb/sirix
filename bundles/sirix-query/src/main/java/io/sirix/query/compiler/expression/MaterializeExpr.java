package io.sirix.query.compiler.expression;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import java.util.Objects;

/** Fully consumes a proven-pure let initializer once for each evaluation of its binding. */
public final class MaterializeExpr implements Expr {
  private static final QNm[] NO_GLOBALS = new QNm[0];
  private final Expr source;
  private final QNm[] globalDefaults;
  private final ConjunctInputs inputs;

  public MaterializeExpr(final Expr source) {
    this(source, NO_GLOBALS, new ConjunctInputs(NO_GLOBALS, NO_GLOBALS, NO_GLOBALS, null, false));
  }

  public MaterializeExpr(final Expr source, final QNm[] globalDefaults, final ConjunctInputs inputs) {
    this.source = Objects.requireNonNull(source);
    this.inputs = Objects.requireNonNull(inputs);
    Objects.requireNonNull(globalDefaults);
    this.globalDefaults = globalDefaults.length == 0
        ? NO_GLOBALS
        : globalDefaults.clone();
    for (final QNm name : this.globalDefaults)
      Objects.requireNonNull(name);
    if (source.isUpdating()) {
      throw new IllegalArgumentException("An updating let initializer cannot be materialized");
    }
  }

  @Override
  public Sequence evaluate(final QueryContext context, final Tuple tuple) {
    if (!inputs.admit(context, tuple)) {
      return source.evaluate(context, tuple);
    }
    // DeclVariable gives caller bindings precedence even over non-external declarations.
    // Such a value may be a lazy, effectful sequence; its initializer's purity no longer proves it.
    for (final QNm name : globalDefaults) {
      if (context.isBound(name))
        return source.evaluate(context, tuple);
    }
    final Sequence evaluated = source.evaluate(context, tuple);
    if (evaluated != null && evaluated.getClass() == ItemSequence.class)
      return evaluated;
    final Sequence result = ExprUtil.materialize(evaluated);
    return result instanceof Array array && !(evaluated instanceof Item)
        ? new ItemSequence(array)
        : result;
  }

  @Override
  public Item evaluateToItem(final QueryContext context, final Tuple tuple) {
    return ExprUtil.asItem(evaluate(context, tuple));
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return source.isVacuous();
  }
}
