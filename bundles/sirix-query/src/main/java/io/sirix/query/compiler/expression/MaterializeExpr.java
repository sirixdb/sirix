package io.sirix.query.compiler.expression;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.ExprUtil;
import java.util.Objects;
import io.sirix.query.json.BasicJsonDBStore;

/** Fully consumes a proven-pure let initializer once for each evaluation of its binding. */
public final class MaterializeExpr implements Expr {
  private static final QNm[] NO_GLOBALS = new QNm[0];
  private final Expr source;
  private final QNm[] globalDefaults;
  private final boolean nativeStore;

  public MaterializeExpr(final Expr source) {
    this(source, NO_GLOBALS, false);
  }

  public MaterializeExpr(final Expr source, final QNm[] globalDefaults, final boolean nativeStore) {
    this.source = Objects.requireNonNull(source);
    this.nativeStore = nativeStore;
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
    // Only the stock store guarantees the read-only document/field behavior used by the proof.
    if (nativeStore && !(context.getJsonItemStore() instanceof BasicJsonDBStore)) {
      return source.evaluate(context, tuple);
    }
    // DeclVariable gives caller bindings precedence even over non-external declarations.
    // Such a value may be a lazy, effectful sequence; its initializer's purity no longer proves it.
    for (final QNm name : globalDefaults) {
      if (context.isBound(name))
        return source.evaluate(context, tuple);
    }
    return ExprUtil.materialize(source.evaluate(context, tuple));
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
