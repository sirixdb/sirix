package io.sirix.query.compiler.expression;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.expr.Cast;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.StaticContext;
import io.sirix.query.json.AtomicStrJsonDBItem;

import static java.util.Objects.requireNonNull;

/** Keeps normal cast semantics, while reusing a stored string field's parsed value. */
public final class StoredDateTimeCast implements Expr {
  private final StaticContext context;
  private final Expr operand;
  private final boolean allowEmpty;

  public StoredDateTimeCast(final StaticContext context, final Expr operand, final boolean allowEmpty) {
    this.context = context;
    this.operand = requireNonNull(operand);
    this.allowEmpty = allowEmpty;
  }

  @Override
  public Item evaluateToItem(final QueryContext queryContext, final Tuple tuple) {
    final Item item = operand.evaluateToItem(queryContext, tuple);
    return item instanceof AtomicStrJsonDBItem stored
        ? stored.dateTime()
        : Cast.cast(context, item, Type.DATI, allowEmpty);
  }

  @Override
  public Sequence evaluate(final QueryContext queryContext, final Tuple tuple) {
    return evaluateToItem(queryContext, tuple);
  }

  @Override
  public boolean isUpdating() {
    return operand.isUpdating();
  }

  @Override
  public boolean isVacuous() {
    return false;
  }
}
