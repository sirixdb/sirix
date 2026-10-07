package io.sirix.query.function;

import io.brackit.query.QueryContext;
import io.brackit.query.function.ConstructorFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.StaticContext;
import io.sirix.query.json.AtomicStrJsonDBItem;

/**
 * Uses the original constructor's signature and function conversion, including cardinality checks.
 */
public final class StoredDateTimeConstructor extends ConstructorFunction {
  public StoredDateTimeConstructor(final ConstructorFunction original) {
    super(original.getName(), original.getSignature(), Type.DATI);
  }

  @Override
  public Sequence execute(final StaticContext context, final QueryContext queryContext, final Sequence[] arguments) {
    return arguments[0] instanceof AtomicStrJsonDBItem stored
        ? stored.dateTime()
        : super.execute(context, queryContext, arguments);
  }
}
