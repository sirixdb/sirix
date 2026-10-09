package io.sirix.query;

import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.sirix.query.compiler.optimizer.PlanCache;

/**
 * A caller may retain a Query as well as ask the chain to compile its text again. Check the schema
 * at execution entry too, so that a retained query never opens an index removed since compilation.
 * The expression keeps no execution state: both the fast arm and recompilation return immutable
 * code, and lazy iteration remains owned by the caller's QueryExecution.
 */
final class SchemaFencedExpr implements Expr {
  private final SirixCompileChain chain;
  private final String text;
  private final long schemaVersion;
  private final Expr delegate;

  SchemaFencedExpr(final SirixCompileChain chain, final String text, final long schemaVersion, final Expr delegate) {
    this.chain = chain;
    this.text = text;
    this.schemaVersion = schemaVersion;
    this.delegate = delegate;
  }

  private Expr resolve() {
    if (schemaVersion == PlanCache.indexSchemaVersion()) {
      return delegate;
    }
    final Expr current = chain.compile(text).getBody();
    return current instanceof SchemaFencedExpr fenced ? fenced.delegate : current;
  }

  @Override
  public Sequence evaluate(final QueryContext context, final Tuple tuple) {
    return resolve().evaluate(context, tuple);
  }

  @Override
  public Item evaluateToItem(final QueryContext context, final Tuple tuple) {
    return resolve().evaluateToItem(context, tuple);
  }

  @Override
  public boolean isUpdating() {
    return delegate.isUpdating();
  }

  @Override
  public boolean isVacuous() {
    return delegate.isVacuous();
  }
}
