package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.ExprUtil;
import io.sirix.query.scan.SirixVectorizedExecutor;

/**
 * Projection-served PER-GROUP AGGREGATE expression (P5b stage 7a): attempts
 * {@link SirixVectorizedExecutor#executeGroupByAggregate} and, on any decline
 * ({@code null} — store not covering, gates failed, transient fill trouble), evaluates the
 * GENERIC pipeline expression compiled alongside. Unlike Brackit's vectorized expressions
 * (which throw on unsupported), this wrapper makes serving strictly best-effort: the
 * answer is always produced, the projection only decides how fast.
 */
public final class SirixGroupAggregateExpr implements Expr {

  private final SirixVectorizedExecutor executor;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String[] groupFields;
  private final String[] keyNames;
  private final String[] funcs;
  private final String[] aggFields;
  private final String[] outNames;
  /** Non-null only for a VARIABLE source (external variable): re-verified per evaluation. */
  private final SourceRef runtimeSourceRef;
  private final Expr genericFallback;

  public SirixGroupAggregateExpr(final SirixVectorizedExecutor executor,
      final String[] sourcePath, final PredicateNode predicateOrNull, final String[] groupFields,
      final String[] keyNames, final String[] funcs, final String[] aggFields,
      final String[] outNames, final SourceRef runtimeSourceRef, final Expr genericFallback) {
    this.executor = executor;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.groupFields = groupFields;
    this.keyNames = keyNames;
    this.funcs = funcs;
    this.aggFields = aggFields;
    this.outNames = outNames;
    this.runtimeSourceRef = runtimeSourceRef;
    this.genericFallback = genericFallback;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // Runtime source gate: an external-variable source is verifiable only now, when the
    // context carries the actual binding — a foreign binding falls back to the generic
    // pipeline, which evaluates the same binding and stays correct.
    if (runtimeSourceRef != null && !executor.acceptsSource(runtimeSourceRef, ctx)) {
      return genericFallback.evaluate(ctx, tuple);
    }
    if (executor.canExecute(ctx)) {
      final Sequence served = executor.executeGroupByAggregate(ctx, sourcePath, predicateOrNull,
          groupFields, keyNames, funcs, aggFields, outNames);
      if (served != null) {
        return served;
      }
    }
    return genericFallback.evaluate(ctx, tuple);
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // ExprUtil.asItem unwraps singletons and raises XPTY0004 on >1 — the PipeExpr contract.
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
