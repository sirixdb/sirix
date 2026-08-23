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
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;

/**
 * Projection-served COMPUTED-EXPRESSION AGGREGATE (gap item 2):
 * {@code sum|avg|min|max|count(for $r in P [where p] return <+/-/* tree>)} attempts
 * {@link SirixVectorizedExecutor#executeComputedAggregate} and, on any decline (gates,
 * exact-arithmetic overflow, transient trouble), evaluates the GENERIC function call compiled
 * alongside — serving can change cost, never answers.
 */
public final class SirixComputedAggregateExpr implements Expr {

  private final SirixExecutorProvider executorProvider;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String func;
  private final String[] fields;
  private final int[] code;
  private final long[] consts;
  /**
   * Carries the admitted source into the revision-stable evaluation lease and the runtime gate: a
   * {@link SourceRef.Kind#VARIABLE} ref cannot be judged at compile time, so this expr re-checks the
   * binding at evaluation time and declines to its generic fallback when it is foreign.
   */
  private final SourceRef sourceRef;
  private final Expr genericFallback;

  public SirixComputedAggregateExpr(final SirixExecutorProvider executorProvider, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String func, final String[] fields, final int[] code,
      final long[] consts, final SourceRef sourceRef, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.func = func;
    this.fields = fields;
    this.code = code;
    this.consts = consts;
    this.sourceRef = sourceRef;
    this.genericFallback = genericFallback;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    final SirixExecutorProvider.Lease lease = executorProvider.acquire(ctx, sourceRef);
    if (lease != null) {
      try (lease) {
        final SirixVectorizedExecutor executor = lease.executor();
        if ((sourceRef == null || executor.acceptsSource(sourceRef, ctx)) && executor.canExecute(ctx)) {
          final Sequence served =
              executor.executeComputedAggregate(sourcePath, predicateOrNull, func, fields, code, consts);
          if (served != null) {
            return served;
          }
        }
      }
    }
    return genericFallback.evaluate(ctx, tuple);
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // ExprUtil.asItem unwraps singletons and raises XPTY0004 on >1 — the function-call
    // contract (and maps the served empty sequence for avg/min/max over zero rows to null).
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
