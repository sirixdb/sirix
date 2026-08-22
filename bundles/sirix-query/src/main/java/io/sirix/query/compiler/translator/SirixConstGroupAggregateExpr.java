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
 * Projection-served CONSTANT-key group-by ({@code let $g := 1 ... group by $g}): the grouping
 * partitions nothing, so the executor folds every aggregate in one scalar pass and emits a single
 * record — with shifted operands ({@code $w := $r.f + k}) answered algebraically. Serving is
 * strictly best-effort, exactly like {@link SirixGroupAggregateExpr}: any decline evaluates the
 * generic pipeline compiled alongside.
 */
public final class SirixConstGroupAggregateExpr implements Expr {

  private final SirixExecutorProvider executorProvider;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String[] funcs;
  private final String[] aggFields;
  private final long[] offsets;
  private final String[] outNames;
  /**
   * Carries the admitted source into the revision-stable evaluation lease and the runtime gate: a
   * {@link SourceRef.Kind#VARIABLE} ref cannot be judged at compile time, so this expr re-checks the
   * binding at evaluation time and declines to its generic fallback when it is foreign.
   */
  private final SourceRef sourceRef;
  private final Expr genericFallback;

  public SirixConstGroupAggregateExpr(final SirixExecutorProvider executorProvider, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] funcs, final String[] aggFields, final long[] offsets,
      final String[] outNames, final SourceRef sourceRef, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.funcs = funcs;
    this.aggFields = aggFields;
    this.offsets = offsets;
    this.outNames = outNames;
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
          final Sequence served = executor.executeConstGroupAggregate(ctx, sourcePath, predicateOrNull, funcs,
              aggFields, offsets, outNames);
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
