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
 * Projection-served COVERED-ROW expression (P5b stage 7c): attempts
 * {@link SirixVectorizedExecutor#executeRowMaterialize} and, on any decline, evaluates the generic
 * pipeline compiled alongside — serving can change cost, never answers.
 */
public final class SirixRowMaterializeExpr implements Expr {

  private final SirixExecutorProvider executorProvider;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String[] fields;
  private final String[] outNames;
  /** Per entry: index into {@code fields}, or {@code -1} = computed program entry. */
  private final int[] direct;
  private final int[][] codes;
  private final long[][] consts;
  /**
   * Carries the admitted source into the revision-stable evaluation lease and the runtime gate: a
   * {@link SourceRef.Kind#VARIABLE} ref cannot be judged at compile time, so this expr re-checks the
   * binding at evaluation time and declines to its generic fallback when it is foreign.
   */
  private final SourceRef sourceRef;
  private final Expr genericFallback;

  public SirixRowMaterializeExpr(final SirixExecutorProvider executorProvider, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] fields, final String[] outNames, final int[] direct,
      final int[][] codes, final long[][] consts, final SourceRef sourceRef, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.fields = fields;
    this.outNames = outNames;
    this.direct = direct;
    this.codes = codes;
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
              executor.executeRowMaterialize(sourcePath, predicateOrNull, fields, outNames, direct, codes, consts);
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
