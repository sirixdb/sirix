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
 * Projection-served COVERED-ROW expression (P5b stage 7c): attempts
 * {@link SirixVectorizedExecutor#executeRowMaterialize} and, on any decline, evaluates the
 * generic pipeline compiled alongside — serving can change cost, never answers.
 */
public final class SirixRowMaterializeExpr implements Expr {

  private final SirixVectorizedExecutor executor;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String[] fields;
  private final String[] outNames;
  /** Per entry: index into {@code fields}, or {@code -1} = computed program entry. */
  private final int[] direct;
  private final int[][] codes;
  private final long[][] consts;
  /** Non-null only for a VARIABLE source (external variable): re-verified per evaluation. */
  private final SourceRef runtimeSourceRef;
  private final Expr genericFallback;

  public SirixRowMaterializeExpr(final SirixVectorizedExecutor executor,
      final String[] sourcePath, final PredicateNode predicateOrNull, final String[] fields,
      final String[] outNames, final int[] direct, final int[][] codes, final long[][] consts,
      final SourceRef runtimeSourceRef, final Expr genericFallback) {
    this.executor = executor;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.fields = fields;
    this.outNames = outNames;
    this.direct = direct;
    this.codes = codes;
    this.consts = consts;
    this.runtimeSourceRef = runtimeSourceRef;
    this.genericFallback = genericFallback;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // Runtime source gate — see SirixGroupAggregateExpr#evaluate.
    if (runtimeSourceRef != null && !executor.acceptsSource(runtimeSourceRef, ctx)) {
      return genericFallback.evaluate(ctx, tuple);
    }
    if (executor.canExecute(ctx)) {
      final Sequence served =
          executor.executeRowMaterialize(sourcePath, predicateOrNull, fields, outNames, direct,
              codes, consts);
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
