package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.vectorized.VectorizedPipelineDetector;

/**
 * Optimization stage that detects vectorizable query pipelines.
 *
 * <p>Wraps {@link VectorizedPipelineDetector#analyze(AST)}, which
 * recursively scans the AST for scan-filter-project patterns eligible
 * for SIMD batch execution. Eligible ForBind nodes are annotated with:
 * <ul>
 *   <li>{@code VECTORIZABLE=true}</li>
 *   <li>{@code VECTORIZABLE_PREDICATE_COUNT} — number of vectorizable predicates</li>
 *   <li>{@code COLUMNAR_STRING_SCAN_ELIGIBLE=true} — if string columnar scan is possible</li>
 * </ul></p>
 *
 * <p>Runs after CostDrivenRoutingStage and before VectorizedRoutingStage
 * in the optimizer pipeline.</p>
 */
public final class VectorizedDetectionStage implements Stage {

  private final VectorizedPipelineDetector detector = new VectorizedPipelineDetector();

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    detector.analyze(ast);
    return ast;
  }
}
