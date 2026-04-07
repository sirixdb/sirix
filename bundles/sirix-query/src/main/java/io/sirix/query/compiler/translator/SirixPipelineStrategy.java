package io.sirix.query.compiler.translator;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.translator.Compiler;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Expr;
import io.brackit.query.operator.Operator;
import io.brackit.query.operator.Start;
import io.brackit.query.operator.TableJoin;
import io.brackit.query.util.Cmp;
import io.brackit.query.atomic.QNm;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Sirix-aware pipeline strategy that extends Brackit's sequential strategy
 * with support for optimizer annotations.
 *
 * <p>When the optimizer marks a join as an intersection join
 * ({@code INTERSECTION_JOIN=true}), this strategy forces hash-based
 * execution ({@code skipSort=true}) to avoid unnecessary post-probe
 * sorting. Index intersection results are already unique by nodeKey,
 * so sort+dedup is wasted work.</p>
 */
public final class SirixPipelineStrategy extends SequentialPipelineStrategy {

  @SuppressWarnings("unchecked")
  @Override
  protected Operator join(Operator in, AST node, Compiler compiler) throws QueryException {
    // Check for intersection join annotation from IndexDecompositionStage
    final boolean isIntersectionJoin = Boolean.TRUE.equals(
        node.getProperty(CostProperties.INTERSECTION_JOIN));

    if (isIntersectionJoin) {
      // Force skipSort=true for hash-based intersection
      node.setProperty("skipSort", true);
    }

    return super.join(in, node, compiler);
  }
}
