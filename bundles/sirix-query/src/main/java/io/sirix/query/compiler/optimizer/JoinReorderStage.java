package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import io.sirix.query.compiler.optimizer.walker.json.CostBasedJoinReorder;

/**
 * Optimization stage that applies DPhyp-based join reordering to the AST.
 *
 * <p>This stage runs after JqgmRewriteStage (which performs logical rewrites like
 * predicate pushdown and join fusion) and after CostBasedStage (which annotates
 * cardinality estimates). It uses the DPhyp algorithm to find optimal join orders
 * and restructures the AST accordingly.</p>
 *
 * <p>The stage wraps {@link CostBasedJoinReorder}, which:
 * <ul>
 *   <li>Identifies connected join groups in the AST</li>
 *   <li>Builds a {@link io.sirix.query.compiler.optimizer.join.JoinGraph}</li>
 *   <li>Runs {@link io.sirix.query.compiler.optimizer.join.AdaptiveJoinOrderOptimizer}
 *       (3-tier: DPhyp → Linearized DP → GOO-DP)</li>
 *   <li>Restructures join nodes based on the optimal plan</li>
 * </ul></p>
 */
public final class JoinReorderStage implements Stage {

  private final JsonCostModel costModel;

  public JoinReorderStage() {
    this.costModel = new JsonCostModel();
  }

  public JoinReorderStage(JsonCostModel costModel) {
    this.costModel = costModel;
  }

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    final var walker = new CostBasedJoinReorder(costModel);
    ast = walker.walk(ast);
    return ast;
  }
}
