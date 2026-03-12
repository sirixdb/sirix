package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.walker.json.JoinDecompositionWalker;

/**
 * Index-aware join decomposition stage (Rules 5 and 6).
 *
 * <p>This stage runs after CostBasedStage (which annotates AST nodes with
 * index preference hints) and JqgmRewriteStage (which fuses adjacent joins
 * via Rule 1). It applies:</p>
 * <ul>
 *   <li><b>Rule 5</b>: Splits n-way joins when an index covers a subtree</li>
 *   <li><b>Rule 6</b>: Creates intersection plans when multiple indexes
 *       overlap at the JPP root</li>
 * </ul>
 *
 * <p>Based on Weiner et al. Section 4.3, adapted for JSON/JSONiq.</p>
 */
public final class IndexDecompositionStage implements Stage {

  // Reuse walker instance across calls (prepare() resets state)
  private final JoinDecompositionWalker walker = new JoinDecompositionWalker();

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    ast = walker.walk(ast);
    return ast;
  }
}
