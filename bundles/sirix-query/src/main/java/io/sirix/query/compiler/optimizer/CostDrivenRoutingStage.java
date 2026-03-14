package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Cost-driven execution routing stage.
 *
 * <p>Runs after CostBasedStage and before IndexMatching. Translates
 * high-level cost annotations ({@code PREFER_INDEX=false}) into
 * gate signals ({@code INDEX_GATE_CLOSED=true}) on descendant nodes
 * that the index matching walkers check before applying index rewrites.</p>
 *
 * <p>When a ForBind's binding expression has been annotated with
 * {@code PREFER_INDEX=false} by CostBasedStage (meaning sequential scan
 * is cheaper), this stage propagates {@code INDEX_GATE_CLOSED=true} to
 * all descendant nodes within that binding scope. Index matching walkers
 * then skip those nodes, preventing index rewrites where the cost model
 * determined they are suboptimal.</p>
 *
 * <p>Conversely, when {@code PREFER_INDEX=true}, the gate stays open (default)
 * and index matching proceeds normally.</p>
 */
public final class CostDrivenRoutingStage implements Stage {

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    propagateGateSignals(ast);
    return ast;
  }

  /**
   * Walk the AST and propagate INDEX_GATE_CLOSED signals from
   * ForBind/LetBind binding expressions to their descendant nodes.
   */
  private static void propagateGateSignals(AST node) {
    if (node == null) {
      return;
    }

    if ((node.getType() == XQ.ForBind || node.getType() == XQ.LetBind)
        && node.getChildCount() >= 2) {
      final AST bindingExpr = node.getChild(1);
      final Object preferIndex = bindingExpr.getProperty(CostProperties.PREFER_INDEX);

      if (Boolean.FALSE.equals(preferIndex)) {
        // Cost model says sequential scan is cheaper — close the index gate
        // for all descendants of this binding
        closeGateForDescendants(bindingExpr);
      }
    }

    // Recurse into children
    for (int i = 0; i < node.getChildCount(); i++) {
      propagateGateSignals(node.getChild(i));
    }
  }

  /**
   * Mark all descendants of the given node with INDEX_GATE_CLOSED=true.
   */
  private static void closeGateForDescendants(AST node) {
    node.setProperty(CostProperties.INDEX_GATE_CLOSED, true);
    for (int i = 0; i < node.getChildCount(); i++) {
      closeGateForDescendants(node.getChild(i));
    }
  }
}
