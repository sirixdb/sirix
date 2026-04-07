package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Selects the best plan from each Mesh equivalence class and applies
 * its properties back to the original AST node.
 *
 * <p>Runs after {@link MeshPopulationStage}. Walks the AST post-order
 * (bottom-up) so that child decisions are resolved before parents, then
 * for each node tagged with {@code MESH_CLASS_ID}, retrieves the best
 * plan from the shared {@link Mesh} instance, copies decision properties,
 * and restructures joins when the best plan has a different ordering.</p>
 *
 * <p>Based on Graefe/DeWitt Exodus optimizer plan selection phase.</p>
 */
public final class MeshSelectionStage implements Stage {

  private final Mesh mesh;

  public MeshSelectionStage(Mesh mesh) {
    this.mesh = mesh;
  }

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    applyBestPlans(ast);
    return ast;
  }

  /**
   * Post-order walk: process children first so child decisions inform
   * parent cost propagation. For each node with a MESH_CLASS_ID, copy
   * the best plan's decision properties to the original AST node.
   */
  private void applyBestPlans(AST node) {
    if (node == null) {
      return;
    }

    // Recurse into children FIRST (bottom-up)
    for (int i = 0; i < node.getChildCount(); i++) {
      applyBestPlans(node.getChild(i));
    }

    final Object classIdObj = node.getProperty(CostProperties.MESH_CLASS_ID);
    if (classIdObj instanceof Integer classId) {
      final AST bestPlan = mesh.getBestPlan(classId);
      if (bestPlan != null) {
        copyDecisionProperties(bestPlan, node);

        // For joins: swap children if the best plan is an alternative ordering
        if (node.getType() == XQ.Join) {
          applyJoinRestructuring(node, classId);
          propagateChildCosts(node, classId);
        }
      }
    }
  }

  /**
   * Properties that represent plan decisions to copy from best plan to original.
   * Includes cost properties needed for downstream stages and explain output.
   */
  private static final String[] DECISION_PROPERTIES = {
      CostProperties.PREFER_INDEX,
      CostProperties.INDEX_ID,
      CostProperties.INDEX_TYPE,
      CostProperties.JOIN_COST,
      CostProperties.INDEX_SCAN_COST,
      CostProperties.SEQ_SCAN_COST,
  };

  /**
   * Copy decision properties from the best plan to the original AST node.
   * Only copies properties that are non-null on the best plan.
   */
  private static void copyDecisionProperties(AST bestPlan, AST original) {
    for (final String key : DECISION_PROPERTIES) {
      final Object value = bestPlan.getProperty(key);
      if (value != null) {
        original.setProperty(key, value);
      }
    }
  }

  /**
   * If the best plan for a join is not the first alternative (original order),
   * swap the join's children to match the preferred ordering.
   */
  private void applyJoinRestructuring(AST joinNode, int classId) {
    final var eqClass = mesh.getClass(classId);
    if (eqClass == null || eqClass.size() < 2) {
      return;
    }

    // Best plan is the alternative (not the original at index 0) → swap
    final AST bestPlan = eqClass.getBestPlan();
    final AST originalPlan = eqClass.getAlternative(0).plan();
    if (bestPlan != originalPlan && joinNode.getChildCount() >= 2) {
      final AST leftInput = joinNode.getChild(0);
      final AST rightInput = joinNode.getChild(1);
      joinNode.replaceChild(0, rightInput);
      joinNode.replaceChild(1, leftInput);
      joinNode.setProperty(CostProperties.JOIN_SWAPPED, true);
    }
  }

  /**
   * Propagate child class costs to the parent join. When children have
   * been resolved to their best plans, their costs should be reflected
   * in the parent's total cost for accurate downstream decisions.
   */
  private void propagateChildCosts(AST joinNode, int classId) {
    final int[] childClassIds = mesh.getChildClasses(classId);
    if (childClassIds == null) {
      return;
    }

    double totalChildCost = 0.0;
    boolean hasChildCost = false;
    for (final int childClassId : childClassIds) {
      if (childClassId >= 0) {
        final double childCost = mesh.getBestCost(childClassId);
        if (childCost < Double.MAX_VALUE) {
          totalChildCost += childCost;
          hasChildCost = true;
        }
      }
    }

    if (hasChildCost) {
      final Object currentCost = joinNode.getProperty(CostProperties.JOIN_COST);
      if (currentCost instanceof Double joinCost) {
        joinNode.setProperty(CostProperties.JOIN_COST, joinCost + totalChildCost);
      }
    }
  }
}
