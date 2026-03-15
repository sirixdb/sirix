package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.stats.CostProperties;

/**
 * Selects the best plan from each Mesh equivalence class and applies
 * its properties back to the original AST node.
 *
 * <p>Runs after {@link MeshPopulationStage}. Walks the AST pre-order
 * (top-down) and for each node tagged with {@code MESH_CLASS_ID},
 * retrieves the best plan from the shared {@link Mesh} instance and
 * copies decision properties ({@code PREFER_INDEX}, {@code INDEX_ID},
 * {@code INDEX_TYPE}, {@code JOIN_COST}) to the original node.</p>
 *
 * <p>Properties NOT copied (already on original from CostBasedStage):
 * {@code INDEX_SCAN_COST}, {@code SEQ_SCAN_COST}, {@code ESTIMATED_CARDINALITY}.</p>
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
   * Pre-order walk: for each node with a MESH_CLASS_ID, copy the best
   * plan's decision properties to the original AST node.
   */
  private void applyBestPlans(AST node) {
    if (node == null) {
      return;
    }

    final Object classIdObj = node.getProperty(CostProperties.MESH_CLASS_ID);
    if (classIdObj instanceof Integer classId) {
      final AST bestPlan = mesh.getBestPlan(classId);
      if (bestPlan != null) {
        copyDecisionProperties(bestPlan, node);
      }
    }

    // Recurse into children
    for (int i = 0; i < node.getChildCount(); i++) {
      applyBestPlans(node.getChild(i));
    }
  }

  /** Properties that represent plan decisions to copy from best plan to original. */
  private static final String[] DECISION_PROPERTIES = {
      CostProperties.PREFER_INDEX,
      CostProperties.INDEX_ID,
      CostProperties.INDEX_TYPE,
      CostProperties.JOIN_COST,
  };

  /**
   * Copy decision properties from the best plan to the original AST node.
   * Only copies properties that represent plan decisions, not properties
   * that already exist on the original from prior stages.
   */
  private static void copyDecisionProperties(AST bestPlan, AST original) {
    for (final String key : DECISION_PROPERTIES) {
      final Object value = bestPlan.getProperty(key);
      if (value != null) {
        original.setProperty(key, value);
      }
    }
  }
}
