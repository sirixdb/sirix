package io.sirix.query.compiler.optimizer;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.module.StaticContext;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;

/**
 * Populates the {@link Mesh} search space with plan alternatives
 * from cost-based annotations on the AST.
 *
 * <p>This stage runs after CostBasedStage and JoinReorderStage.
 * For each AST node annotated with cost-based alternatives (e.g.,
 * index scan vs sequential scan, different join orders), it creates
 * an equivalence class in the Mesh and adds the alternatives.</p>
 *
 * <p>The Mesh is then available for downstream stages to query
 * the best plan for a given equivalence class.</p>
 *
 * <p>Based on Graefe/DeWitt Exodus optimizer, Weiner et al. Section 4.2.</p>
 */
public final class MeshPopulationStage implements Stage {

  private final Mesh mesh;
  private final JsonCostModel costModel;

  public MeshPopulationStage(Mesh mesh) {
    this.mesh = mesh;
    this.costModel = new JsonCostModel();
  }

  public MeshPopulationStage(Mesh mesh, JsonCostModel costModel) {
    this.mesh = mesh;
    this.costModel = costModel;
  }

  /**
   * Get the populated Mesh for downstream stages.
   */
  public Mesh getMesh() {
    return mesh;
  }

  @Override
  public AST rewrite(StaticContext sctx, AST ast) throws QueryException {
    mesh.clear();
    populateMesh(ast);
    return ast;
  }

  /**
   * Recursively walk the AST and populate the Mesh with plan alternatives.
   */
  private void populateMesh(AST node) {
    if (node == null) {
      return;
    }

    // ForBind/LetBind with PREFER_INDEX annotation → index scan vs seq scan alternatives
    if ((node.getType() == XQ.ForBind || node.getType() == XQ.LetBind)
        && node.getChildCount() >= 2) {
      final AST bindingExpr = node.getChild(1);
      populateAccessAlternatives(bindingExpr);
    }

    // Join nodes with cost annotations → join order alternatives
    if (node.getType() == XQ.Join) {
      populateJoinAlternatives(node);
    }

    // Recurse into children
    for (int i = 0; i < node.getChildCount(); i++) {
      populateMesh(node.getChild(i));
    }
  }

  /**
   * For binding expressions with index preference annotations,
   * create equivalence class with seq scan and index scan alternatives.
   */
  private void populateAccessAlternatives(AST bindingExpr) {
    final Object preferIndex = bindingExpr.getProperty(CostProperties.PREFER_INDEX);
    if (preferIndex == null) {
      return; // No cost annotation — skip
    }

    final Object seqCostObj = bindingExpr.getProperty(CostProperties.SEQ_SCAN_COST);
    final Object idxCostObj = bindingExpr.getProperty(CostProperties.INDEX_SCAN_COST);

    if (!(seqCostObj instanceof Double seqCost) || !(idxCostObj instanceof Double idxCost)) {
      return;
    }

    // Create equivalence class: original plan (seq scan) as first alternative
    final AST seqPlan = bindingExpr.copyTree();
    seqPlan.setProperty(CostProperties.PREFER_INDEX, false);
    final int classId = mesh.createClass(seqPlan, seqCost);

    // Add index scan alternative if available
    final Object indexId = bindingExpr.getProperty(CostProperties.INDEX_ID);
    if (indexId != null) {
      final AST idxPlan = bindingExpr.copyTree();
      idxPlan.setProperty(CostProperties.PREFER_INDEX, true);
      idxPlan.setProperty(CostProperties.INDEX_ID, indexId);
      mesh.addAlternative(classId, idxPlan, idxCost);
    }

    // Tag the original node with the mesh class ID for downstream lookup
    bindingExpr.setProperty(CostProperties.MESH_CLASS_ID, classId);
  }

  /**
   * For join nodes with reorder annotations, create equivalence class
   * with original order and the DPhyp-optimal order.
   */
  private void populateJoinAlternatives(AST joinNode) {
    final Object joinCostObj = joinNode.getProperty(CostProperties.JOIN_COST);
    if (!(joinCostObj instanceof Double joinCost)) {
      return;
    }

    // Create equivalence class with current join plan
    final int classId = mesh.createClass(joinNode, joinCost);

    // If the join was swapped by CostBasedJoinReorder, the original
    // (pre-swap) order is an alternative. We don't reconstruct it here
    // since the swap already happened — the Mesh records the cost.
    final Object leftCardObj = joinNode.getProperty(CostProperties.JOIN_LEFT_CARD);
    final Object rightCardObj = joinNode.getProperty(CostProperties.JOIN_RIGHT_CARD);

    if (leftCardObj instanceof Long leftCard && rightCardObj instanceof Long rightCard) {
      // Compute alternative cost (swapped ordering)
      final double altJoinCost = costModel.estimateHashJoinCost(rightCard, leftCard);
      if (Math.abs(altJoinCost - joinCost) > 0.01) {
        // Hash join is symmetric so costs are same, but NLJ would differ
        final double altNljCost = costModel.estimateNestedLoopJoinCost(rightCard, leftCard);
        final double curNljCost = costModel.estimateNestedLoopJoinCost(leftCard, rightCard);
        if (Math.abs(altNljCost - curNljCost) > 0.01) {
          final AST altPlan = joinNode.copyTree();
          mesh.addAlternative(classId, altPlan, altNljCost);
        }
      }
    }

    // Tag the original node
    joinNode.setProperty(CostProperties.MESH_CLASS_ID, classId);

    // Set up child class references if children have mesh classes
    populateChildClasses(joinNode, classId);
  }

  /**
   * Link parent's mesh class to its children's mesh classes (virtual edges).
   */
  private void populateChildClasses(AST joinNode, int parentClassId) {
    final int childCount = Math.min(joinNode.getChildCount(), 2);
    final int[] childClassIds = new int[childCount];
    boolean hasChildClasses = false;

    for (int i = 0; i < childCount; i++) {
      final AST child = joinNode.getChild(i);
      final Object childClassObj = child.getProperty(CostProperties.MESH_CLASS_ID);
      if (childClassObj instanceof Integer childClassId) {
        childClassIds[i] = childClassId;
        hasChildClasses = true;
      } else {
        childClassIds[i] = -1;
      }
    }

    if (hasChildClasses) {
      mesh.setChildClasses(parentClassId, childClassIds);
    }
  }
}
