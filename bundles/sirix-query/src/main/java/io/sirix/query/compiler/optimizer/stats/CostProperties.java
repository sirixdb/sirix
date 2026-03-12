package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.compiler.AST;

/**
 * Constants for AST property keys used by cost-based optimization stages.
 * Centralizes all stringly-typed property names to prevent typos and
 * enable refactoring.
 */
public final class CostProperties {

  private CostProperties() {}

  // --- CostBasedStage annotations (Milestone 1) ---
  public static final String PREFER_INDEX = "costBased.preferIndex";
  public static final String INDEX_ID = "costBased.indexId";
  public static final String INDEX_TYPE = "costBased.indexType";
  public static final String INDEX_SCAN_COST = "costBased.indexScanCost";
  public static final String SEQ_SCAN_COST = "costBased.seqScanCost";
  public static final String PATH_CARDINALITY = "costBased.pathCardinality";
  public static final String TOTAL_NODE_COUNT = "costBased.totalNodeCount";

  // --- CardinalityEstimator annotations (Milestone 2) ---
  public static final String ESTIMATED_CARDINALITY = "costBased.estimatedCardinality";

  // --- SelectAccessFusionWalker annotations (Milestone 2, Rule 3) ---
  public static final String FUSED_COUNT = "fusedPredicate.count";
  public static final String FUSED_OPERATOR = "fusedPredicate.operator";
  public static final String FUSED_FIELD_NAME = "fusedPredicate.fieldName";
  public static final String FUSED_OPERATOR2 = "fusedPredicate.operator2";
  public static final String FUSED_FIELD_NAME2 = "fusedPredicate.fieldName2";
  public static final String FUSED_HAS_PUSHDOWN = "fusedPredicate.hasPredicatePushdown";
  public static final String FUSED_PREDICATE_COUNT = "fusedPredicate.predicateCount";

  // --- Join ordering annotations (Milestone 3) ---
  /** Brackit's property key for left outer joins (set by JoinRewriter). */
  public static final String LEFT_JOIN = "leftJoin";
  /** Brackit's property key for comparison operator on join nodes. */
  public static final String CMP = "cmp";
  public static final String JOIN_REORDERED = "joinOrder.reordered";
  public static final String JOIN_LEFT_CARD = "joinOrder.leftCardinality";
  public static final String JOIN_RIGHT_CARD = "joinOrder.rightCardinality";
  public static final String JOIN_COST = "joinOrder.cost";
  public static final String JOIN_SWAPPED = "joinOrder.swapped";
  public static final String JOIN_PREDICATE_PUSHED = "joinOrder.predicatePushed";

  // --- Join fusion annotations (Milestone 4, Rule 1) ---
  public static final String JOIN_FUSED = "joinFusion.fused";
  public static final String JOIN_FUSION_GROUP_ID = "joinFusion.groupId";
  public static final String JOIN_FUSION_STEP_COUNT = "joinFusion.stepCount";

  // --- Mesh / search space annotations (Milestone 5) ---
  public static final String MESH_CLASS_ID = "mesh.classId";

  // --- Cost-driven routing annotations (Milestone 5) ---
  /** Propagated from PREFER_INDEX to descendant nodes for index matching gate. */
  public static final String INDEX_GATE_CLOSED = "costBased.indexGateClosed";

  // --- Join decomposition annotations (Milestone 4, Rules 5-6) ---
  public static final String DECOMPOSITION_APPLICABLE = "joinDecomposition.applicable";
  public static final String DECOMPOSITION_TYPE = "joinDecomposition.type";
  public static final String DECOMPOSITION_RULE_5 = "joinDecomposition.rule5";
  public static final String DECOMPOSITION_RULE_6 = "joinDecomposition.rule6";
  public static final String DECOMPOSITION_INDEX_ID = "joinDecomposition.indexId";
  public static final String DECOMPOSITION_INDEX_TYPE = "joinDecomposition.indexType";
  public static final String DECOMPOSITION_INDEX_ID_RIGHT = "joinDecomposition.indexId.right";
  public static final String DECOMPOSITION_INDEX_TYPE_RIGHT = "joinDecomposition.indexType.right";
  public static final String DECOMPOSITION_INTERSECT = "joinDecomposition.intersect";

  /**
   * Check if an AST node represents a left outer join.
   */
  public static boolean isLeftJoin(AST node) {
    return Boolean.TRUE.equals(node.getProperty(LEFT_JOIN));
  }

  /**
   * Check if the index gate is closed for an AST node, meaning cost-based
   * analysis determined that sequential scan is cheaper than index scan.
   *
   * <p>This is checked by index matching walkers before applying index
   * rewrites. When the gate is closed, the walker should skip index
   * replacement for this subtree.</p>
   *
   * @param node the AST node to check
   * @return true if index usage should be suppressed
   */
  public static boolean isIndexGateClosed(AST node) {
    return Boolean.TRUE.equals(node.getProperty(INDEX_GATE_CLOSED));
  }

  /**
   * Extract cardinality from an AST node or its shallow descendants.
   * Checks ESTIMATED_CARDINALITY first, then PATH_CARDINALITY as fallback.
   *
   * @param node     the AST node to inspect
   * @param maxDepth maximum depth to search children (0 = node only)
   * @return positive cardinality, or -1 if unknown
   */
  public static long extractCardinality(AST node, int maxDepth) {
    Object prop = node.getProperty(ESTIMATED_CARDINALITY);
    if (prop instanceof Long l && l > 0) {
      return l;
    }
    prop = node.getProperty(PATH_CARDINALITY);
    if (prop instanceof Long l && l > 0) {
      return l;
    }
    if (maxDepth <= 0) {
      return -1L;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final long childCard = extractCardinality(node.getChild(i), maxDepth - 1);
      if (childCard > 0) {
        return childCard;
      }
    }
    return -1L;
  }
}
