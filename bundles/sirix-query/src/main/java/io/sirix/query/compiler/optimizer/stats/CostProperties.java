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

  /**
   * Check if an AST node represents a left outer join.
   */
  public static boolean isLeftJoin(AST node) {
    return Boolean.TRUE.equals(node.getProperty(LEFT_JOIN));
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
