package io.sirix.query.compiler.optimizer.stats;

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
}
