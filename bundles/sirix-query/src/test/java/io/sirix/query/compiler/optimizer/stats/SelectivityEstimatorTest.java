package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SelectivityEstimator} — verifies default heuristics,
 * SQL Server exponential backoff for AND, and inclusion-exclusion for OR.
 */
final class SelectivityEstimatorTest {

  private final SelectivityEstimator estimator = new SelectivityEstimator();

  @Test
  void equalityComparisonReturnsOnePercent() {
    // ComparisonExpr with child(0) = "ValueCompEQ"
    final var comp = makeComparison("ValueCompEQ");
    final double sel = estimator.estimateSelectivity(comp);
    assertEquals(0.01, sel, 1e-9, "Equality should have 1% selectivity");
  }

  @Test
  void rangeComparisonReturnsThirtyThreePercent() {
    final var comp = makeComparison("ValueCompGT");
    final double sel = estimator.estimateSelectivity(comp);
    assertEquals(0.33, sel, 1e-9, "Range comparison should have 33% selectivity");
  }

  @Test
  void andExprUsesExponentialBackoff() {
    // AND(EQ, GT) → sorted: [0.01, 0.33]
    // Formula: 0.01^(1/1) * 0.33^(1/2) = 0.01 * 0.5745 ≈ 0.005745
    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(makeComparison("ValueCompEQ"));  // 0.01
    andExpr.addChild(makeComparison("ValueCompGT"));  // 0.33

    final double sel = estimator.estimateSelectivity(andExpr);

    // Expected: 0.01 * sqrt(0.33) ≈ 0.00574
    final double expected = 0.01 * Math.sqrt(0.33);
    assertEquals(expected, sel, 1e-4,
        "AND with exponential backoff: 0.01 * 0.33^(1/2) ≈ " + expected);
  }

  @Test
  void andExprWithThreePredicates() {
    // AND(EQ, GT, LE) → sorted: [0.01, 0.33, 0.33]
    // Formula: 0.01^1 * 0.33^(1/2) * 0.33^(1/3) = 0.01 * 0.5745 * 0.6910 ≈ 0.00397
    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(makeComparison("ValueCompEQ"));  // 0.01
    andExpr.addChild(makeComparison("ValueCompGT"));  // 0.33
    andExpr.addChild(makeComparison("ValueCompLE"));  // 0.33

    final double sel = estimator.estimateSelectivity(andExpr);

    final double expected = 0.01 * Math.pow(0.33, 0.5) * Math.pow(0.33, 1.0 / 3.0);
    assertEquals(expected, sel, 1e-4,
        "AND with 3 predicates using exponential backoff");
  }

  @Test
  void andExprHigherThanNaiveMultiplication() {
    // Exponential backoff should produce HIGHER selectivity than naive AND
    // (which assumes full independence and systematically underestimates)
    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(makeComparison("ValueCompEQ"));  // 0.01
    andExpr.addChild(makeComparison("ValueCompGT"));  // 0.33

    final double sel = estimator.estimateSelectivity(andExpr);
    final double naive = 0.01 * 0.33; // = 0.0033

    assertTrue(sel > naive,
        "Exponential backoff (" + sel + ") should be higher than naive (" + naive + ")");
  }

  @Test
  void orExprUsesInclusionExclusion() {
    // OR(EQ, GT) → P(A∪B) = P(A) + P(B) - P(A)*P(B) = 0.01 + 0.33 - 0.0033 = 0.3367
    final var orExpr = new AST(XQ.OrExpr, null);
    orExpr.addChild(makeComparison("ValueCompEQ"));  // 0.01
    orExpr.addChild(makeComparison("ValueCompGT"));  // 0.33

    final double sel = estimator.estimateSelectivity(orExpr);

    final double expected = 0.01 + 0.33 - (0.01 * 0.33);
    assertEquals(expected, sel, 1e-9, "OR with inclusion-exclusion");
  }

  @Test
  void orExprNeverExceedsOne() {
    // OR of three high-selectivity predicates
    final var orExpr = new AST(XQ.OrExpr, null);
    orExpr.addChild(makeComparison("ValueCompNE"));  // 0.99
    orExpr.addChild(makeComparison("ValueCompNE"));  // 0.99
    orExpr.addChild(makeComparison("ValueCompNE"));  // 0.99

    final double sel = estimator.estimateSelectivity(orExpr);
    assertTrue(sel <= 1.0, "OR selectivity should not exceed 1.0, got " + sel);
    assertTrue(sel > 0.99, "OR of three 99% should be very high, got " + sel);
  }

  @Test
  void predicateWrapperDelegatesToChild() {
    final var predicate = new AST(XQ.Predicate, null);
    predicate.addChild(makeComparison("ValueCompEQ"));

    final double sel = estimator.estimateSelectivity(predicate);
    assertEquals(0.01, sel, 1e-9, "Predicate wrapper should delegate to child");
  }

  @Test
  void nullReturnsDefaultSelectivity() {
    assertEquals(0.5, estimator.estimateSelectivity(null), 1e-9);
  }

  /**
   * Create a mock ComparisonExpr AST: [operator, leftPlaceholder, rightPlaceholder]
   */
  private static AST makeComparison(String operator) {
    final var comp = new AST(XQ.ComparisonExpr, null);
    comp.addChild(new AST(XQ.Str, operator));  // operator node
    comp.addChild(new AST(XQ.DerefExpr, null)); // left operand placeholder
    comp.addChild(new AST(XQ.Str, "value"));    // right operand placeholder
    return comp;
  }
}
