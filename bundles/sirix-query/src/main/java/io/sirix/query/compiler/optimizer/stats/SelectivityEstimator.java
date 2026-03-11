package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;

import java.util.Arrays;

/**
 * Estimates predicate selectivity using default heuristics and
 * SQL Server's exponential backoff formula for compound predicates.
 *
 * <p>Default selectivity estimates follow industry conventions:
 * equality = 1%, range = 33%, LIKE = 25%.
 * For AND predicates, instead of naive multiplication (which
 * systematically underestimates), we use exponential backoff:
 * sort selectivities ascending, then {@code s[0] × s[1]^(1/2) × s[2]^(1/3) × ...}</p>
 *
 * <p>For OR predicates, we use the inclusion-exclusion principle:
 * {@code P(A∪B) = P(A) + P(B) - P(A)·P(B)}</p>
 */
public final class SelectivityEstimator {

  private static final double DEFAULT_EQUALITY_SELECTIVITY = 0.01;
  private static final double DEFAULT_RANGE_SELECTIVITY = 0.33;
  private static final double DEFAULT_LIKE_SELECTIVITY = 0.25;
  private static final double DEFAULT_SELECTIVITY = 0.5;
  private static final double MIN_SELECTIVITY = 0.0001;

  /**
   * Estimate the selectivity of a predicate expression.
   *
   * @param predicateExpr the AST node representing the predicate
   * @return estimated selectivity in [MIN_SELECTIVITY, 1.0]
   */
  public double estimateSelectivity(AST predicateExpr) {
    if (predicateExpr == null) {
      return DEFAULT_SELECTIVITY;
    }

    final int type = predicateExpr.getType();

    if (type == XQ.AndExpr) {
      return estimateAndSelectivity(predicateExpr);
    }
    if (type == XQ.OrExpr) {
      return estimateOrSelectivity(predicateExpr);
    }
    if (type == XQ.ComparisonExpr) {
      return estimateComparisonSelectivity(predicateExpr);
    }
    // Brackit may represent comparisons with the operator type directly
    {
      final double typeSel = selectivityForComparisonType(type);
      if (typeSel != DEFAULT_SELECTIVITY) {
        return typeSel;
      }
    }

    // For Predicate wrapper nodes, recurse into child
    if (type == XQ.Predicate && predicateExpr.getChildCount() > 0) {
      return estimateSelectivity(predicateExpr.getChild(0));
    }

    return DEFAULT_SELECTIVITY;
  }

  /**
   * AND: SQL Server exponential backoff formula.
   * Sort selectivities ascending (most selective first), then:
   * {@code sel[0] × sel[1]^(1/2) × sel[2]^(1/3) × ...}
   */
  private double estimateAndSelectivity(AST andExpr) {
    final int childCount = andExpr.getChildCount();
    if (childCount == 0) {
      return DEFAULT_SELECTIVITY;
    }

    final double[] childSels = new double[childCount];
    for (int i = 0; i < childCount; i++) {
      childSels[i] = estimateSelectivity(andExpr.getChild(i));
    }
    Arrays.sort(childSels); // ascending: most selective first

    double selectivity = 1.0;
    for (int i = 0; i < childCount; i++) {
      selectivity *= Math.pow(childSels[i], 1.0 / (i + 1));
    }
    return Math.max(MIN_SELECTIVITY, selectivity);
  }

  /**
   * OR: Inclusion-exclusion principle.
   * {@code P(A∪B) = P(A) + P(B) - P(A)·P(B)}
   */
  private double estimateOrSelectivity(AST orExpr) {
    double selectivity = 0.0;
    for (int i = 0; i < orExpr.getChildCount(); i++) {
      final double childSel = estimateSelectivity(orExpr.getChild(i));
      selectivity = selectivity + childSel - (selectivity * childSel);
    }
    return Math.min(1.0, selectivity);
  }

  /**
   * Estimate selectivity for a comparison expression.
   * ComparisonExpr has 3 children: [operator, left, right]
   */
  private double estimateComparisonSelectivity(AST compExpr) {
    if (compExpr.getChildCount() < 1) {
      return DEFAULT_SELECTIVITY;
    }
    final AST operatorNode = compExpr.getChild(0);
    final String operator = operatorNode.getStringValue();
    if (operator == null) {
      return DEFAULT_SELECTIVITY;
    }
    return selectivityForOperator(operator);
  }

  private double selectivityForOperator(String operator) {
    return switch (operator) {
      case "ValueCompEQ", "GeneralCompEQ" -> DEFAULT_EQUALITY_SELECTIVITY;
      case "ValueCompNE", "GeneralCompNE" -> 1.0 - DEFAULT_EQUALITY_SELECTIVITY;
      case "ValueCompLT", "ValueCompLE", "ValueCompGT", "ValueCompGE",
           "GeneralCompLT", "GeneralCompLE", "GeneralCompGT", "GeneralCompGE"
          -> DEFAULT_RANGE_SELECTIVITY;
      default -> DEFAULT_SELECTIVITY;
    };
  }

  private double selectivityForComparisonType(int type) {
    return switch (type) {
      case XQ.ValueCompEQ, XQ.GeneralCompEQ -> DEFAULT_EQUALITY_SELECTIVITY;
      case XQ.ValueCompNE, XQ.GeneralCompNE -> 1.0 - DEFAULT_EQUALITY_SELECTIVITY;
      case XQ.ValueCompLT, XQ.ValueCompLE, XQ.ValueCompGT, XQ.ValueCompGE,
           XQ.GeneralCompLT, XQ.GeneralCompLE, XQ.GeneralCompGT, XQ.GeneralCompGE
          -> DEFAULT_RANGE_SELECTIVITY;
      default -> DEFAULT_SELECTIVITY;
    };
  }

}
