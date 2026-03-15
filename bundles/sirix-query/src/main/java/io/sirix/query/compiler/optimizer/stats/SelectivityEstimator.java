package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.module.Namespaces;

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

  static final double DEFAULT_EQUALITY_SELECTIVITY = 0.01;
  static final double DEFAULT_RANGE_SELECTIVITY = 0.33;
  static final double DEFAULT_LIKE_SELECTIVITY = 0.25;
  static final double DEFAULT_SELECTIVITY = 0.5;
  static final double MIN_SELECTIVITY = 0.0001;

  /**
   * Optional histogram for data-driven selectivity estimates.
   * When set, equality and range predicates use histogram data
   * instead of hardcoded defaults.
   * Volatile for safe publication across threads.
   */
  private volatile Histogram histogram;

  /**
   * Set a histogram for data-driven selectivity estimation.
   * When set, equality and range predicates use histogram data.
   *
   * @param histogram the histogram to use, or null to revert to defaults
   */
  public void setHistogram(Histogram histogram) {
    this.histogram = histogram;
  }

  /**
   * Get the current histogram, if set.
   *
   * @return the histogram, or null if using defaults
   */
  public Histogram getHistogram() {
    return histogram;
  }

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
    // fn:not() is represented as a FunctionCall with QNm "not" in the FN namespace
    if (type == XQ.FunctionCall && isFnNot(predicateExpr)) {
      return estimateNotSelectivity(predicateExpr);
    }
    // Brackit may represent comparisons with the operator type directly
    {
      final double typeSel = selectivityForComparisonType(type, predicateExpr);
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
   * fn:not(p): selectivity = 1 - selectivity(p).
   * Child(0) of the FunctionCall is the argument expression.
   */
  private double estimateNotSelectivity(AST fnNotExpr) {
    if (fnNotExpr.getChildCount() < 1) {
      return DEFAULT_SELECTIVITY;
    }
    final double childSel = estimateSelectivity(fnNotExpr.getChild(0));
    return Math.max(MIN_SELECTIVITY, 1.0 - childSel);
  }

  /**
   * Check if a FunctionCall AST node represents fn:not().
   */
  private static boolean isFnNot(AST node) {
    final Object value = node.getValue();
    if (value instanceof QNm qnm) {
      return "not".equals(qnm.getLocalName())
          && (Namespaces.FN_NSURI.equals(qnm.getNamespaceURI())
              || Namespaces.DEFAULT_FN_NSURI.equals(qnm.getNamespaceURI()));
    }
    return false;
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
    return selectivityForOperator(operator, compExpr);
  }

  private double selectivityForOperator(String operator, AST compExpr) {
    final int opType = operatorStringToType(operator);
    if (opType < 0) {
      return DEFAULT_SELECTIVITY;
    }
    return selectivityForComparisonType(opType, compExpr);
  }

  /**
   * Unified selectivity dispatch for comparison operators.
   * Uses histogram data when available, otherwise falls back to defaults.
   */
  private double selectivityForComparisonType(int type, AST compExpr) {
    // Capture volatile once to prevent TOCTOU race (another thread could
    // set histogram=null between the null check and the use)
    final Histogram hist = this.histogram;
    if (hist != null) {
      return histogramSelectivity(hist, type, compExpr);
    }
    return switch (type) {
      case XQ.ValueCompEQ, XQ.GeneralCompEQ -> DEFAULT_EQUALITY_SELECTIVITY;
      case XQ.ValueCompNE, XQ.GeneralCompNE -> 1.0 - DEFAULT_EQUALITY_SELECTIVITY;
      case XQ.ValueCompLT, XQ.ValueCompLE, XQ.ValueCompGT, XQ.ValueCompGE,
           XQ.GeneralCompLT, XQ.GeneralCompLE, XQ.GeneralCompGT, XQ.GeneralCompGE
          -> DEFAULT_RANGE_SELECTIVITY;
      default -> DEFAULT_SELECTIVITY;
    };
  }

  /** Map operator string names to XQ type constants. Returns -1 if unknown. */
  private static int operatorStringToType(String operator) {
    return switch (operator) {
      case "ValueCompEQ" -> XQ.ValueCompEQ;
      case "ValueCompNE" -> XQ.ValueCompNE;
      case "ValueCompLT" -> XQ.ValueCompLT;
      case "ValueCompLE" -> XQ.ValueCompLE;
      case "ValueCompGT" -> XQ.ValueCompGT;
      case "ValueCompGE" -> XQ.ValueCompGE;
      case "GeneralCompEQ" -> XQ.GeneralCompEQ;
      case "GeneralCompNE" -> XQ.GeneralCompNE;
      case "GeneralCompLT" -> XQ.GeneralCompLT;
      case "GeneralCompLE" -> XQ.GeneralCompLE;
      case "GeneralCompGT" -> XQ.GeneralCompGT;
      case "GeneralCompGE" -> XQ.GeneralCompGE;
      default -> -1;
    };
  }

  /**
   * Extract a numeric constant value from a comparison expression's operands.
   *
   * <p>ComparisonExpr has children: [operator, left, right].
   * Tries child(2) (right operand) first, then child(1) (left operand),
   * looking for a {@link Numeric} value or a parseable numeric string.</p>
   *
   * @param compExpr the ComparisonExpr AST node, or null
   * @return the numeric constant, or {@link Double#NaN} if not found
   */
  static double extractConstantValue(AST compExpr) {
    if (compExpr == null || compExpr.getChildCount() < 3) {
      return Double.NaN;
    }
    // Try right operand first (child 2), then left (child 1)
    double value = tryExtractNumeric(compExpr.getChild(2));
    if (!Double.isNaN(value)) {
      return value;
    }
    return tryExtractNumeric(compExpr.getChild(1));
  }

  /**
   * Attempt to extract a numeric value from an AST node.
   * Checks whether the node's value is a {@link Numeric} (Int32, Int64, Dbl, Dec)
   * or a string that can be parsed as a double.
   */
  private static double tryExtractNumeric(AST node) {
    if (node == null) {
      return Double.NaN;
    }
    final Object val = node.getValue();
    if (val instanceof Numeric numeric) {
      return numeric.doubleValue();
    }
    // Try parsing the string value as a number (handles literal nodes
    // whose value is stored as a String rather than an Atomic)
    if (val instanceof String str) {
      try {
        return Double.parseDouble(str);
      } catch (NumberFormatException ignored) {
        // Not a numeric string
      }
    }
    return Double.NaN;
  }

  /**
   * Unified histogram-based selectivity dispatch.
   * Extracts the actual constant from the AST operands when available,
   * falling back to the histogram midpoint otherwise.
   */
  private static double histogramSelectivity(Histogram hist, int type, AST compExpr) {
    final double value = extractConstantValue(compExpr);
    final double v = Double.isNaN(value)
        ? (hist.minValue() + hist.maxValue()) / 2.0
        : value;

    return switch (type) {
      case XQ.ValueCompEQ, XQ.GeneralCompEQ ->
          hist.estimateEqualitySelectivity(v);
      case XQ.ValueCompNE, XQ.GeneralCompNE ->
          1.0 - hist.estimateEqualitySelectivity(v);
      case XQ.ValueCompLT, XQ.GeneralCompLT ->
          hist.estimateLessThanSelectivity(v);
      case XQ.ValueCompLE, XQ.GeneralCompLE ->
          hist.estimateLessThanOrEqualSelectivity(v);
      case XQ.ValueCompGT, XQ.GeneralCompGT ->
          hist.estimateGreaterThanSelectivity(v);
      case XQ.ValueCompGE, XQ.GeneralCompGE ->
          hist.estimateGreaterThanOrEqualSelectivity(v);
      default -> DEFAULT_SELECTIVITY;
    };
  }

}
