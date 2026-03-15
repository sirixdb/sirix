package io.sirix.query.compiler.vectorized;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

/**
 * Comparison operators for vectorized predicate evaluation.
 *
 * <p>These operators are used by {@link ColumnBatchFilter} and {@link SimdSelect}
 * to apply SIMD-accelerated filters on numeric columns within a {@link ColumnBatch}.</p>
 *
 * <p>Each enum constant stores its {@link VectorOperators.Comparison} counterpart,
 * enabling single-instruction SIMD comparisons via {@link #applyLong} and
 * {@link #applyDouble} without per-constant method overrides.</p>
 */
public enum ComparisonOperator {

  /** Equal to. */
  EQ(VectorOperators.EQ),

  /** Not equal to. */
  NE(VectorOperators.NE),

  /** Less than. */
  LT(VectorOperators.LT),

  /** Less than or equal to. */
  LE(VectorOperators.LE),

  /** Greater than. */
  GT(VectorOperators.GT),

  /** Greater than or equal to. */
  GE(VectorOperators.GE);

  private final VectorOperators.Comparison vectorOp;

  ComparisonOperator(VectorOperators.Comparison vectorOp) {
    this.vectorOp = vectorOp;
  }

  /**
   * Apply SIMD comparison on a long vector.
   *
   * @param vec      the data vector
   * @param constant the broadcast constant vector
   * @return mask of lanes satisfying the comparison
   */
  public VectorMask<Long> applyLong(LongVector vec, LongVector constant) {
    return vec.compare(vectorOp, constant);
  }

  /**
   * Apply SIMD comparison on a double vector.
   *
   * @param vec      the data vector
   * @param constant the broadcast constant vector
   * @return mask of lanes satisfying the comparison
   */
  public VectorMask<Double> applyDouble(DoubleVector vec, DoubleVector constant) {
    return vec.compare(vectorOp, constant);
  }

  /**
   * Scalar comparison on long values.
   *
   * <p>Used for the scalar tail when the row count is not a multiple of
   * the SIMD lane width.</p>
   *
   * @param a left operand
   * @param b right operand
   * @return true if the comparison holds
   */
  public boolean testLong(long a, long b) {
    return switch (this) {
      case EQ -> a == b;
      case NE -> a != b;
      case LT -> a < b;
      case LE -> a <= b;
      case GT -> a > b;
      case GE -> a >= b;
    };
  }

  /**
   * Scalar comparison on double values.
   *
   * <p>Uses IEEE 754 exact comparison semantics: {@code NaN != NaN} and
   * {@code -0.0 == +0.0}. This matches the SIMD path behavior.</p>
   *
   * @param a left operand
   * @param b right operand
   * @return true if the comparison holds
   */
  public boolean testDouble(double a, double b) {
    return switch (this) {
      case EQ -> a == b;
      case NE -> a != b;
      case LT -> a < b;
      case LE -> a <= b;
      case GT -> a > b;
      case GE -> a >= b;
    };
  }

  /**
   * Scalar comparison using a pre-computed {@link Comparable#compareTo} result.
   *
   * <p>Useful for string range predicates where the comparison result
   * is already available from {@code String.compareTo()}.</p>
   *
   * @param cmp the compareTo result (negative, zero, or positive)
   * @return true if the comparison holds
   */
  public boolean testCompareTo(int cmp) {
    return switch (this) {
      case EQ -> cmp == 0;
      case NE -> cmp != 0;
      case LT -> cmp < 0;
      case LE -> cmp <= 0;
      case GT -> cmp > 0;
      case GE -> cmp >= 0;
    };
  }
}
