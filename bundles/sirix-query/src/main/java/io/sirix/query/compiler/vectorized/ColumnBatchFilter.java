package io.sirix.query.compiler.vectorized;

import java.util.Objects;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated filter for {@link ColumnBatch} selection vectors.
 *
 * <p>Applies a scalar predicate ({@code column op constant}) to a ColumnBatch
 * using the Java Vector API. For numeric columns (INT64, FLOAT64), the
 * comparison is executed across SIMD lanes — processing 4-8 values per
 * instruction on AVX2/AVX-512 hardware.</p>
 *
 * <p>The filter updates the batch's selection vector in-place. Multiple
 * filters can be chained (conjunctive predicates) by applying successive
 * filters to the same batch.</p>
 *
 * <p>This utility operates on {@link ColumnBatch} directly, independent
 * of Brackit's block pipeline. For Brackit-integrated SIMD filtering,
 * use {@link SimdSelect} which implements {@link io.brackit.query.block.Block}.</p>
 */
public final class ColumnBatchFilter {

  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

  private ColumnBatchFilter() {}

  /**
   * Apply a SIMD filter on an INT64 column.
   *
   * @param batch    the batch to filter (selection vector modified in place)
   * @param col      the column index to filter
   * @param op       the comparison operator
   * @param constant the constant to compare against
   */
  public static void filterLong(ColumnBatch batch, int col, ComparisonOperator op, long constant) {
    Objects.requireNonNull(batch, "batch");
    Objects.requireNonNull(op, "op");
    final long[] column = batch.longColumn(col);
    final boolean[] nulls = batch.nullFlags(col);
    final int[] sel = batch.selectionVector();
    final int inputCount = batch.selectionCount();
    final int lanes = LONG_SPECIES.length();
    final int simdBound = inputCount - (inputCount % lanes);

    final long[] gathered = new long[lanes];
    final LongVector cst = LongVector.broadcast(LONG_SPECIES, constant);
    int outPos = 0;

    for (int i = 0; i < simdBound; i += lanes) {
      boolean anyNull = false;
      for (int j = 0; j < lanes; j++) {
        final int row = sel[i + j];
        gathered[j] = column[row];
        if (nulls[row]) {
          anyNull = true;
        }
      }

      final LongVector vec = LongVector.fromArray(LONG_SPECIES, gathered, 0);
      final VectorMask<Long> mask = op.applyLong(vec, cst);

      for (int j = 0; j < lanes; j++) {
        if (mask.laneIsSet(j) && !(anyNull && nulls[sel[i + j]])) {
          sel[outPos++] = sel[i + j];
        }
      }
    }

    // Scalar tail
    for (int i = simdBound; i < inputCount; i++) {
      final int row = sel[i];
      if (!nulls[row] && op.testLong(column[row], constant)) {
        sel[outPos++] = row;
      }
    }

    batch.setSelectionCount(outPos);
  }

  /**
   * Apply a SIMD filter on a FLOAT64 column.
   *
   * @param batch    the batch to filter (selection vector modified in place)
   * @param col      the column index to filter
   * @param op       the comparison operator
   * @param constant the constant to compare against
   */
  public static void filterDouble(ColumnBatch batch, int col, ComparisonOperator op, double constant) {
    Objects.requireNonNull(batch, "batch");
    Objects.requireNonNull(op, "op");
    final double[] column = batch.doubleColumn(col);
    final boolean[] nulls = batch.nullFlags(col);
    final int[] sel = batch.selectionVector();
    final int inputCount = batch.selectionCount();
    final int lanes = DOUBLE_SPECIES.length();
    final int simdBound = inputCount - (inputCount % lanes);

    final double[] gathered = new double[lanes];
    final DoubleVector cst = DoubleVector.broadcast(DOUBLE_SPECIES, constant);
    int outPos = 0;

    for (int i = 0; i < simdBound; i += lanes) {
      boolean anyNull = false;
      for (int j = 0; j < lanes; j++) {
        final int row = sel[i + j];
        gathered[j] = column[row];
        if (nulls[row]) {
          anyNull = true;
        }
      }

      final DoubleVector vec = DoubleVector.fromArray(DOUBLE_SPECIES, gathered, 0);
      final VectorMask<Double> mask = op.applyDouble(vec, cst);

      for (int j = 0; j < lanes; j++) {
        if (mask.laneIsSet(j) && !(anyNull && nulls[sel[i + j]])) {
          sel[outPos++] = sel[i + j];
        }
      }
    }

    // Scalar tail
    for (int i = simdBound; i < inputCount; i++) {
      final int row = sel[i];
      if (!nulls[row] && op.testDouble(column[row], constant)) {
        sel[outPos++] = row;
      }
    }

    batch.setSelectionCount(outPos);
  }
}
