package io.sirix.query.compiler.vectorized;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int64;
import io.brackit.query.block.Block;
import io.brackit.query.block.Sink;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Sequence;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated Select block for Brackit's block execution pipeline.
 *
 * <p>Drop-in replacement for {@link io.brackit.query.block.Select} when the
 * predicate is a simple numeric comparison ({@code column op constant}).
 * Instead of evaluating the Brackit expression per-tuple, this block:
 * <ol>
 *   <li>Extracts the numeric values from the tuple buffer into a primitive array</li>
 *   <li>Runs a SIMD comparison using the Java Vector API</li>
 *   <li>Compacts the Tuple[] buffer in-place with only matching tuples</li>
 * </ol></p>
 *
 * <p>When SIMD is not applicable (null values, non-numeric types), falls
 * back to the standard Brackit expression evaluator per-tuple.</p>
 *
 * <p>Implements {@link Block}, plugging directly into Brackit's
 * {@link io.brackit.query.block.BlockChain} infrastructure.</p>
 *
 * <p>Based on MonetDB/X100 vectorized selection primitives adapted
 * for Brackit's Tuple-based block model.</p>
 */
public final class SimdSelect implements Block {

  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

  private final int tuplePosition;
  private final ComparisonOperator op;
  private final long longConstant;
  private final double doubleConstant;
  private final boolean isLongMode;
  private final Expr fallbackPred;

  /**
   * Create a SIMD Select block for INT64 comparisons.
   *
   * @param tuplePosition the position in the tuple to extract the value from
   * @param op            the comparison operator
   * @param constant      the constant to compare against
   * @param fallbackPred  fallback Brackit expression for non-SIMD cases (may be null)
   * @return the SimdSelect block
   */
  public static SimdSelect forLong(int tuplePosition, ComparisonOperator op,
                                    long constant, Expr fallbackPred) {
    return new SimdSelect(tuplePosition, op, constant, 0.0, true, fallbackPred);
  }

  /**
   * Create a SIMD Select block for FLOAT64 comparisons.
   *
   * @param tuplePosition the position in the tuple to extract the value from
   * @param op            the comparison operator
   * @param constant      the constant to compare against
   * @param fallbackPred  fallback Brackit expression for non-SIMD cases (may be null)
   * @return the SimdSelect block
   */
  public static SimdSelect forDouble(int tuplePosition, ComparisonOperator op,
                                      double constant, Expr fallbackPred) {
    return new SimdSelect(tuplePosition, op, 0L, constant, false, fallbackPred);
  }

  private SimdSelect(int tuplePosition, ComparisonOperator op,
                     long longConstant, double doubleConstant,
                     boolean isLongMode, Expr fallbackPred) {
    if (tuplePosition < 0) {
      throw new IllegalArgumentException("Tuple position must be non-negative: " + tuplePosition);
    }
    if (op == null) {
      throw new IllegalArgumentException("Comparison operator must not be null");
    }
    this.tuplePosition = tuplePosition;
    this.op = op;
    this.longConstant = longConstant;
    this.doubleConstant = doubleConstant;
    this.isLongMode = isLongMode;
    this.fallbackPred = fallbackPred;
  }

  @Override
  public Sink create(QueryContext ctx, Sink sink) throws QueryException {
    return new SimdSelectSink(ctx, sink);
  }

  @Override
  public int outputWidth(int inputWidth) {
    return inputWidth; // Select doesn't change tuple width
  }

  /**
   * @return the tuple position this block filters on
   */
  public int tuplePosition() {
    return tuplePosition;
  }

  /**
   * @return the comparison operator
   */
  public ComparisonOperator comparisonOperator() {
    return op;
  }

  private final class SimdSelectSink implements Sink {
    private final QueryContext ctx;
    private final Sink downstream;

    SimdSelectSink(QueryContext ctx, Sink downstream) {
      this.ctx = ctx;
      this.downstream = downstream;
    }

    @Override
    public void output(Tuple[] buf, int len) throws QueryException {
      final int nlen;
      if (isLongMode) {
        nlen = filterLong(buf, len);
      } else {
        nlen = filterDouble(buf, len);
      }
      if (nlen > 0) {
        downstream.output(buf, nlen);
      }
    }

    /**
     * SIMD-accelerated filter on INT64 tuple values.
     * Extracts values from tuples, runs SIMD comparison, compacts buffer.
     */
    private int filterLong(Tuple[] buf, int len) throws QueryException {
      final int lanes = LONG_SPECIES.length();
      final int simdBound = len - (len % lanes);
      final long[] values = new long[lanes];
      final LongVector cst = LongVector.broadcast(LONG_SPECIES, longConstant);
      int outPos = 0;

      // SIMD main loop
      for (int i = 0; i < simdBound; i += lanes) {
        boolean anyFallback = false;

        for (int j = 0; j < lanes; j++) {
          final Sequence s = buf[i + j].get(tuplePosition);
          if (s instanceof Int64 v) {
            values[j] = v.longValue();
          } else {
            anyFallback = true;
            break;
          }
        }

        if (anyFallback) {
          // Fall back to per-tuple evaluation for this chunk
          for (int j = 0; j < lanes; j++) {
            if (evaluateFallback(ctx, buf[i + j])) {
              buf[outPos++] = buf[i + j];
            }
          }
        } else {
          final LongVector vec = LongVector.fromArray(LONG_SPECIES, values, 0);
          final VectorMask<Long> mask = op.applyLong(vec, cst);

          for (int j = 0; j < lanes; j++) {
            if (mask.laneIsSet(j)) {
              buf[outPos++] = buf[i + j];
            }
          }
        }
      }

      // Scalar tail
      for (int i = simdBound; i < len; i++) {
        final Sequence s = buf[i].get(tuplePosition);
        if (s instanceof Int64 v) {
          if (op.testLong(v.longValue(), longConstant)) {
            buf[outPos++] = buf[i];
          }
        } else if (evaluateFallback(ctx, buf[i])) {
          buf[outPos++] = buf[i];
        }
      }

      return outPos;
    }

    /**
     * SIMD-accelerated filter on FLOAT64 tuple values.
     */
    private int filterDouble(Tuple[] buf, int len) throws QueryException {
      final int lanes = DOUBLE_SPECIES.length();
      final int simdBound = len - (len % lanes);
      final double[] values = new double[lanes];
      final DoubleVector cst = DoubleVector.broadcast(DOUBLE_SPECIES, doubleConstant);
      int outPos = 0;

      for (int i = 0; i < simdBound; i += lanes) {
        boolean anyFallback = false;

        for (int j = 0; j < lanes; j++) {
          final Sequence s = buf[i + j].get(tuplePosition);
          if (s instanceof Dbl v) {
            values[j] = v.doubleValue();
          } else if (s instanceof Int64 v) {
            values[j] = (double) v.longValue();
          } else {
            anyFallback = true;
            break;
          }
        }

        if (anyFallback) {
          for (int j = 0; j < lanes; j++) {
            if (evaluateFallback(ctx, buf[i + j])) {
              buf[outPos++] = buf[i + j];
            }
          }
        } else {
          final DoubleVector vec = DoubleVector.fromArray(DOUBLE_SPECIES, values, 0);
          final VectorMask<Double> mask = op.applyDouble(vec, cst);

          for (int j = 0; j < lanes; j++) {
            if (mask.laneIsSet(j)) {
              buf[outPos++] = buf[i + j];
            }
          }
        }
      }

      // Scalar tail
      for (int i = simdBound; i < len; i++) {
        final Sequence s = buf[i].get(tuplePosition);
        if (s instanceof Dbl v) {
          if (op.testDouble(v.doubleValue(), doubleConstant)) {
            buf[outPos++] = buf[i];
          }
        } else if (s instanceof Int64 v) {
          if (op.testDouble((double) v.longValue(), doubleConstant)) {
            buf[outPos++] = buf[i];
          }
        } else if (evaluateFallback(ctx, buf[i])) {
          buf[outPos++] = buf[i];
        }
      }

      return outPos;
    }

    /**
     * Fallback evaluation using the Brackit expression evaluator.
     */
    private boolean evaluateFallback(QueryContext ctx, Tuple t) throws QueryException {
      if (fallbackPred == null) {
        return false;
      }
      final Sequence p = fallbackPred.evaluate(ctx, t);
      return p != null && p.booleanValue();
    }

    @Override
    public Sink fork() {
      return new SimdSelectSink(ctx, downstream.fork());
    }

    @Override
    public Sink partition(Sink stopAt) {
      return new SimdSelectSink(ctx, downstream.partition(stopAt));
    }

    @Override
    public void begin() throws QueryException {
      downstream.begin();
    }

    @Override
    public void end() throws QueryException {
      downstream.end();
    }

    @Override
    public void fail() throws QueryException {
      downstream.fail();
    }
  }
}
