package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionIndexScan;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exhaustive unit tests for
 * {@link SirixVectorizedExecutor#rewriteFpCmpForIntegralColumn(int, int, double)}:
 * the rewrite of a double-threshold comparison over a provably-integral
 * NUMERIC_LONG projection column into an exact long-space predicate.
 *
 * <p>The specification is the interpreter's numeric promotion: a long
 * {@code L} matches {@code <op> d} iff
 * {@code Double.compare((double) L, d) <op> 0}. Every rewrite is verified by
 * brute force against that oracle over adversarial candidate longs: values
 * around the threshold, around {@code ±2^53} (where {@code (double) L}
 * starts rounding), and at the {@code Long.MIN_VALUE}/{@code MAX_VALUE}
 * extremes.
 */
public final class FpCmpIntegralRewriteTest {

  // Mirror of the executor's private cmp codes (stable by construction —
  // asserted against the rewrite results below, not blindly trusted).
  private static final int OP_GT = 1;
  private static final int OP_LT = 2;
  private static final int OP_GE = 3;
  private static final int OP_LE = 4;
  private static final int OP_EQ = 5;

  private static final double[] THRESHOLDS = {
      9.99, -9.99, 0.5, -0.5, 0.0, -0.0, 10.0, -10.0, 1.0, -1.0,
      20.5, 20.999, 0.1, -0.1,
      9007199254740991.0,           // 2^53 - 1 (exact)
      9007199254740992.0,           // 2^53 (exact; 2^53 + 1 is NOT a double)
      9007199254740994.0,           // 2^53 + 2
      -9007199254740992.0,
      1.152921504606847E18,         // 2^60 — multiple longs share one double image
      -1.152921504606847E18,
      9.223372036854776E18,         // (double) Long.MAX_VALUE == 2^63
      -9.223372036854776E18,        // (double) Long.MIN_VALUE
      1.0E19, -1.0E19,              // beyond the long range
      4.9E-324, -4.9E-324,          // sub-normal neighbourhood of zero
      123456789.000001, 3.5, 3.7,
  };

  private static final int[] OPS = { OP_GT, OP_LT, OP_GE, OP_LE, OP_EQ };

  /** Oracle: the interpreter's promotion semantics. */
  private static boolean oracle(final long v, final int op, final double d) {
    final int c = Double.compare((double) v, d);
    return switch (op) {
      case OP_GT -> c > 0;
      case OP_LT -> c < 0;
      case OP_GE -> c >= 0;
      case OP_LE -> c <= 0;
      case OP_EQ -> c == 0;
      default -> throw new IllegalStateException();
    };
  }

  /** Semantic evaluation of the rewritten long-space ColumnPredicate. */
  private static boolean evalRewritten(final ProjectionIndexScan.ColumnPredicate p, final long v) {
    return switch (p.op) {
      case GT -> v > p.longLit;
      case LT -> v < p.longLit;
      case GE -> v >= p.longLit;
      case LE -> v <= p.longLit;
      case EQ -> v == p.longLit;
      case BETWEEN_GT_LT -> v > p.longLit && v < p.highLit;
      case BETWEEN_GT_LE -> v > p.longLit && v <= p.highLit;
      case BETWEEN_GE_LT -> v >= p.longLit && v < p.highLit;
      case BETWEEN_GE_LE -> v >= p.longLit && v <= p.highLit;
    };
  }

  /** Candidate longs likely to expose an off-by-one or rounding mistake for {@code d}. */
  private static List<Long> candidatesFor(final double d) {
    final List<Long> out = new ArrayList<>();
    final long[] anchors = {
        0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE,
        (1L << 53) - 2, (1L << 53) - 1, 1L << 53, (1L << 53) + 1, (1L << 53) + 2,
        -(1L << 53) - 2, -(1L << 53) - 1, -(1L << 53), -(1L << 53) + 1,
        1L << 60, (1L << 60) + 1, (1L << 60) - 1,
    };
    for (final long a : anchors) {
      for (long delta = -2; delta <= 2; delta++) {
        final long v = a + delta;  // benign wrap at the extremes still yields valid candidates
        out.add(v);
      }
    }
    if (Double.isFinite(d) && Math.abs(d) < 9.0e18) {
      final long near = (long) d;
      for (long delta = -3; delta <= 3; delta++) {
        out.add(near + delta);
      }
    }
    return out;
  }

  @Test
  void rewriteMatchesOracleForAllOpsAndThresholds() {
    for (final double d : THRESHOLDS) {
      for (final int op : OPS) {
        final ProjectionIndexScan.ColumnPredicate p =
            SirixVectorizedExecutor.rewriteFpCmpForIntegralColumn(0, op, d);
        assertEquals(0, p.column);
        for (final long v : candidatesFor(d)) {
          assertEquals(oracle(v, op, d), evalRewritten(p, v),
              "op=" + op + " d=" + d + " v=" + v + " rewritten=" + p.op + "/" + p.longLit
                  + (p.highLit != 0 ? "/" + p.highLit : ""));
        }
      }
    }
  }

  @Test
  void randomizedSweepAgainstOracle() {
    final java.util.Random rng = new java.util.Random(42);
    for (int t = 0; t < 200; t++) {
      // Mix small fractional, large, and exactly-integral thresholds.
      final double d = switch (t % 4) {
        case 0 -> (rng.nextInt(2001) - 1000) / 10.0;
        case 1 -> rng.nextLong() / 1024.0;
        case 2 -> (double) rng.nextLong();
        default -> rng.nextInt(100) - 50;  // exact small integers
      };
      for (final int op : OPS) {
        final ProjectionIndexScan.ColumnPredicate p =
            SirixVectorizedExecutor.rewriteFpCmpForIntegralColumn(0, op, d);
        for (final long v : candidatesFor(d)) {
          assertEquals(oracle(v, op, d), evalRewritten(p, v),
              "op=" + op + " d=" + d + " v=" + v);
        }
        for (int r = 0; r < 32; r++) {
          final long v = rng.nextLong();
          assertEquals(oracle(v, op, d), evalRewritten(p, v),
              "op=" + op + " d=" + d + " v=" + v);
        }
      }
    }
  }

  @Test
  void fractionalEqualityIsUnsatisfiable() {
    final ProjectionIndexScan.ColumnPredicate p =
        SirixVectorizedExecutor.rewriteFpCmpForIntegralColumn(3, OP_EQ, 9.99);
    for (final long v : new long[] { 9, 10, 0, Long.MIN_VALUE, Long.MAX_VALUE }) {
      assertEquals(false, evalRewritten(p, v));
    }
  }

  @Test
  void hugeThresholdEqualityMatchesEveryLongSharingTheDoubleImage() {
    // 2^60: every long whose double image rounds to 2^60 must match EQ —
    // exactly the interpreter's behavior (it compares (double) L itself).
    final double d = 1.152921504606846976E18;
    final ProjectionIndexScan.ColumnPredicate p =
        SirixVectorizedExecutor.rewriteFpCmpForIntegralColumn(0, OP_EQ, d);
    final long base = 1L << 60;
    for (long delta = -130; delta <= 130; delta += 13) {
      final long v = base + delta;
      assertEquals(oracle(v, OP_EQ, d), evalRewritten(p, v), "v=" + v);
    }
  }

  @Test
  void nanThresholdNeverMatches() {
    for (final int op : OPS) {
      final ProjectionIndexScan.ColumnPredicate p =
          SirixVectorizedExecutor.rewriteFpCmpForIntegralColumn(0, op, Double.NaN);
      for (final long v : new long[] { 0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE }) {
        assertEquals(false, evalRewritten(p, v), "op=" + op + " v=" + v);
      }
    }
  }
}
