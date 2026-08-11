package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexScan.Op;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The run-aware scan kernel, held against the positional one it replaces.
 *
 * <h2>What has to be true</h2>
 *
 * <p>Agreement, on every operator, for every row. A compressor picks the scheme per block, so the
 * same column can be run-encoded in one block and bit-packed in the next; if the two kernels
 * disagreed anywhere — a boundary, an empty run, a mask word edge — a query's answer would depend
 * on a compression decision, which is the worst kind of wrong answer because it is not reproducible
 * from the query alone.
 *
 * <p>The row counts are chosen to straddle word boundaries deliberately: runs that end mid-word,
 * runs that span several words, and a total that is not a multiple of 64. Those are where a
 * word-at-a-time range clear goes wrong, and where a per-row loop never would.
 */
final class RleScanTest {

  /** Runs built so boundaries fall inside words, across words, and on a word edge. */
  private static final long[] RUN_VALUES = {5, 9, 5, 100, 7, 9};
  private static final int[] RUN_LENGTHS = {1, 63, 64, 65, 200, 7};   // sums to 400

  private static final int ROWS = 400;

  private static long[] expand() {
    final long[] out = new long[ROWS];
    int at = 0;
    for (int r = 0; r < RUN_VALUES.length; r++) {
      Arrays.fill(out, at, at + RUN_LENGTHS[r], RUN_VALUES[r]);
      at += RUN_LENGTHS[r];
    }
    assertEquals(ROWS, at, "test fixture does not sum to ROWS");
    return out;
  }

  /** The positional reference: evaluate per row, exactly as a bit-packed column would. */
  private static long[] positionalMask(final long[] values, final Op op, final long lit,
      final long high) {
    final long[] mask = new long[(ROWS + 63) >>> 6];
    for (int i = 0; i < ROWS; i++) {
      final long v = values[i];
      final boolean hit = switch (op) {
        case GT -> v > lit;
        case LT -> v < lit;
        case GE -> v >= lit;
        case LE -> v <= lit;
        case EQ -> v == lit;
        case BETWEEN_GT_LT -> v > lit && v < high;
        case BETWEEN_GT_LE -> v > lit && v <= high;
        case BETWEEN_GE_LT -> v >= lit && v < high;
        case BETWEEN_GE_LE -> v >= lit && v <= high;
      };
      if (hit) {
        mask[i >>> 6] |= 1L << (i & 63);
      }
    }
    return mask;
  }

  private static long[] allTrue() {
    final long[] mask = new long[(ROWS + 63) >>> 6];
    Arrays.fill(mask, -1L);
    final int tail = ROWS & 63;
    if (tail != 0) {
      mask[mask.length - 1] = (1L << tail) - 1L;
    }
    return mask;
  }

  @Test
  @DisplayName("every operator agrees with the positional kernel, row for row")
  void agreesWithPositionalOnEveryOp() {
    final long[] values = expand();
    for (final Op op : Op.values()) {
      for (final long lit : new long[] {4, 5, 7, 9, 100, 101}) {
        final long high = lit + 90;
        final long[] mine = allTrue();
        assertTrue(RleScan.evalInto(RUN_VALUES, RUN_LENGTHS, RUN_VALUES.length, ROWS, op, lit, high,
                                    mine),
                   "kernel refused a well-formed run set");
        assertArrayEquals(positionalMask(values, op, lit, high), mine,
                          "run-aware and positional disagree for " + op + " lit=" + lit);
      }
    }
  }

  @Test
  @DisplayName("counting needs no mask and no row visit")
  void countMatchesTheMask() {
    final long[] values = expand();
    for (final Op op : Op.values()) {
      for (final long lit : new long[] {5, 9, 100}) {
        final long high = lit + 90;
        long expected = 0;
        for (final long word : positionalMask(values, op, lit, high)) {
          expected += Long.bitCount(word);
        }
        assertEquals(expected,
                     RleScan.countMatching(RUN_VALUES, RUN_LENGTHS, RUN_VALUES.length, op, lit,
                                           high),
                     "count disagrees with the mask for " + op + " lit=" + lit);
      }
    }
  }

  @Test
  @DisplayName("the mask is narrowed, never widened — earlier predicates survive")
  void narrowsTheRunningConjunction() {
    // Every other row already excluded by a prior predicate. A kernel that SET bits for matching
    // runs instead of leaving them alone would resurrect rows the conjunction had ruled out.
    final long[] mask = new long[(ROWS + 63) >>> 6];
    Arrays.fill(mask, 0x5555555555555555L);
    // Bits past the last row must be zero — the convention fillAllTrue establishes and every
    // kernel relies on. Seeding them would leave residue no kernel is supposed to touch, and the
    // popcount of the result would count rows that do not exist.
    final int tailBits = ROWS & 63;
    if (tailBits != 0) {
      mask[mask.length - 1] &= (1L << tailBits) - 1L;
    }
    final long[] before = mask.clone();

    assertTrue(RleScan.evalInto(RUN_VALUES, RUN_LENGTHS, RUN_VALUES.length, ROWS, Op.GE, 0,
                                Long.MAX_VALUE, mask),
               "kernel refused a well-formed run set");
    // GE 0 matches every run here, so a correct AND leaves the mask exactly as it was.
    assertArrayEquals(before, mask, "a predicate matching every run changed the mask");

    assertTrue(RleScan.evalInto(RUN_VALUES, RUN_LENGTHS, RUN_VALUES.length, ROWS, Op.EQ,
                                -12345, 0, mask),
               "kernel refused a well-formed run set");
    for (final long word : mask) {
      assertEquals(0L, word, "a predicate matching no run left bits set");
    }
  }

  @Test
  @DisplayName("a run set that does not cover the rows is refused, not half-applied")
  void refusesInconsistentRuns() {
    final long[] mask = allTrue();
    final long[] before = mask.clone();
    assertFalse(RleScan.evalInto(RUN_VALUES, RUN_LENGTHS, RUN_VALUES.length, ROWS + 1, Op.EQ, 5, 0,
                                 mask),
                "kernel accepted runs that do not sum to the row count");
    assertArrayEquals(before, mask,
                      "the mask was modified before the run set was found inconsistent — a "
                          + "half-narrowed conjunction is a wrong answer, not a slow one");
  }

  @Test
  @DisplayName("decodeRle expands the fixture back to the values the kernels scanned")
  void decodeRleExpandsTheFixture() {
    final long[] out = new long[ROWS];
    assertEquals(ROWS,
                 LightweightSchemes.decodeRle(RUN_VALUES, RUN_LENGTHS, RUN_VALUES.length, out),
                 "decodeRle wrote a different row count than the runs sum to");
    assertArrayEquals(expand(), out, "decodeRle disagrees with the positional expansion");
  }

  @Test
  @DisplayName("an adversarial run length that would wrap the cursor is refused, not thrown at")
  void decodeRleRefusesWrappingRunLength() {
    // With the cursor already advanced, a run length near Integer.MAX_VALUE makes the naive
    // cursor-plus-length sum wrap negative — the documented answer is -1, never an exception.
    final long[] out = new long[8];
    assertEquals(-1,
                 LightweightSchemes.decodeRle(new long[] {1L, 2L},
                                              new int[] {4, Integer.MAX_VALUE - 1}, 2, out),
                 "a run length overflowing the output must report -1 like any other too-small out");
  }

  @Test
  @DisplayName("range clear and set handle word edges")
  void rangeHelpersHandleWordEdges() {
    for (final int[] range : new int[][] {{0, 1}, {0, 64}, {63, 65}, {64, 128}, {1, 63}, {5, 200}}) {
      final long[] mask = new long[4];
      RleScan.setRange(mask, range[0], range[1]);
      int set = 0;
      for (final long word : mask) {
        set += Long.bitCount(word);
      }
      assertEquals(range[1] - range[0], set,
                   "setRange(" + range[0] + "," + range[1] + ") set the wrong number of bits");
      RleScan.clearRange(mask, range[0], range[1]);
      for (final long word : mask) {
        assertEquals(0L, word, "clearRange did not undo setRange for " + Arrays.toString(range));
      }
    }
  }
}
