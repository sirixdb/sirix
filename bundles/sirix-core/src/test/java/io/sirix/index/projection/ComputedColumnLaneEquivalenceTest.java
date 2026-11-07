/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ProjectionComputedColumn} answers identically whichever way it reads its operands. Like
 * the group folds, it decides per selection word: above
 * {@link ProjectionVectorKernels#FOLD_WALK_MAX_BITS} surviving rows it gathers the word's 64 values
 * of every operand into a reused window and evaluates from there, below it it reads the lanes in
 * place. That dispatch is legitimate only while the two forms agree row for row, on native slab
 * lanes and on the heap-array lanes an unconverted producer still hands out — which is what this
 * test pins.
 *
 * <p>
 * The derived slice is always checked against an INDEPENDENT oracle computed here, not only against
 * the other form, so a defect shared by both still fails. Density is steered by the operand
 * presence words, which is the only thing the production dispatch looks at: an all-present fixture
 * gathers every word, a fixture with at most {@code FOLD_WALK_MAX_BITS} rows per word gathers none,
 * and the mixed fixture takes both sides.
 */
final class ComputedColumnLaneEquivalenceTest {

  /**
   * Not a multiple of 64, so the last word is partly invalid and the gather must clamp — and its 45
   * valid rows are above the crossover, so the clamped word is one the dispatch really gathers.
   */
  private static final int ROWS = 557;

  private static final int WORDS = (ROWS + 63) >>> 6;

  private static final int OPERANDS = 2;

  /** {@code cost * qty}: the program Q7, Q9, Q11 and Q12 of the bitemporal suite compile to. */
  private static final int[] MUL = {0, 1, ProjectionIndexByteScan.COMPUTED_OP_MUL};

  /** {@code (cost * qty) + 7}: three pushes and two operators, so the stack depth is exercised. */
  private static final int[] MUL_PLUS_CONST = {0, 1, ProjectionIndexByteScan.COMPUTED_OP_MUL,
      ProjectionIndexByteScan.COMPUTED_CONST_BASE, ProjectionIndexByteScan.COMPUTED_OP_ADD};

  private static final long[] CONSTS = {7L};

  private static final long[] NO_CONSTS = new long[0];

  private final long[][] values = new long[OPERANDS][ROWS];

  ComputedColumnLaneEquivalenceTest() {
    final Random rnd = new Random(0x5E1E7L);
    for (int c = 0; c < OPERANDS; c++) {
      for (int r = 0; r < ROWS; r++) {
        // Signed and wide enough that min/max are exercised on both sides of zero, while a product
        // of two of them stays far inside a long.
        values[c][r] = rnd.nextLong(-1L << 20, 1L << 20);
      }
    }
  }

  @Test
  @DisplayName("the fixtures cover both sides of the gather crossover and neither alone")
  void theFixturesStraddleTheCrossover() {
    final long[][] mixed = mixedPresence();
    boolean dense = false;
    boolean sparse = false;
    for (int w = 0; w < WORDS; w++) {
      final long word = derivedWord(mixed, w);
      dense |= ProjectionColumnGroupScan.gatherPays(word);
      sparse |= word != 0L && !ProjectionColumnGroupScan.gatherPays(word);
    }
    assertTrue(dense, "no word of the mixed fixture gathers: the window form would go untested");
    assertTrue(sparse, "every word of the mixed fixture gathers: the in-place form would go untested");
    for (int w = 0; w < WORDS; w++) {
      assertTrue(ProjectionColumnGroupScan.gatherPays(derivedWord(allPresent(), w)),
          "the dense fixture must gather word " + w);
      assertFalse(ProjectionColumnGroupScan.gatherPays(derivedWord(sparsePresence(), w)),
          "the sparse fixture must read word " + w + " in place");
    }
  }

  @Test
  @DisplayName("every density answers the oracle over slab lanes and over heap-array lanes")
  void everyDensityAnswersTheOracle() {
    try (Arena arena = Arena.ofConfined()) {
      for (final long[][] presence : new long[][][] {allPresent(), sparsePresence(), mixedPresence(), nonePresent()}) {
        final ColumnSlice fromSlab = evaluateOne(slabOperands(arena, presence), MUL, NO_CONSTS);
        final ColumnSlice fromHeap = evaluateOne(heapOperands(presence), MUL, NO_CONSTS);
        assertDerived(oracle(presence, MUL, NO_CONSTS), fromSlab, "slab lanes");
        assertDerived(oracle(presence, MUL, NO_CONSTS), fromHeap, "heap-array lanes");
      }
    }
  }

  @Test
  @DisplayName("a multi-operator program answers the oracle on both sides of the crossover")
  void aDeeperProgramAnswersTheOracle() {
    try (Arena arena = Arena.ofConfined()) {
      for (final long[][] presence : new long[][][] {allPresent(), sparsePresence(), mixedPresence()}) {
        assertDerived(oracle(presence, MUL_PLUS_CONST, CONSTS),
            evaluateOne(slabOperands(arena, presence), MUL_PLUS_CONST, CONSTS), "slab lanes");
        assertDerived(oracle(presence, MUL_PLUS_CONST, CONSTS),
            evaluateOne(heapOperands(presence), MUL_PLUS_CONST, CONSTS), "heap-array lanes");
      }
    }
  }

  @Test
  @DisplayName("the tail word is clamped: no read past the lane, no derived value above the row count")
  void theTailWordIsClamped() {
    // Presence words carry set bits ABOVE the row count — what a leaf's last word looks like — and
    // the slabs are sized to exactly ROWS rows, so a gather of the full 64 would run off the lane.
    final long[][] presence = allPresent();
    presence[0][WORDS - 1] = -1L;
    presence[1][WORDS - 1] = -1L;
    try (Arena arena = Arena.ofConfined()) {
      final ColumnSlice slice = evaluateOne(slabOperands(arena, presence), MUL, NO_CONSTS);
      assertDerived(oracle(presence, MUL, NO_CONSTS), slice, "slab lanes with a full tail word");
      final int tailBits = ROWS - ((WORDS - 1) << 6);
      assertEquals((1L << tailBits) - 1L, slice.presenceWords()[WORDS - 1],
          "the derived presence must clear every row above the leaf's row count");
    }
  }

  @Test
  @DisplayName("two lanes at distinct 64-byte-aligned bases of one slab are read at their own base")
  void lanesAtDistinctBasesInOneSlabAreReadAtTheirOwnBase() {
    final long[][] presence = allPresent();
    try (Arena arena = Arena.ofConfined()) {
      final long stride = (ROWS * (long) Long.BYTES + 63L) & ~63L;
      final MemorySegment slab = arena.allocate(stride * OPERANDS, 64);
      final ColumnSlice[][] operands = new ColumnSlice[OPERANDS][1];
      for (int c = 0; c < OPERANDS; c++) {
        final long base = c * stride;
        for (int r = 0; r < ROWS; r++) {
          SegmentAccess.putLong(slab, base + ((long) r << 3), values[c][r]);
        }
        assertEquals(0L, base & 63L, "lane bases stay 64-byte aligned");
        operands[c][0] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], new NumericLane(slab, base, ROWS));
      }
      assertTrue(operands[1][0].laneBase() > 0L, "the second lane must not start at the slab's own base");
      assertDerived(oracle(presence, MUL, NO_CONSTS), evaluateOne(operands, MUL, NO_CONSTS), "one shared slab");
    }
  }

  @Test
  @DisplayName("a leaf the keep mask drops is the pruned sentinel and is never read")
  void aDroppedLeafIsThePrunedSentinel() {
    final long[][] presence = allPresent();
    try (Arena arena = Arena.ofConfined()) {
      final ColumnSlice[][] single = slabOperands(arena, presence);
      final ColumnSlice[][] two = new ColumnSlice[OPERANDS][2];
      for (int c = 0; c < OPERANDS; c++) {
        two[c][0] = single[c][0];
        // Leaf 1 would throw if it were evaluated: no lane at all.
        two[c][1] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], null, null, null, null, null, null, null);
      }
      final ColumnSlice[] out = ProjectionComputedColumn.evaluate(two, MUL, NO_CONSTS, new long[] {1L});
      assertEquals(2, out.length, "one derived slice per leaf");
      assertDerived(oracle(presence, MUL, NO_CONSTS), out[0], "the kept leaf");
      assertSame(ProjectionColumnStore.prunedSlice(), out[1], "the dropped leaf is the shared rowless sentinel");
    }
  }

  @Test
  @DisplayName("an overflowing row declines through an ArithmeticException on both read forms")
  void anOverflowingRowDeclinesOnBothForms() {
    for (final int bits : new int[] {64, 1}) {
      final long[][] presence = {new long[] {mask(bits)}, new long[] {mask(bits)}};
      final boolean gathers = ProjectionColumnGroupScan.gatherPays(presence[0][0]);
      assertTrue(gathers == bits > ProjectionVectorKernels.FOLD_WALK_MAX_BITS,
          "the fixture must take the read form it claims: " + bits + " rows, gathered=" + gathers);
      try (Arena arena = Arena.ofConfined()) {
        final ColumnSlice[][] operands = new ColumnSlice[OPERANDS][1];
        for (int c = 0; c < OPERANDS; c++) {
          final MemorySegment slab = arena.allocate(8L * 64, 64);
          for (int r = 0; r < 64; r++) {
            SegmentAccess.putLong(slab, (long) r << 3, c == 0
                ? Long.MAX_VALUE
                : 2L);
          }
          operands[c][0] = new ColumnSlice(64, (byte) 0, 0L, 0L, presence[c], new NumericLane(slab, 0L, 64));
        }
        assertThrows(ArithmeticException.class, () -> ProjectionComputedColumn.evaluate(operands, MUL, NO_CONSTS, null),
            "an exact product that overflows must decline, not wrap");
      }
    }
  }

  @Test
  @DisplayName("a malformed program, a shape mismatch and a laneless operand are refused by name")
  void malformedInputIsRefused() {
    final long[][] presence = allPresent();
    try (Arena arena = Arena.ofConfined()) {
      final ColumnSlice[][] operands = slabOperands(arena, presence);
      assertThrows(
          IllegalArgumentException.class, () -> ProjectionComputedColumn.evaluate(operands,
              new int[] {0, ProjectionIndexByteScan.COMPUTED_OP_MUL}, NO_CONSTS, null),
          "a program that underflows its stack");
      assertThrows(IllegalArgumentException.class,
          () -> ProjectionComputedColumn.evaluate(operands, new int[] {0, 1}, NO_CONSTS, null),
          "a program leaving two values");
      assertThrows(IllegalArgumentException.class,
          () -> ProjectionComputedColumn.evaluate(new ColumnSlice[][] {operands[0]}, MUL, NO_CONSTS, null),
          "a program pushing an operand that was not supplied");
      final ColumnSlice[][] shorter = {operands[0], {shortSlice(arena, presence[1])}};
      assertThrows(IllegalArgumentException.class,
          () -> ProjectionComputedColumn.evaluate(shorter, MUL, NO_CONSTS, null),
          "operands disagreeing on the row count of a leaf");
      final ColumnSlice[][] laneless = {operands[0],
          {new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[1], null, null, null, null, null, null, null)}};
      assertThrows(IllegalArgumentException.class,
          () -> ProjectionComputedColumn.evaluate(laneless, MUL, NO_CONSTS, null),
          "an operand that is not a long-lane column");
    }
  }

  // ==================== harness ====================

  /** Every row of every operand present: {@code gatherPays} holds for every word. */
  private long[][] allPresent() {
    final long[][] presence = new long[OPERANDS][WORDS];
    for (int c = 0; c < OPERANDS; c++) {
      for (int w = 0; w < WORDS; w++) {
        presence[c][w] = w == WORDS - 1
            ? (1L << (ROWS - (w << 6))) - 1L
            : -1L;
      }
    }
    return presence;
  }

  /** At most {@code FOLD_WALK_MAX_BITS} rows per word: no word gathers. */
  private long[][] sparsePresence() {
    final long[][] presence = new long[OPERANDS][WORDS];
    for (int c = 0; c < OPERANDS; c++) {
      for (int w = 0; w < WORDS; w++) {
        presence[c][w] = mask(ProjectionVectorKernels.FOLD_WALK_MAX_BITS);
      }
    }
    return presence;
  }

  /** Dense and sparse words in one leaf, and columns that disagree about which rows are present. */
  private long[][] mixedPresence() {
    final long[][] presence = new long[OPERANDS][WORDS];
    final Random rnd = new Random(0xC0FFEEL);
    for (int w = 0; w < WORDS; w++) {
      final boolean dense = (w & 1) == 0;
      for (int c = 0; c < OPERANDS; c++) {
        presence[c][w] = dense
            ? rnd.nextLong() | 0x0F0F0F0F0F0F0F0FL // >= 32 bits: the AND of the two still gathers
            : mask(2 + c);
      }
    }
    return presence;
  }

  /** No row has every operand: every word is empty and no form runs at all. */
  private long[][] nonePresent() {
    final long[][] presence = new long[OPERANDS][WORDS];
    for (int w = 0; w < WORDS; w++) {
      presence[0][w] = 0x5555555555555555L;
      presence[1][w] = 0xAAAAAAAAAAAAAAAAL; // disjoint: the AND is zero everywhere
    }
    return presence;
  }

  private static long mask(final int bits) {
    return bits >= 64
        ? -1L
        : (1L << bits) - 1L;
  }

  /** The word of derived presence, as {@code evaluateLeaf} computes it: every operand present. */
  private long derivedWord(final long[][] presence, final int w) {
    long word = -1L;
    for (int c = 0; c < OPERANDS; c++) {
      word &= presence[c][w];
    }
    final int tailBits = ROWS - (w << 6);
    return tailBits < 64
        ? word & ((1L << tailBits) - 1L)
        : word;
  }

  /** One slab per operand, as {@code ProjectionColumnStore.fillColumn} allocates them. */
  private ColumnSlice[][] slabOperands(final Arena arena, final long[][] presence) {
    final ColumnSlice[][] operands = new ColumnSlice[OPERANDS][1];
    for (int c = 0; c < OPERANDS; c++) {
      final MemorySegment slab = arena.allocate(8L * ROWS, 64);
      for (int r = 0; r < ROWS; r++) {
        SegmentAccess.putLong(slab, (long) r << 3, values[c][r]);
      }
      operands[c][0] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], new NumericLane(slab, 0L, ROWS));
    }
    return operands;
  }

  /** The shape an unconverted producer still hands out: a heap value array, no lane object. */
  private ColumnSlice[][] heapOperands(final long[][] presence) {
    final ColumnSlice[][] operands = new ColumnSlice[OPERANDS][1];
    for (int c = 0; c < OPERANDS; c++) {
      operands[c][0] =
          new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], values[c].clone(), null, null, null, null, null, null);
    }
    return operands;
  }

  private ColumnSlice shortSlice(final Arena arena, final long[] presence) {
    final MemorySegment slab = arena.allocate(8L * (ROWS - 1), 64);
    return new ColumnSlice(ROWS - 1, (byte) 0, 0L, 0L, presence, new NumericLane(slab, 0L, ROWS - 1));
  }

  private ColumnSlice evaluateOne(final ColumnSlice[][] operands, final int[] code, final long[] consts) {
    final ColumnSlice[] out = ProjectionComputedColumn.evaluate(operands, code, consts, null);
    assertEquals(1, out.length, "one derived slice per leaf");
    return out[0];
  }

  /**
   * Independent answer: the derived values, presence and zone map computed straight from the fixture,
   * with no kernel and no window involved.
   */
  private ColumnSlice oracle(final long[][] presence, final int[] code, final long[] consts) {
    final long[] words = new long[WORDS];
    final long[] expected = new long[ROWS];
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int row = 0; row < ROWS; row++) {
      final int w = row >>> 6;
      boolean present = true;
      for (int c = 0; c < OPERANDS; c++) {
        present &= (presence[c][w] & 1L << (row & 63)) != 0L;
      }
      if (!present) {
        continue;
      }
      words[w] |= 1L << (row & 63);
      int depth = 0;
      final long[] stack = new long[8];
      for (final int op : code) {
        if (op >= ProjectionIndexByteScan.COMPUTED_CONST_BASE) {
          stack[depth++] = consts[op - ProjectionIndexByteScan.COMPUTED_CONST_BASE];
        } else if (op >= 0) {
          stack[depth++] = values[op][row];
        } else {
          final long b = stack[--depth];
          final long a = stack[--depth];
          stack[depth++] = op == ProjectionIndexByteScan.COMPUTED_OP_MUL
              ? Math.multiplyExact(a, b)
              : op == ProjectionIndexByteScan.COMPUTED_OP_ADD
                  ? Math.addExact(a, b)
                  : Math.subtractExact(a, b);
        }
      }
      final long acc = stack[0];
      expected[row] = acc;
      min = Math.min(min, acc);
      max = Math.max(max, acc);
    }
    // No reset when nothing is present: the kernel leaves the zone map inverted (min > max), which is
    // how every evaluator reads "this leaf holds no derived value". Mirrored here, not corrected.
    return new ColumnSlice(ROWS, (byte) 0, min, max, words, expected, null, null, null, null, null, null);
  }

  private static void assertDerived(final ColumnSlice expected, final ColumnSlice actual, final String what) {
    assertEquals(expected.rowCount(), actual.rowCount(), what + ": row count");
    assertArrayEquals(expected.presenceWords(), actual.presenceWords(), what + ": derived presence");
    final long[] actualValues = actual.heapNumericValues();
    assertNotNull(actualValues, what + ": the derived column is a heap value array");
    assertArrayEquals(expected.heapNumericValues(), actualValues, what + ": derived values");
    assertEquals(expected.min(), actual.min(), what + ": zone-map min");
    assertEquals(expected.max(), actual.max(), what + ": zone-map max");
  }
}
