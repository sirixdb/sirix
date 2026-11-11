/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.AfterEach;
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
 * {@link ProjectionComputedColumn} answers identically whichever way it reads its operands, and it
 * has three ways. A leaf on which every row carries every operand stages each operand lane once
 * with one counted copy and walks its rows counted; otherwise the choice is per selection word,
 * gathering the word's 64 values of every operand into a reused flat window above the crossover
 * {@link ProjectionVectorKernels#FOLD_WALK_MAX_BITS} and reading the lanes in place per surviving
 * row below it. Choosing on speed alone is legitimate only while the three agree row for row, on
 * native slab lanes and on the heap-array lanes an unconverted producer still hands out — which is
 * what this test pins.
 *
 * <p>
 * The derived slice is always checked against an INDEPENDENT oracle computed here, not only against
 * another form, so a defect shared by two of them still fails. Which form runs is steered by the
 * operand presence words alone, the only thing the dispatch looks at: the all-present fixture
 * stages the leaf, the mixed fixture — dense words that are not full, alternating with sparse ones
 * — takes the gather and the per-row read within one leaf, the sparse fixture reads every word per
 * row, and the none-present fixture returns before any read. Every case runs at one, two and three
 * operand columns.
 */
final class ComputedColumnLaneEquivalenceTest {
  private final Arena derivedArena = Arena.ofConfined();

  @AfterEach
  void closeDerivedLanes() {
    derivedArena.close();
  }

  private ColumnSlice[] evaluate(final ColumnSlice[][] operands, final int[] code, final long[] consts,
      final long[] keepWords) {
    return ProjectionComputedColumn.evaluate(operands, code, consts, keepWords, derivedArena.allocate(65536, 64));
  }


  /**
   * Not a multiple of 64, so the last word is partly invalid and the gather must clamp — and its 45
   * valid rows are above the crossover, so the clamped word is one the gather really serves.
   */
  private static final int ROWS = 557;

  private static final int WORDS = (ROWS + 63) >>> 6;

  /** The widest fixture; every case runs at one, two and three operand columns. */
  private static final int MAX_OPERANDS = 3;

  /** {@code cost * qty}: the program Q7, Q9, Q11 and Q12 of the bitemporal suite compile to. */
  private static final int[] MUL = {0, 1, ProjectionIndexByteScan.COMPUTED_OP_MUL};

  /** One operand and a constant, the narrowest program that still reads a lane. */
  private static final int[] PLUS_CONST =
      {0, ProjectionIndexByteScan.COMPUTED_CONST_BASE, ProjectionIndexByteScan.COMPUTED_OP_ADD};

  /** {@code cost * qty + fee}: three operand columns, so a row's reads are not a fixed pair. */
  private static final int[] MUL_PLUS_OPERAND =
      {0, 1, ProjectionIndexByteScan.COMPUTED_OP_MUL, 2, ProjectionIndexByteScan.COMPUTED_OP_ADD};

  private static final long[] CONSTS = {7L};

  private static final long[] NO_CONSTS = new long[0];

  private static final String[] DENSITIES = {"all-present", "sparse", "mixed", "none-present"};

  private final long[][] values = new long[MAX_OPERANDS][ROWS];

  ComputedColumnLaneEquivalenceTest() {
    final Random rnd = new Random(0x5E1E7L);
    for (int c = 0; c < MAX_OPERANDS; c++) {
      for (int r = 0; r < ROWS; r++) {
        // Signed and wide enough that min/max are exercised on both sides of zero, while a product
        // of two of them plus a third stays far inside a long.
        values[c][r] = rnd.nextLong(-1L << 20, 1L << 20);
      }
    }
  }

  @Test
  @DisplayName("the fixtures cover all three read forms and no fixture covers them all")
  void theFixturesCoverEveryReadForm() {
    for (int operands = 1; operands <= MAX_OPERANDS; operands++) {
      int allPresentRows = 0;
      int mixedRows = 0;
      for (int w = 0; w < WORDS; w++) {
        final long fullMask = mask(Math.min(64, ROWS - (w << 6)));
        final long full = derivedWord(allPresent(operands), w, operands);
        assertEquals(fullMask, full, "the all-present fixture must make word " + w + " full at " + operands
            + " operands, which is what staging the leaf needs");
        allPresentRows += Long.bitCount(full);
        final long mixed = derivedWord(mixedPresence(operands), w, operands);
        mixedRows += Long.bitCount(mixed);
        assertTrue(mixed != fullMask, "word " + w + " of the mixed fixture must not be full");
        if ((w & 1) == 0) {
          assertTrue(GroupArrayReference.gatherPays(mixed), "the mixed fixture must gather word " + w);
        } else {
          assertTrue(mixed != 0L, "word " + w + " of the mixed fixture must keep rows");
          assertFalse(GroupArrayReference.gatherPays(mixed), "the mixed fixture must read word " + w + " per row");
        }
        assertFalse(GroupArrayReference.gatherPays(derivedWord(sparsePresence(operands), w, operands)),
            "the sparse fixture must read word " + w + " per row");
        assertEquals(0L, derivedWord(nonePresent(operands), w, operands),
            "the none-present fixture must keep no row of word " + w);
      }
      assertEquals(ROWS, allPresentRows, "the all-present fixture must leave the leaf fully present");
      assertTrue(mixedRows < ROWS, "the mixed fixture must not leave the leaf fully present");
      // The LAST word of the mixed fixture is a dense one, so the gather's clamp to the leaf's 45
      // remaining rows is exercised there; the all-present fixture stages the whole leaf instead.
      assertTrue(GroupArrayReference.gatherPays(derivedWord(mixedPresence(operands), WORDS - 1, operands)),
          "the mixed fixture must gather the partly valid tail word, which is where the clamp shows");
    }
  }

  @Test
  @DisplayName("every density and every program width answers the oracle over slab lanes")
  void everyDensityAnswersTheOracleOverSlabLanes() {
    try (Arena arena = Arena.ofConfined()) {
      for (final Program program : programs()) {
        for (final String density : DENSITIES) {
          final long[][] presence = presenceOf(density, program.operands());
          assertDerived(oracle(presence, program),
              evaluateOne(slabOperands(arena, presence, program.operands()), program),
              program + " over slab lanes, " + density);
        }
      }
    }
  }

  @Test
  @DisplayName("every density and every program width answers the oracle over heap-array lanes")
  void everyDensityAnswersTheOracleOverHeapLanes() {
    for (final Program program : programs()) {
      for (final String density : DENSITIES) {
        final long[][] presence = presenceOf(density, program.operands());
        assertDerived(oracle(presence, program), evaluateOne(heapOperands(presence, program.operands()), program),
            program + " over heap-array lanes, " + density);
      }
    }
  }

  @Test
  @DisplayName("the tail word is clamped: no read past the lane, no derived value above the row count")
  void theTailWordIsClamped() {
    // Presence words carry set bits ABOVE the row count — what a leaf's last word looks like — and
    // the slabs are sized to exactly ROWS rows, so reading or gathering the full 64 would run off the
    // lane. Checked at every width, so both the counted read and the gather are tried.
    try (Arena arena = Arena.ofConfined()) {
      for (final Program program : programs()) {
        final long[][] presence = allPresent(program.operands());
        for (int c = 0; c < program.operands(); c++) {
          presence[c][WORDS - 1] = -1L;
        }
        final ColumnSlice slice = evaluateOne(slabOperands(arena, presence, program.operands()), program);
        assertDerived(oracle(presence, program), slice, program + " with a full tail word");
        assertEquals(mask(ROWS - ((WORDS - 1) << 6)), slice.presenceWords()[WORDS - 1],
            "the derived presence must clear every row above the leaf's row count");
      }
    }
  }

  @Test
  @DisplayName("lanes at distinct 64-byte-aligned bases of one slab are read at their own base")
  void lanesAtDistinctBasesInOneSlabAreReadAtTheirOwnBase() {
    try (Arena arena = Arena.ofConfined()) {
      for (final Program program : programs()) {
        final long[][] presence = allPresent(program.operands());
        final long stride = (ROWS * (long) Long.BYTES + 63L) & ~63L;
        final MemorySegment slab = arena.allocate(stride * program.operands(), 64);
        final ColumnSlice[][] operands = new ColumnSlice[program.operands()][1];
        for (int c = 0; c < program.operands(); c++) {
          final long base = c * stride;
          for (int r = 0; r < ROWS; r++) {
            SegmentAccess.putLong(slab, base + ((long) r << 3), values[c][r]);
          }
          assertEquals(0L, base & 63L, "lane bases stay 64-byte aligned");
          operands[c][0] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], new NumericLane(slab, base, ROWS));
        }
        if (program.operands() > 1) {
          assertTrue(operands[1][0].laneBase() > 0L, "the second lane must not start at the slab's own base");
        }
        assertDerived(oracle(presence, program), evaluateOne(operands, program), program + " over one shared slab");
      }
    }
  }

  @Test
  @DisplayName("a leaf the keep mask drops is the pruned sentinel and is never read")
  void aDroppedLeafIsThePrunedSentinel() {
    final Program program = new Program("cost*qty", MUL, NO_CONSTS, 2);
    final long[][] presence = allPresent(2);
    try (Arena arena = Arena.ofConfined()) {
      final ColumnSlice[][] single = slabOperands(arena, presence, 2);
      final ColumnSlice[][] two = new ColumnSlice[2][2];
      for (int c = 0; c < 2; c++) {
        two[c][0] = single[c][0];
        // Leaf 1 would throw if it were evaluated: no lane at all.
        two[c][1] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], null, null, null, null, null, null, null);
      }
      final ColumnSlice[] out = evaluate(two, MUL, NO_CONSTS, new long[] {1L});
      assertEquals(2, out.length, "one derived slice per leaf");
      assertDerived(oracle(presence, program), out[0], "the kept leaf");
      assertSame(ProjectionColumnStore.prunedSlice(), out[1], "the dropped leaf is the shared rowless sentinel");
    }
  }

  @Test
  @DisplayName("an overflowing row declines through an ArithmeticException on every read form")
  void anOverflowingRowDeclinesOnEveryForm() {
    // 64 rows present per word is the counted read, 9 the gather, 1 the per-row read.
    for (final int bits : new int[] {64, 9, 1}) {
      for (final Program program : programs()) {
        final long[][] presence = new long[program.operands()][1];
        for (int c = 0; c < program.operands(); c++) {
          presence[c][0] = mask(bits);
        }
        try (Arena arena = Arena.ofConfined()) {
          final ColumnSlice[][] operands = new ColumnSlice[program.operands()][1];
          for (int c = 0; c < program.operands(); c++) {
            final MemorySegment slab = arena.allocate(8L * 64, 64);
            for (int r = 0; r < 64; r++) {
              SegmentAccess.putLong(slab, (long) r << 3, c == 0
                  ? Long.MAX_VALUE
                  : 2L);
            }
            operands[c][0] = new ColumnSlice(64, (byte) 0, 0L, 0L, presence[c], new NumericLane(slab, 0L, 64));
          }
          assertThrows(ArithmeticException.class, () -> evaluate(operands, program.code(), program.consts(), null),
              "an exact " + program + " that overflows must decline, not wrap, at " + bits + " rows per word");
        }
      }
    }
  }

  @Test
  @DisplayName("a malformed program, a shape mismatch and a laneless operand are refused by name")
  void malformedInputIsRefused() {
    final long[][] presence = allPresent(2);
    try (Arena arena = Arena.ofConfined()) {
      final ColumnSlice[][] operands = slabOperands(arena, presence, 2);
      assertThrows(IllegalArgumentException.class,
          () -> evaluate(operands, new int[] {0, ProjectionIndexByteScan.COMPUTED_OP_MUL}, NO_CONSTS, null),
          "a program that underflows its stack");
      assertThrows(IllegalArgumentException.class, () -> evaluate(operands, new int[] {0, 1}, NO_CONSTS, null),
          "a program leaving two values");
      assertThrows(IllegalArgumentException.class,
          () -> evaluate(new ColumnSlice[][] {operands[0]}, MUL, NO_CONSTS, null),
          "a program pushing an operand that was not supplied");
      final ColumnSlice[][] shorter = {operands[0], {shortSlice(arena, presence[1])}};
      assertThrows(IllegalArgumentException.class, () -> evaluate(shorter, MUL, NO_CONSTS, null),
          "operands disagreeing on the row count of a leaf");
      final ColumnSlice[][] laneless = {operands[0],
          {new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[1], null, null, null, null, null, null, null)}};
      assertThrows(IllegalArgumentException.class, () -> evaluate(laneless, MUL, NO_CONSTS, null),
          "an operand that is not a long-lane column");
    }
  }

  // ==================== harness ====================

  /** One postfix program with the operand width it needs. */
  private record Program(String name, int[] code, long[] consts, int operands) {
    @Override
    public String toString() {
      return name;
    }
  }

  /** One unary, one binary and one ternary program: the counted read serves the first two. */
  private static Program[] programs() {
    return new Program[] {new Program("cost+7", PLUS_CONST, CONSTS, 1), new Program("cost*qty", MUL, NO_CONSTS, 2),
        new Program("cost*qty+fee", MUL_PLUS_OPERAND, NO_CONSTS, 3)};
  }

  private long[][] presenceOf(final String density, final int operandCount) {
    return switch (density) {
      case "all-present" -> allPresent(operandCount);
      case "sparse" -> sparsePresence(operandCount);
      case "mixed" -> mixedPresence(operandCount);
      case "none-present" -> nonePresent(operandCount);
      default -> throw new IllegalArgumentException(density);
    };
  }

  /**
   * Every valid row of every operand present: every word is FULL, the counted read's precondition.
   */
  private long[][] allPresent(final int operandCount) {
    final long[][] presence = new long[operandCount][WORDS];
    for (int c = 0; c < operandCount; c++) {
      for (int w = 0; w < WORDS; w++) {
        presence[c][w] = mask(Math.min(64, ROWS - (w << 6)));
      }
    }
    return presence;
  }

  /** At most {@code FOLD_WALK_MAX_BITS} rows per word: no word is full and no word gathers. */
  private long[][] sparsePresence(final int operandCount) {
    final long[][] presence = new long[operandCount][WORDS];
    for (int c = 0; c < operandCount; c++) {
      for (int w = 0; w < WORDS; w++) {
        presence[c][w] = mask(ProjectionVectorKernels.FOLD_WALK_MAX_BITS);
      }
    }
    return presence;
  }

  /**
   * Dense-but-not-full words alternating with sparse ones, and columns that disagree about which rows
   * are present: the gather and the per-row read, never the counted read.
   */
  private long[][] mixedPresence(final int operandCount) {
    final long[][] presence = new long[operandCount][WORDS];
    final Random rnd = new Random(0xC0FFEEL);
    for (int w = 0; w < WORDS; w++) {
      final boolean dense = (w & 1) == 0;
      for (int c = 0; c < operandCount; c++) {
        // Dense: the shared 0x0F0F pattern keeps at least 32 rows after the AND, and clearing row 1
        // keeps the word off full. Sparse: 2 + c rows, so the AND keeps 2.
        presence[c][w] = dense
            ? (rnd.nextLong() | 0x0F0F0F0F0F0F0F0EL) & ~2L
            : mask(2 + c);
      }
    }
    return presence;
  }

  /** No row has every operand: every word is empty and no form runs at all. */
  private long[][] nonePresent(final int operandCount) {
    final long[][] presence = new long[operandCount][WORDS];
    for (int c = 0; c < operandCount; c++) {
      for (int w = 0; w < WORDS; w++) {
        // Disjoint for the first two columns, so the AND is zero at every width above one; at one
        // operand the single column itself keeps nothing.
        presence[c][w] = operandCount == 1
            ? 0L
            : (c & 1) == 0
                ? 0x5555555555555555L
                : 0xAAAAAAAAAAAAAAAAL;
      }
    }
    return presence;
  }

  private static long mask(final int bits) {
    return bits >= 64
        ? -1L
        : (1L << bits) - 1L;
  }

  /** The word of derived presence, as {@code evaluateLeaf} computes it: every operand present. */
  private long derivedWord(final long[][] presence, final int w, final int operandCount) {
    long word = -1L;
    for (int c = 0; c < operandCount; c++) {
      word &= presence[c][w];
    }
    return word & mask(Math.min(64, ROWS - (w << 6)));
  }

  /** One slab per operand, as {@code ProjectionColumnStore.fillColumn} allocates them. */
  private ColumnSlice[][] slabOperands(final Arena arena, final long[][] presence, final int operandCount) {
    final ColumnSlice[][] operands = new ColumnSlice[operandCount][1];
    for (int c = 0; c < operandCount; c++) {
      final MemorySegment slab = arena.allocate(8L * ROWS, 64);
      for (int r = 0; r < ROWS; r++) {
        SegmentAccess.putLong(slab, (long) r << 3, values[c][r]);
      }
      operands[c][0] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], new NumericLane(slab, 0L, ROWS));
    }
    return operands;
  }

  /** The shape an unconverted producer still hands out: a heap value array, no lane object. */
  private ColumnSlice[][] heapOperands(final long[][] presence, final int operandCount) {
    final ColumnSlice[][] operands = new ColumnSlice[operandCount][1];
    for (int c = 0; c < operandCount; c++) {
      operands[c][0] =
          new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[c], values[c].clone(), null, null, null, null, null, null);
    }
    return operands;
  }

  private ColumnSlice shortSlice(final Arena arena, final long[] presence) {
    final MemorySegment slab = arena.allocate(8L * (ROWS - 1), 64);
    return new ColumnSlice(ROWS - 1, (byte) 0, 0L, 0L, presence, new NumericLane(slab, 0L, ROWS - 1));
  }

  private ColumnSlice evaluateOne(final ColumnSlice[][] operands, final Program program) {
    final ColumnSlice[] out = evaluate(operands, program.code(), program.consts(), null);
    assertEquals(1, out.length, "one derived slice per leaf");
    return out[0];
  }

  /**
   * Independent answer: the derived values, presence and zone map computed straight from the fixture,
   * with no kernel, no window and no dispatch involved.
   */
  private ColumnSlice oracle(final long[][] presence, final Program program) {
    final long[] words = new long[WORDS];
    final long[] expected = new long[ROWS];
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int row = 0; row < ROWS; row++) {
      final int w = row >>> 6;
      boolean present = true;
      for (int c = 0; c < program.operands(); c++) {
        present &= (presence[c][w] & 1L << (row & 63)) != 0L;
      }
      if (!present) {
        continue;
      }
      words[w] |= 1L << (row & 63);
      int depth = 0;
      final long[] stack = new long[8];
      for (final int op : program.code()) {
        if (op >= ProjectionIndexByteScan.COMPUTED_CONST_BASE) {
          stack[depth++] = program.consts()[op - ProjectionIndexByteScan.COMPUTED_CONST_BASE];
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
      final long v = stack[0];
      expected[row] = v;
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    // No reset when nothing is present: the kernel leaves the zone map inverted (min > max), which is
    // how every evaluator reads "this leaf holds no derived value". Mirrored here, not corrected.
    return new ColumnSlice(ROWS, (byte) 0, min, max, words, expected, null, null, null, null, null, null);
  }

  private static void assertDerived(final ColumnSlice expected, final ColumnSlice actual, final String what) {
    assertEquals(expected.rowCount(), actual.rowCount(), what + ": row count");
    assertArrayEquals(expected.presenceWords(), actual.presenceWords(), what + ": derived presence");
    final long[] actualValues = actual.numericValues();
    assertNotNull(actual.lane(), what + ": the derived column owns a numeric lane");
    assertArrayEquals(expected.heapNumericValues(), actualValues, what + ": derived values");
    assertEquals(expected.min(), actual.min(), what + ": zone-map min");
    assertEquals(expected.max(), actual.max(), what + ": zone-map max");
  }
}
