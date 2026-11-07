/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SegmentAccess;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two forms of every group-fold kernel answer identically. A kernel either reads the lanes in
 * place ({@code foldSlicedLane}, {@code foldNumericPlainLane}, {@code foldSumsSlicedLane}) or folds
 * the word's values from a gathered 64-row window ({@code foldSlicedWindow},
 * {@code foldNumericPlainWindow}, {@code foldSumsWindow}), and {@code gatherPays} picks between
 * them per word for speed alone. That is only legitimate while the two agree row for row, on native
 * slab lanes and on the heap-array lanes the still-heap windowed slices hand a converted kernel —
 * which is what this test pins.
 *
 * <p>
 * Every case is also checked against an INDEPENDENT oracle computed here rather than only against
 * the other kernel, so a defect shared by both forms cannot pass.
 */
final class GroupFoldLaneEquivalenceTest {

  /** Not a multiple of 64, so the last word is partly invalid and the gather must clamp. */
  private static final int ROWS = 517;

  private static final int WORDS = (ROWS + 63) >>> 6;
  private static final int AGGS = 3;
  private static final long ALL_SUMS_EXACT = -1L;

  /** Full stripe: {@code [count, firstSeen]} then {@code [count, sum, min, max]} per column. */
  private static final int FULL_WIDTH = 2 + 4 * AGGS;

  /** Sums-only stripe: {@code [count, firstSeen]} then {@code [count, sum]} per column. */
  private static final int SUMS_WIDTH = 2 + 2 * AGGS;

  private final long[][] values = new long[AGGS][ROWS];
  private final long[][] presence = new long[AGGS][WORDS];
  private final long[] mask = new long[WORDS];

  GroupFoldLaneEquivalenceTest() {
    final Random rnd = new Random(0x1A4E5L);
    for (int a = 0; a < AGGS; a++) {
      for (int r = 0; r < ROWS; r++) {
        // Signed and wide: min/max must be exercised on both sides of zero, and a sum of 517 of
        // these cannot overflow.
        values[a][r] = rnd.nextLong(-1L << 40, 1L << 40);
      }
      for (int w = 0; w < WORDS; w++) {
        presence[a][w] = rnd.nextLong(); // ~half the rows absent per column, independently
      }
    }
    for (int w = 0; w < WORDS; w++) {
      mask[w] = rnd.nextLong();
    }
  }

  @Test
  @DisplayName("the fixture covers both sides of the gather crossover")
  void theFixtureStraddlesTheCrossover() {
    boolean sparse = false;
    boolean dense = false;
    for (int w = 0; w < WORDS; w++) {
      final long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, WORDS, ROWS);
      dense |= ProjectionColumnGroupScan.gatherPays(word);
      sparse |= word != 0L && !ProjectionColumnGroupScan.gatherPays(word);
    }
    assertTrue(dense, "no word is dense enough to gather: the window form would go untested");
    assertTrue(sparse, "every word gathers: the in-place form would go untested");
  }

  @Test
  @DisplayName("foldSlicedWindow equals foldSlicedLane over a slab, over a heap segment, and the oracle")
  void theFullStripeAgreesOnBothForms() {
    try (Arena arena = Arena.ofConfined()) {
      final long[] inPlaceSlab = foldFull(slabOf(arena), false);
      final long[] windowedSlab = foldFull(slabOf(arena), true);
      final long[] inPlaceHeap = foldFull(heapSegments(), false);
      final long[] windowedHeap = foldFull(heapSegments(), true);
      final long[] expected = oracleFull();
      assertArrayEquals(expected, inPlaceSlab, "slab lanes read in place");
      assertArrayEquals(expected, windowedSlab, "slab lanes folded from a gathered window");
      assertArrayEquals(expected, inPlaceHeap, "heap-array lanes read in place");
      assertArrayEquals(expected, windowedHeap, "heap-array lanes folded from a gathered window");
    }
  }

  @Test
  @DisplayName("foldNumericPlainWindow equals foldNumericPlainLane and fills the same stripe")
  void thePlainKernelAgreesOnBothForms() {
    try (Arena arena = Arena.ofConfined()) {
      final long[] expected = oracleFull();
      assertArrayEquals(expected, foldPlain(slabOf(arena), false), "slab lanes read in place");
      assertArrayEquals(expected, foldPlain(slabOf(arena), true), "slab lanes folded from a window");
      assertArrayEquals(expected, foldPlain(heapSegments(), false), "heap-array lanes read in place");
      assertArrayEquals(expected, foldPlain(heapSegments(), true), "heap-array lanes folded from a window");
    }
  }

  @Test
  @DisplayName("foldSumsWindow equals foldSumsSlicedLane and the full stripe's count/sum pairs")
  void theSumsOnlyKernelAgreesOnBothForms() {
    final long[] full = oracleFull();
    final long[] expected = new long[SUMS_WIDTH];
    expected[0] = full[0];
    for (int a = 0; a < AGGS; a++) {
      expected[2 + 2 * a] = full[2 + 4 * a];
      expected[3 + 2 * a] = full[3 + 4 * a];
    }
    try (Arena arena = Arena.ofConfined()) {
      assertArrayEquals(expected, foldSums(slabOf(arena), false), "slab lanes read in place");
      assertArrayEquals(expected, foldSums(slabOf(arena), true), "slab lanes folded from a window");
      assertArrayEquals(expected, foldSums(heapSegments(), false), "heap-array lanes read in place");
      assertArrayEquals(expected, foldSums(heapSegments(), true), "heap-array lanes folded from a window");
    }
  }

  @Test
  @DisplayName("the gather clamps to the leaf's rows and never reads past the lane")
  void theGatherClampsTheTailWord() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment[] segs = slabOf(arena);
      final long[][] window = new long[AGGS][64];
      final int tail = WORDS - 1;
      // The tail word holds ROWS - 512 = 5 valid rows; a gather of the full 64 would run off the
      // lane, so this call proves the clamp by not throwing and by copying exactly what is there.
      ProjectionColumnGroupScan.gatherWindow(segs, new long[AGGS], AGGS, tail, ROWS, window);
      final int validRows = ROWS - (tail << 6);
      assertEquals(5, validRows, "the fixture's tail word is the one being clamped");
      for (int a = 0; a < AGGS; a++) {
        for (int r = 0; r < validRows; r++) {
          assertEquals(values[a][(tail << 6) + r], window[a][r], "gathered tail row " + r + " of column " + a);
        }
      }
    }
  }

  @Test
  @DisplayName("a lane at its own 64-byte-aligned base in a shared slab is read at that base")
  void lanesAtDistinctBasesInOneSlabAreReadAtTheirOwnBase() {
    try (Arena arena = Arena.ofConfined()) {
      final long laneStride = (ROWS * (long) Long.BYTES + 63L) & ~63L;
      final MemorySegment slab = arena.allocate(laneStride * AGGS, 64);
      final MemorySegment[] segs = new MemorySegment[AGGS];
      final long[] bases = new long[AGGS];
      for (int a = 0; a < AGGS; a++) {
        bases[a] = a * laneStride;
        segs[a] = slab; // ONE segment, distinct bases — what a masked view's slab looks like
        for (int r = 0; r < ROWS; r++) {
          SegmentAccess.putLong(slab, bases[a] + ((long) r << 3), values[a][r]);
        }
        assertEquals(0L, bases[a] & 63L, "lane bases stay 64-byte aligned");
      }
      assertFalse(bases[1] == 0L, "the second lane must not start at the slab's own base");
      assertArrayEquals(oracleFull(), foldFull(segs, bases, false), "shared slab, read in place");
      assertArrayEquals(oracleFull(), foldFull(segs, bases, true), "shared slab, folded from a window");
    }
  }

  @Test
  @DisplayName("an overflowing sum declines through the same ArithmeticException on both forms")
  void anOverflowingSumDeclinesOnBothForms() {
    final long[][] overflowing = {{Long.MAX_VALUE, 1L}};
    final long[][] bothPresent = {{3L}};
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slab = arena.allocate(16, 64);
      SegmentAccess.putLong(slab, 0, Long.MAX_VALUE);
      SegmentAccess.putLong(slab, 8, 1L);
      final long[] fromWindow = new long[6];
      final long[] fromLane = new long[6];
      ProjectionColumnGroupScan.foldNumericPlainWindow(fromWindow, 0, overflowing, bothPresent, 1, 0, 0, 0,
          ALL_SUMS_EXACT);
      ProjectionColumnGroupScan.foldNumericPlainLane(fromLane, 0, new MemorySegment[] {slab}, new long[] {0L},
          bothPresent, 1, 0, 0, 0, ALL_SUMS_EXACT);
      assertArrayEquals(fromWindow, fromLane, "the first row folds identically on both forms");
      assertThrows(ArithmeticException.class, () -> ProjectionColumnGroupScan.foldNumericPlainWindow(fromWindow, 0,
          overflowing, bothPresent, 1, 0, 1, 1, ALL_SUMS_EXACT));
      assertThrows(ArithmeticException.class, () -> ProjectionColumnGroupScan.foldNumericPlainLane(fromLane, 0,
          new MemorySegment[] {slab}, new long[] {0L}, bothPresent, 1, 0, 1, 1, ALL_SUMS_EXACT));
    }
  }

  // ==================== harness ====================

  /** One slab per column, as {@code ProjectionColumnStore.fillColumn} allocates them. */
  private MemorySegment[] slabOf(final Arena arena) {
    final MemorySegment[] segs = new MemorySegment[AGGS];
    for (int a = 0; a < AGGS; a++) {
      segs[a] = arena.allocate(ROWS * (long) Long.BYTES, 64);
      for (int r = 0; r < ROWS; r++) {
        SegmentAccess.putLong(segs[a], (long) r << 3, values[a][r]);
      }
    }
    return segs;
  }

  private MemorySegment[] heapSegments() {
    final MemorySegment[] segs = new MemorySegment[AGGS];
    for (int a = 0; a < AGGS; a++) {
      segs[a] = MemorySegment.ofArray(values[a]);
    }
    return segs;
  }

  private long[] foldFull(final MemorySegment[] segs, final boolean windowed) {
    return foldFull(segs, new long[AGGS], windowed);
  }

  private long[] foldFull(final MemorySegment[] segs, final long[] bases, final boolean windowed) {
    final long[] slot = new long[FULL_WIDTH];
    resetFull(slot);
    walk(segs, bases, windowed, (window, w, bit, row) -> {
      if (window != null) {
        ProjectionColumnGroupScan.foldSlicedWindow(slot, 0, window, presence, new int[AGGS][], null, null, AGGS, w, bit,
            row, -1, null, null, null, null, null, null, null, ALL_SUMS_EXACT, new int[AGGS][]);
      } else {
        ProjectionColumnGroupScan.foldSlicedLane(slot, 0, segs, bases, presence, new int[AGGS][], null, null, AGGS, w,
            bit, row, -1, null, null, null, null, null, null, null, ALL_SUMS_EXACT, new int[AGGS][], null, 0L);
      }
    });
    return slot;
  }

  private long[] foldPlain(final MemorySegment[] segs, final boolean windowed) {
    final long[] slot = new long[FULL_WIDTH];
    final long[] bases = new long[AGGS];
    resetFull(slot);
    walk(segs, bases, windowed, (window, w, bit, row) -> {
      if (window != null) {
        ProjectionColumnGroupScan.foldNumericPlainWindow(slot, 0, window, presence, AGGS, w, bit, row, ALL_SUMS_EXACT);
      } else {
        ProjectionColumnGroupScan.foldNumericPlainLane(slot, 0, segs, bases, presence, AGGS, w, bit, row,
            ALL_SUMS_EXACT);
      }
    });
    return slot;
  }

  private long[] foldSums(final MemorySegment[] segs, final boolean windowed) {
    final long[] slot = new long[SUMS_WIDTH];
    final long[] bases = new long[AGGS];
    walk(segs, bases, windowed, (window, w, bit, row) -> {
      if (window != null) {
        ProjectionColumnGroupScan.foldSumsWindow(slot, 0, window, presence, AGGS, w, bit, ALL_SUMS_EXACT);
      } else {
        ProjectionColumnGroupScan.foldSumsSlicedLane(slot, 0, segs, bases, presence, AGGS, w, bit, row, ALL_SUMS_EXACT);
      }
    });
    return slot;
  }

  /**
   * The mask walk every group kernel's caller runs. {@code windowed} forces the gather for every
   * non-empty word instead of letting {@code gatherPays} decide, so one fixture proves both forms
   * over the SAME rows — including the sparse words the production dispatch would read in place.
   */
  private void walk(final MemorySegment[] segs, final long[] bases, final boolean windowed, final RowSink sink) {
    final long[][] window = new long[AGGS][64];
    for (int w = 0; w < WORDS; w++) {
      long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, WORDS, ROWS);
      if (word == 0L) {
        continue;
      }
      final long[][] gathered = windowed
          ? ProjectionColumnGroupScan.gatherWindow(segs, bases, AGGS, w, ROWS, window)
          : null;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        sink.accept(gathered, w, bit, (w << 6) + bit);
      }
    }
  }

  private interface RowSink {
    void accept(long @Nullable [][] window, int word, int bit, int row);
  }

  /** Independent answer: no kernel involved, so a defect shared by both forms still fails. */
  private long[] oracleFull() {
    final long[] expected = new long[FULL_WIDTH];
    resetFull(expected);
    for (int row = 0; row < ROWS; row++) {
      final int w = row >>> 6;
      if ((mask[w] & ProjectionIndexByteScan.validRowsMask(w, WORDS, ROWS) & 1L << (row & 63)) == 0L) {
        continue;
      }
      expected[0]++;
      for (int a = 0; a < AGGS; a++) {
        if ((presence[a][w] & 1L << (row & 63)) == 0L) {
          continue;
        }
        final long v = values[a][row];
        final int at = 2 + 4 * a;
        expected[at]++;
        expected[at + 1] += v;
        expected[at + 2] = Math.min(expected[at + 2], v);
        expected[at + 3] = Math.max(expected[at + 3], v);
      }
    }
    return expected;
  }

  private static void resetFull(final long[] slot) {
    for (int a = 0; a < AGGS; a++) {
      final int at = 2 + 4 * a;
      slot[at + 2] = Long.MAX_VALUE;
      slot[at + 3] = Long.MIN_VALUE;
    }
  }
}
