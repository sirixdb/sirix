/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;
import java.util.function.IntToLongFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ProjectionRecordKeySet#filterMembership} answers identically whichever way it reads its
 * value lane, and it has three ways. A leaf with a candidate in more than one row in eight stages
 * its whole lane with one counted copy; otherwise the choice is per selection word, gathering the
 * word's 64 values into a reused 512-byte window above the crossover
 * {@link ProjectionVectorKernels#FOLD_WALK_MAX_BITS} and reading the lane in place per candidate
 * row below it. Choosing on speed alone is legitimate only while the three agree row for row, on
 * native slab lanes and on the heap-array lanes an unconverted producer still hands out — which is
 * what this test pins.
 *
 * <p>
 * Every mask is checked against an INDEPENDENT oracle computed here from the fixture's own values
 * and presence, not only against another form, so a defect shared by two of them still fails. Which
 * form runs is steered by the candidate density alone, the only thing the dispatch looks at, and
 * the four leaves of one pass cover all three plus the no-read case: leaf 0 stages, leaf 1 mixes
 * gathered and in-place words inside one leaf, leaf 2 reads every word in place, leaf 3 has no
 * candidate at all. {@link ProjectionRecordKeySet#membershipStagedLeaves()} and its two siblings
 * are asserted, so a fixture that silently stopped reaching its form fails rather than passing
 * vacuously.
 * </p>
 */
final class MembershipLaneReadFormsTest {

  /**
   * Not a multiple of 64, so the last word is partly invalid and a gather of it must clamp — and the
   * fixture puts a 16-candidate word there, so the clamped word is one the gather really serves. A
   * slab allocated at exactly {@code 8 * ROWS} bytes turns a missing clamp into a bounds failure.
   */
  private static final int ROWS = 557;

  private static final int WORDS = (ROWS + 63) >>> 6;

  /** Above this many candidates a leaf stages: {@code bits << 3 >= ROWS}, so 70 of 557 rows. */
  private static final int STAGE_MIN_CANDIDATES = (ROWS + 7) >>> 3;

  private static final int LEAVES = 4;

  private final long[][] values = new long[LEAVES][ROWS];

  private final long[][] presence = new long[LEAVES][WORDS];

  private final long[][] selected = new long[LEAVES][WORDS];

  MembershipLaneReadFormsTest() {
    final Random rnd = new Random(0xB17E5L);
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      for (int row = 0; row < ROWS; row++) {
        // A small value domain on purpose: the membership set then hits and misses often, so both
        // sides of `anti != member` are exercised on every form.
        values[leaf][row] = rnd.nextLong(-32, 32);
      }
      // Every row carries a cell except every eleventh, so the ANTI form's "selected but missing"
      // word operation is exercised beside the walk on every leaf.
      for (int row = 0; row < ROWS; row++) {
        if (row % 11 != 0) {
          presence[leaf][row >>> 6] |= 1L << (row & 63);
        }
      }
    }
    // Leaf 0: candidates everywhere — denser than one row in eight, so the whole lane is staged.
    for (int row = 0; row < ROWS; row += 2) {
      selected[0][row >>> 6] |= 1L << (row & 63);
    }
    // Leaf 1: three words of 16 candidates and two of 4, 56 in all — under the staging threshold, so
    // the dispatch is per word and this one leaf takes the gather AND the in-place read. One of the
    // dense words is the partly valid last word, which pins the gather's clamp.
    for (final int word : new int[] {0, 4, WORDS - 1}) {
      selected[1][word] |= 0xFFFFL;
    }
    for (final int word : new int[] {2, 6}) {
      selected[1][word] |= 0xFL;
    }
    // Leaf 2: three or four candidates in every word, 31 in all — every word at or below the
    // crossover and the leaf under the staging threshold, so every word is read in place.
    for (int word = 0; word < WORDS; word++) {
      selected[2][word] |= 0xFL << (word & 3);
    }
    // Leaf 3: no candidate at all. Its rows are only ever kept by the ANTI form's word operation.
    for (int row = 0; row < ROWS; row += 11) {
      selected[3][row >>> 6] |= 1L << (row & 63);
    }
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      // Tail bits beyond ROWS stay clear, the contract every caller of the membership walk keeps.
      selected[leaf][WORDS - 1] &= tailMask();
      presence[leaf][WORDS - 1] &= tailMask();
    }
  }

  private static long tailMask() {
    final int valid = ROWS - ((WORDS - 1) << 6);
    return valid == 64
        ? -1L
        : (1L << valid) - 1L;
  }

  @Test
  @DisplayName("the four leaves reach all three read forms, and no leaf reaches them all")
  void theFixtureCoversEveryReadForm() {
    assertTrue(candidates(0) >= STAGE_MIN_CANDIDATES, "leaf 0 must stage, has " + candidates(0));
    assertTrue(candidates(1) < STAGE_MIN_CANDIDATES, "leaf 1 must not stage, has " + candidates(1));
    assertTrue(candidates(2) < STAGE_MIN_CANDIDATES, "leaf 2 must not stage, has " + candidates(2));
    assertEquals(0, candidates(3), "leaf 3 must have no candidate");
    int dense = 0;
    int sparse = 0;
    for (int word = 0; word < WORDS; word++) {
      final int bits = Long.bitCount(selected[1][word] & presence[1][word]);
      if (bits > ProjectionVectorKernels.FOLD_WALK_MAX_BITS) {
        dense++;
      } else if (bits > 0) {
        sparse++;
      }
    }
    assertEquals(3, dense, "leaf 1 must gather three words");
    assertEquals(2, sparse, "leaf 1 must read two words in place");
    assertEquals(253, candidates(0), "leaf 0's candidate count is what makes it stage");
    assertEquals(51, candidates(1));
    assertEquals(31, candidates(2));
    assertTrue(
        Long.bitCount(selected[1][WORDS - 1] & presence[1][WORDS - 1]) > ProjectionVectorKernels.FOLD_WALK_MAX_BITS,
        "the partly valid last word must be gathered");
    for (int word = 0; word < WORDS; word++) {
      final int bits2 = Long.bitCount(selected[2][word] & presence[2][word]);
      assertTrue(bits2 > 0 && bits2 <= ProjectionVectorKernels.FOLD_WALK_MAX_BITS,
          "leaf 2 word " + word + " must carry candidates and stay below the crossover, has " + bits2);
    }
  }

  @Test
  @DisplayName("a slab lane and a heap lane give the oracle's masks, each through its own read form")
  void everyReadFormMatchesTheOracleOnBothLaneKinds() {
    for (final boolean anti : new boolean[] {false, true}) {
      for (final boolean empty : new boolean[] {false, true}) {
        final LongOpenHashSet set = empty
            ? new LongOpenHashSet(0)
            : membershipSet();
        try (Arena arena = Arena.ofConfined()) {
          final long[][] fromSlab = run(slabSlices(arena), set, anti);
          final long[][] fromHeap = run(heapSlices(), set, anti);
          for (int leaf = 0; leaf < LEAVES; leaf++) {
            final long[] oracle = oracle(leaf, set, anti);
            assertArrayEquals(oracle, fromSlab[leaf],
                "slab lane, leaf " + leaf + ", anti=" + anti + ", empty=" + empty);
            assertArrayEquals(oracle, fromHeap[leaf],
                "heap lane, leaf " + leaf + ", anti=" + anti + ", empty=" + empty);
          }
        }
      }
    }
  }

  @Test
  @DisplayName("each leaf reaches the form its density asks for, counted at the source")
  void theDispatchReachesTheFormEachDensityAsksFor() {
    try (Arena arena = Arena.ofConfined()) {
      final long stagedBefore = ProjectionRecordKeySet.membershipStagedLeaves();
      final long gatheredBefore = ProjectionRecordKeySet.membershipGatheredWords();
      final long inPlaceBefore = ProjectionRecordKeySet.membershipInPlaceWords();
      run(slabSlices(arena), membershipSet(), false);
      // Leaf 0 stages its 253 candidates. Leaf 1 gathers its three dense words (14, 15 and the last
      // word's 15 candidates) and reads its two sparse ones in place. Leaf 2 reads its nine words in
      // place. Leaf 3 touches no value at all.
      assertEquals(1, ProjectionRecordKeySet.membershipStagedLeaves() - stagedBefore, "staged leaves");
      assertEquals(3, ProjectionRecordKeySet.membershipGatheredWords() - gatheredBefore, "gathered words");
      assertEquals(11, ProjectionRecordKeySet.membershipInPlaceWords() - inPlaceBefore, "in-place words");
    }
  }

  @Test
  @DisplayName("a lane exactly as long as its rows is never read past, clamped last word included")
  void theGatherNeverReadsPastTheLane() {
    // The slab is allocated at exactly 8 * ROWS bytes, so a gather of the partly valid last word
    // that failed to clamp to its 45 valid rows would read 64 and fail the segment's bounds check
    // rather than returning a wrong answer quietly. Running every arm over that slab is the
    // assertion; the oracle comparison above is what makes a quiet wrong answer visible too.
    for (final boolean anti : new boolean[] {false, true}) {
      try (Arena arena = Arena.ofConfined()) {
        final ColumnSlice[] slices = slabSlices(arena);
        for (final ColumnSlice slice : slices) {
          assertNotNull(slice.lane(), "the fixture must hand the kernel a lane, not an array");
          assertEquals(ROWS, slice.rowCount());
        }
        final long[][] masks = run(slices, membershipSet(), anti);
        for (int leaf = 0; leaf < LEAVES; leaf++) {
          assertArrayEquals(oracle(leaf, membershipSet(), anti), masks[leaf], "leaf " + leaf);
        }
      }
    }
  }

  @Test
  @DisplayName("a leaf whose rows no longer fit a staging buffer falls back to the per-word forms")
  void aLeafWiderThanARowGroupReadsPerWord() {
    // ProjectionIndexRowGroupPage.MAX_ROWS bounds a projection leaf, so the staging buffer is sized
    // for it; a producer handing out a wider slice must still be answered, per word.
    final int wide = ProjectionIndexRowGroupPage.MAX_ROWS + 64;
    final int wideWords = (wide + 63) >>> 6;
    final long[] widePresence = new long[wideWords];
    final long[] wideSelected = new long[wideWords];
    final long[] wideValues = new long[wide];
    for (int row = 0; row < wide; row++) {
      wideValues[row] = row % 7;
      widePresence[row >>> 6] |= 1L << (row & 63);
      wideSelected[row >>> 6] |= 1L << (row & 63);
    }
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slab = arena.allocate(8L * wide, 64);
      for (int row = 0; row < wide; row++) {
        SegmentAccess.putLong(slab, (long) row << 3, wideValues[row]);
      }
      final ColumnSlice slice = new ColumnSlice(wide, (byte) 0, 0L, 0L, widePresence, new NumericLane(slab, 0L, wide));
      final LongOpenHashSet set = new LongOpenHashSet(new long[] {0L, 3L});
      final long stagedBefore = ProjectionRecordKeySet.membershipStagedLeaves();
      final ProjectionRecordKeySet.Masks masks = ProjectionRecordKeySet.filterMembership(new long[][] {wideSelected},
          new long[][] {{1L}}, new ColumnSlice[] {slice}, leaf -> leaf, set, false);
      assertEquals(stagedBefore, ProjectionRecordKeySet.membershipStagedLeaves(), "a wide leaf must not stage");
      final long[] mask = masks.byFirstKey().get(1L);
      assertNotNull(mask);
      final long[] expected = new long[wideWords];
      for (int row = 0; row < wide; row++) {
        if (set.contains(wideValues[row])) {
          expected[row >>> 6] |= 1L << (row & 63);
        }
      }
      assertArrayEquals(expected, mask);
    }
  }

  private int candidates(final int leaf) {
    int bits = 0;
    for (int word = 0; word < WORDS; word++) {
      bits += Long.bitCount(selected[leaf][word] & presence[leaf][word]);
    }
    return bits;
  }

  /** Every third present value, so the set both hits and misses on every leaf. */
  private LongOpenHashSet membershipSet() {
    final LongOpenHashSet set = new LongOpenHashSet();
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      for (int row = 0; row < ROWS; row += 3) {
        if ((presence[leaf][row >>> 6] & 1L << (row & 63)) != 0L) {
          set.add(values[leaf][row]);
        }
      }
    }
    return set;
  }

  /**
   * The masks the kernel produced, per leaf, with a leaf it dropped reported as all-zero so the
   * oracle comparison covers the drop too.
   */
  private long[][] run(final ColumnSlice[] slices, final LongOpenHashSet set, final boolean anti) {
    final long[][] keys = new long[LEAVES][];
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      keys[leaf] = new long[] {100L + leaf};
    }
    final IntToLongFunction physicalSlot = leaf -> leaf;
    final ProjectionRecordKeySet.Masks masks =
        ProjectionRecordKeySet.filterMembership(copyOfSelected(), keys, slices, physicalSlot, set, anti);
    final long[][] out = new long[LEAVES][];
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final long[] mask = masks.byFirstKey().get(100L + leaf);
      out[leaf] = mask == null
          ? new long[WORDS]
          : mask;
    }
    return out;
  }

  /** The kernel writes into masks of its own, but the fixture's words must not be shared anyway. */
  private long[][] copyOfSelected() {
    final long[][] copy = new long[LEAVES][];
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      copy[leaf] = selected[leaf].clone();
    }
    return copy;
  }

  /** The verdict of every selected row, computed from the fixture alone. */
  private long[] oracle(final int leaf, final LongOpenHashSet set, final boolean anti) {
    final long[] mask = new long[WORDS];
    for (int row = 0; row < ROWS; row++) {
      if ((selected[leaf][row >>> 6] & 1L << (row & 63)) == 0L) {
        continue;
      }
      final boolean present = (presence[leaf][row >>> 6] & 1L << (row & 63)) != 0L;
      final boolean member = present && set.contains(values[leaf][row]);
      if (anti != member) {
        mask[row >>> 6] |= 1L << (row & 63);
      }
    }
    return mask;
  }

  /** One slab per leaf, exactly as long as its rows, as {@code fillColumn} lays a column out. */
  private ColumnSlice[] slabSlices(final Arena arena) {
    final ColumnSlice[] slices = new ColumnSlice[LEAVES];
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final MemorySegment slab = arena.allocate(8L * ROWS, 64);
      for (int row = 0; row < ROWS; row++) {
        SegmentAccess.putLong(slab, (long) row << 3, values[leaf][row]);
      }
      slices[leaf] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[leaf], new NumericLane(slab, 0L, ROWS));
    }
    return slices;
  }

  /** The shape an unconverted producer still hands out: a heap value array, no lane object. */
  private ColumnSlice[] heapSlices() {
    final ColumnSlice[] slices = new ColumnSlice[LEAVES];
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      slices[leaf] = new ColumnSlice(ROWS, (byte) 0, 0L, 0L, presence[leaf], values[leaf].clone(), null, null, null,
          null, null, null);
    }
    return slices;
  }
}
