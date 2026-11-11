/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.index.projection.NumericLane;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.node.SegmentAccess;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MembershipValueSetReadFormsTest {

  private static final int ROWS = 557;

  private static final int WORDS = (ROWS + 63) >>> 6;

  private static final int LEAVES = 4;

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  private final long[][] values = new long[LEAVES][ROWS];

  private final long[][] presence = new long[LEAVES][WORDS];

  private final long[][] masks = new long[LEAVES][WORDS];

  MembershipValueSetReadFormsTest() {
    final Random rnd = new Random(0xA11E5L);
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      for (int row = 0; row < ROWS; row++) {
        // A small value domain on purpose: the interned set then holds far fewer values than rows,
        // so a form that read the wrong row would almost certainly still produce a plausible size.
        values[leaf][row] = rnd.nextLong(-32, 32);
      }
      for (int row = 0; row < ROWS; row++) {
        if (row % 11 != 0) {
          presence[leaf][row >>> 6] |= 1L << (row & 63);
        }
      }
    }
    for (int row = 0; row < ROWS; row += 2) {
      masks[0][row >>> 6] |= 1L << (row & 63);
    }
    for (final int word : new int[] {0, 4, WORDS - 1}) {
      masks[1][word] |= 0xFFFFL;
    }
    for (final int word : new int[] {2, 6}) {
      masks[1][word] |= 0xFL;
    }
    for (int word = 0; word < WORDS; word++) {
      masks[2][word] |= 0xFL << (word & 3);
    }
    // Leaf 3 selects only the rows that carry no cell, so it is a selected leaf that reads nothing.
    for (int row = 0; row < ROWS; row += 11) {
      masks[3][row >>> 6] |= 1L << (row & 63);
    }
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      masks[leaf][WORDS - 1] &= tailMask();
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
  @DisplayName("the fixture covers dense, sparse and empty selections")
  void theFixtureCoversDifferentSelectionDensities() {
    assertTrue(present(0) << 3 >= ROWS, "leaf 0 must be dense, has " + present(0));
    assertTrue(present(1) << 3 < ROWS, "leaf 1 must be sparse, has " + present(1));
    assertTrue(present(2) << 3 < ROWS, "leaf 2 must be sparse, has " + present(2));
    assertEquals(0, present(3), "leaf 3 must read nothing");
    int dense = 0;
    int sparse = 0;
    for (int word = 0; word < WORDS; word++) {
      final int bits = Long.bitCount(masks[1][word] & presence[1][word]);
      if (bits > 8) {
        dense++;
      } else if (bits > 0) {
        sparse++;
      }
    }
    assertEquals(3, dense, "leaf 1 must have three dense words");
    assertEquals(2, sparse, "leaf 1 must have two sparse words");
    assertTrue(Long.bitCount(masks[1][WORDS - 1] & presence[1][WORDS - 1]) > 8,
        "the partly valid last word must carry a dense selection");
    for (int word = 0; word < WORDS; word++) {
      final int bits = Long.bitCount(masks[2][word] & presence[2][word]);
      assertTrue(bits > 0 && bits <= 8, "leaf 2 word " + word + " must carry a sparse selection");
    }
  }

  @Test
  @DisplayName("a slab lane and a heap lane give the oracle's value set")
  void everyReadFormMatchesTheOracleOnBothLaneKinds() {
    final LongOpenHashSet oracle = oracle();
    try (Arena arena = Arena.ofConfined();
        MaskedColumns slab = view(slabSlices(arena));
        MaskedColumns heap = view(heapSlices())) {
      assertEquals(oracle, slab.presentLongValues(0), "slab lane");
      assertEquals(oracle, heap.presentLongValues(0), "heap lane");
    }
  }

  @Test
  @DisplayName("every nonempty selection word reads its lane directly")
  void everyNonemptyWordReadsTheLaneDirectly() {
    int expectedWords = 0;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      for (int word = 0; word < WORDS; word++) {
        if ((masks[leaf][word] & presence[leaf][word]) != 0L) {
          expectedWords++;
        }
      }
    }
    try (Arena arena = Arena.ofConfined(); MaskedColumns columns = view(slabSlices(arena))) {
      final long before = MaskedColumns.valueInPlaceWords();
      assertEquals(oracle(), columns.presentLongValues(0));
      assertEquals(expectedWords, MaskedColumns.valueInPlaceWords() - before);
    }
  }

  @Test
  @DisplayName("a lane exactly as long as its rows is never read past")
  void directReadsNeverReadPastTheLane() {
    try (Arena arena = Arena.ofConfined()) {
      final ColumnSlice[] slices = slabSlices(arena);
      for (final ColumnSlice slice : slices) {
        assertNotNull(slice.lane(), "the fixture must hand the kernel a lane, not an array");
        assertEquals(ROWS, slice.rowCount());
      }
      try (MaskedColumns columns = view(slices)) {
        assertEquals(oracle(), columns.presentLongValues(0));
      }
    }
  }

  private int present(final int leaf) {
    int bits = 0;
    for (int word = 0; word < WORDS; word++) {
      bits += Long.bitCount(masks[leaf][word] & presence[leaf][word]);
    }
    return bits;
  }

  /** The values of every selected row that carries a cell, computed from the fixture alone. */
  private LongOpenHashSet oracle() {
    final LongOpenHashSet expected = new LongOpenHashSet();
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      for (int row = 0; row < ROWS; row++) {
        final long bit = 1L << (row & 63);
        if ((masks[leaf][row >>> 6] & bit) != 0L && (presence[leaf][row >>> 6] & bit) != 0L) {
          expected.add(values[leaf][row]);
        }
      }
    }
    return expected;
  }

  private MaskedColumns view(final ColumnSlice[] slices) {
    final long[][] keys = new long[LEAVES][];
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      keys[leaf] = new long[] {100L + leaf};
    }
    final long[][] copies = new long[LEAVES][];
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      copies[leaf] = masks[leaf].clone();
    }
    return new MaskedColumns(store(), copies, KeyLanes.onHeap(keys), new ColumnSlice[][] {slices}, KINDS,
        (long) LEAVES * ROWS);
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

  /** A minimal store: this kernel reads the view's masks and slices, never the store. */
  private static ProjectionColumnStore store() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
    page.appendRow(10, new long[] {1}, new boolean[1], new String[1]);
    final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    assertNotNull(encoded);
    final long[] offsets = new long[encoded.columnSegmentIds().length];
    Arrays.fill(offsets, Constants.NULL_ID_LONG);
    return new ProjectionColumnStore(List.of(
        new RowGroupDirectory(1, encoded.descriptor(), encoded.columnSegmentIds(), offsets, encoded.segments())));
  }
}
