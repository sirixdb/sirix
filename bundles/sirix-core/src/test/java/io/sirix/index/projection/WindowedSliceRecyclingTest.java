/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LeafColumnAccess;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The windowed access's slice recycling ({@link SliceArrayPool}): a windowed group-by pass decoded every
 * leaf of every column into fresh arrays that died two windows later (3.2 GB per pass per column set at
 * 100M), so a recycling access hands an evicted slice's presence and value arrays to the next window's
 * decode. These tests pin the three things that make that safe: the answers are unchanged, every reused
 * array is fully overwritten (an all-missing leaf after a present one, FOR width 0 after wide values),
 * and a leaf whose length does not match a pooled array gets its own.
 */
final class WindowedSliceRecyclingTest {

  /** Column 0 wide random, column 1 constant (FOR width 0), column 2 present on even leaves only. */
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
  private static final int LEAVES = 21;
  private static final int ROWS = 64;
  /** The last leaf is short: its arrays never match a pooled length. */
  private static final int SHORT_ROWS = 37;
  private static final int WINDOW = 4;
  private static final int CACHE = 2 * WINDOW;

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher) {
  }

  @Test
  @DisplayName("a recycling access answers exactly like the allocating one and reuses the evicted arrays")
  void recyclingAnswersIdenticallyAndReuses() {
    final Fixture f = build();
    final long reusedBefore = ProjectionColumnStore.recycledSliceArraysCount();
    final LeafColumnAccess plain = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE, false);
    final LeafColumnAccess recycling = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE, true);
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      for (int col = 0; col < KINDS.length; col++) {
        final ColumnSlice expected = plain.slice(col, leaf);
        final ColumnSlice actual = recycling.slice(col, leaf);
        assertEquals(expected.rowCount(), actual.rowCount(), "rowCount leaf " + leaf + " col " + col);
        assertArrayEquals(expected.presenceWords(), actual.presenceWords(), "presence leaf " + leaf + " col " + col);
        assertArrayEquals(expected.numericValues(), actual.numericValues(), "values leaf " + leaf + " col " + col);
        assertEquals(expected.numericValues().length, actual.rowCount(), "a values array IS the row count");
      }
    }
    // 21 leaves = 5 full windows + 1 short leaf per column. The pool is shared by the access's columns,
    // so at least what ONE column's own evictions give back is reused (windows 3 and 4: 4 presence + 4
    // value arrays each), and at most every evicted array: (21 - 8) slices × 2 lanes per column. The
    // short leaf finds 64-row arrays on the stacks and allocates 37-row ones instead.
    final long reused = ProjectionColumnStore.recycledSliceArraysCount() - reusedBefore;
    assertTrue(reused >= 2 * WINDOW * 2 * KINDS.length, "reused " + reused + " below one column's own evictions");
    assertTrue(reused <= (LEAVES - CACHE) * 2 * KINDS.length, "reused " + reused + " exceeds the evicted arrays");
  }

  @Test
  @DisplayName("an evicted slice's arrays ARE a later slice's arrays — and the plain access never shares")
  void evictedArraysBecomeTheNextWindows() {
    final Fixture f = build();
    final LeafColumnAccess recycling = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE, true);
    final long[] w0Values = recycling.slice(0, 0).numericValues();
    final long[] w0Presence = recycling.slice(0, 0).presenceWords();
    final long[] w0Copy = w0Values.clone();
    recycling.slice(0, WINDOW); // window 1: cache holds 8
    recycling.slice(0, 2 * WINDOW); // window 2: evicts window 0 into the pool
    final ColumnSlice w3 = recycling.slice(0, 3 * WINDOW); // window 3: decodes INTO window 0's arrays
    boolean sharedValues = false;
    boolean sharedPresence = false;
    for (int i = 0; i < WINDOW; i++) {
      final ColumnSlice s = recycling.slice(0, 3 * WINDOW + i);
      sharedValues |= s.numericValues() == w0Values;
      sharedPresence |= s.presenceWords() == w0Presence;
    }
    assertTrue(sharedValues, "window 3 must own window 0's value arrays");
    assertTrue(sharedPresence, "window 3 must own window 0's presence arrays");
    assertEquals(ROWS, w3.numericValues().length);
    assertFalse(Arrays.equals(w0Copy, w0Values), "the recycled array was rewritten with window 3's rows");

    final LeafColumnAccess plain = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE, false);
    final long[] p0 = plain.slice(0, 0).numericValues();
    plain.slice(0, WINDOW);
    plain.slice(0, 2 * WINDOW);
    for (int i = 0; i < WINDOW; i++) {
      assertNotSame(p0, plain.slice(0, 3 * WINDOW + i).numericValues(), "the allocating access shares nothing");
    }
    assertArrayEquals(w0Copy, p0);
  }

  @Test
  @DisplayName("a reused presence array is zeroed for an all-missing leaf and a reused values array refilled by width 0")
  void reusedArraysCarryNoStaleWords() {
    final Fixture f = build();
    final LeafColumnAccess recycling = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE, true);
    // Column 2 is present on even leaves and all-missing on odd ones; column 1 is a constant. Walk far
    // enough that every window past the second decodes into recycled arrays, and check each leaf.
    for (int leaf = 0; leaf < 5 * WINDOW; leaf++) {
      final ColumnSlice missing = recycling.slice(2, leaf);
      final long[] words = missing.presenceWords();
      if ((leaf & 1) == 1) {
        for (final long w : words) {
          assertEquals(0L, w, "all-missing leaf " + leaf + " must have zero presence words");
        }
      } else {
        assertEquals(-1L, words[0], "present leaf " + leaf);
      }
      final long[] constant = recycling.slice(1, leaf).numericValues();
      for (int r = 0; r < ROWS; r++) {
        assertEquals(42L, constant[r], "constant column leaf " + leaf + " row " + r);
      }
    }
  }

  @Test
  @DisplayName("the short last leaf gets arrays of its own length, never a pooled 64-row one")
  void aShortLeafIsNotHandedAPooledArray() {
    final Fixture f = build();
    final LeafColumnAccess recycling = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE, true);
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      recycling.slice(0, leaf);
    }
    final ColumnSlice last = recycling.slice(0, LEAVES - 1);
    assertEquals(SHORT_ROWS, last.rowCount());
    assertEquals(SHORT_ROWS, last.numericValues().length);
    assertEquals(1, last.presenceWords().length);
    assertEquals((1L << SHORT_ROWS) - 1, last.presenceWords()[0]);
  }

  @Test
  @DisplayName("the pool hands out exact lengths only and never grows past its cap")
  void poolHandsOutExactLengthsOnly() {
    final SliceArrayPool pool = new SliceArrayPool();
    final long[] presence = new long[1];
    final long[] values = new long[64];
    pool.recycle(new ColumnSlice(64, (byte) 0, 0L, 0L, presence, values, null, null, null, null));
    assertEquals(2, pool.held());
    assertNotSame(values, pool.values(37), "a length mismatch allocates");
    assertSame(presence, pool.presence(1));
    assertEquals(0, pool.held(), "a mismatched top is dropped, not kept for later");
    for (int i = 0; i < SliceArrayPool.MAX_FREE + 5; i++) {
      pool.recycle(new ColumnSlice(64, (byte) 0, 0L, 0L, new long[1], new long[64], null, null, null, null));
    }
    assertEquals(2 * SliceArrayPool.MAX_FREE, pool.held());
    pool.recycle(new ColumnSlice(0, (byte) 0, 0L, 0L, new long[0], null, null, null, null, null));
    assertEquals(2 * SliceArrayPool.MAX_FREE, pool.held(), "a rowless slice's empty arrays are never pooled");
  }

  @Test
  @DisplayName("a recycling slice array refuses a fill range its cache cannot hold at once")
  void recyclingArraysRefuseAFillWiderThanTheCache() {
    final Fixture f = build();
    final WindowedSliceArrays arrays = new WindowedSliceArrays(f.store(), f.fetcher(), null, WINDOW, CACHE, true);
    assertThrows(IllegalArgumentException.class, () -> arrays.column(0, 0, CACHE - WINDOW + 2));
    final ColumnSlice[] filled = arrays.column(0, 0, CACHE - WINDOW + 1);
    assertEquals(ROWS, filled[0].rowCount());
    arrays.release(0, CACHE - WINDOW + 1);
    final WindowedSliceArrays allocating = new WindowedSliceArrays(f.store(), f.fetcher(), null, WINDOW, CACHE, false);
    assertEquals(ROWS, allocating.column(0, 0, LEAVES)[0].rowCount(), "an allocating access has no such bound");
  }

  private static Fixture build() {
    final Random rnd = new Random(20260903L);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(LEAVES);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final long[] longs = new long[KINDS.length];
      final boolean[] bools = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      final boolean[] present = {true, true, (leaf & 1) == 0};
      final boolean[] unrepresentable = new boolean[KINDS.length];
      final int rows = leaf == LEAVES - 1
          ? SHORT_ROWS
          : ROWS;
      long recordKey = leaf * 100_000L + 1;
      for (int row = 0; row < rows; row++) {
        longs[0] = rnd.nextLong();
        longs[1] = 42L;
        longs[2] = leaf * 1_000L + row;
        page.appendRow(recordKey++, longs, bools, strings, present, unrepresentable);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int segments = encoded.columnSegmentIds().length;
      final int[] ids = new int[segments];
      final long[] offsets = new long[segments];
      for (int i = 0; i < segments; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets, new byte[ids.length][]));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    return new Fixture(new ProjectionColumnStore(directories), fetcher);
  }
}
