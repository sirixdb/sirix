/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchLease;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LeafColumnAccess;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The windowed access's borrowed lanes: a windowed group-by pass decoded every leaf of every column
 * into fresh arrays that died two windows later (3.2 GB per pass per column set at 100M). The
 * access used to recycle them through a free stack of its own; it now decodes each window's long
 * lanes into one buffer of allocator scratch and returns the buffer when the window's last slice
 * leaves its cache. These tests pin what makes that safe: the answers are unchanged, a buffer stays
 * borrowed for as long as one cached slice reads it and no longer, and closing the access (or
 * failing a decode) returns whatever is borrowed.
 *
 * <p>
 * The assertions are deltas of the process allocator's counters for {@link ScratchUse#PROJECTION}
 * and of its lent-buffer gauge, so the class runs isolated.
 */
@Isolated
final class WindowedSliceLanesTest {

  /** Column 0 wide random, column 1 constant (FOR width 0), column 2 present on even leaves only. */
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
  private static final int LEAVES = 21;
  private static final int ROWS = 64;
  /** The last leaf is short, and alone in its window. */
  private static final int SHORT_ROWS = 37;
  private static final int WINDOW = 4;
  private static final int CACHE = 2 * WINDOW;

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher) {
  }

  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L * 1024 * 1024 * 1024);
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  private static long borrows(final ScratchStats stats) {
    return stats.acquisitions(ScratchUse.PROJECTION);
  }

  /** The lane's rows read from the borrowed buffer itself, never through a heap copy. */
  private static long[] laneRows(final ColumnSlice slice) {
    final NumericLane lane = slice.lane();
    assertNotNull(lane, "a slice of a borrowing access carries a lane");
    final long[] rows = new long[slice.rowCount()];
    for (int row = 0; row < rows.length; row++) {
      rows[row] = lane.get(row);
    }
    return rows;
  }

  @Test
  @DisplayName("borrowed lanes answer exactly like heap arrays, one buffer per window and column")
  void borrowedLanesAnswerIdenticallyAndEveryWindowBorrowsOneBuffer() {
    final Fixture f = build();
    final ScratchStats before = stats();
    final LeafColumnAccess plain = f.store().residentLeafAccess(f.fetcher(), null);
    try (LeafColumnAccess borrowing = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE)) {
      for (int leaf = 0; leaf < LEAVES; leaf++) {
        for (int col = 0; col < KINDS.length; col++) {
          final ColumnSlice expected = plain.slice(col, leaf);
          final ColumnSlice actual = borrowing.slice(col, leaf);
          assertEquals(expected.rowCount(), actual.rowCount(), "rowCount leaf " + leaf + " col " + col);
          assertEquals(expected.min(), actual.min(), "min leaf " + leaf + " col " + col);
          assertEquals(expected.max(), actual.max(), "max leaf " + leaf + " col " + col);
          assertArrayEquals(expected.presenceWords(), actual.presenceWords(), "presence leaf " + leaf + " col " + col);
          assertArrayEquals(laneRows(expected), laneRows(actual), "values leaf " + leaf + " col " + col);
          assertNull(actual.heapNumericValues(), "a borrowed lane has no heap array beside it");
        }
      }
      final ScratchStats walked = stats();
      // 21 leaves in windows of 4: five full windows and the short last leaf, per column.
      assertEquals(6 * KINDS.length, borrows(walked) - borrows(before), "one buffer per window and column");
      // The cache holds the last 8 leaves of a column, 13..20: three of window 3, window 4, window 5.
      assertEquals(before.totalLentBuffers() + 3 * KINDS.length, walked.totalLentBuffers(),
          "only the windows with a cached slice are still borrowed");
      assertEquals(0, borrows(stats()) - borrows(walked), "the plain access borrows nothing");
    }
    final ScratchStats closed = stats();
    assertEquals(before.totalLentBuffers(), closed.totalLentBuffers(), "closing returned the cached windows");
    assertEquals(0, closed.staleReleases() - before.staleReleases());
    assertEquals(0, closed.refusals() - before.refusals());
  }

  @Test
  @DisplayName("a window's buffer stays borrowed until its LAST slice leaves the cache")
  void aWindowsLanesStayBorrowedUntilItsLastSliceLeavesTheCache() {
    final Fixture f = build();
    final LeafColumnAccess plain = f.store().residentLeafAccess(f.fetcher(), null);
    final long[] expectedLeaf0 = laneRows(plain.slice(0, 0));
    final ScratchStats before = stats();
    try (LeafColumnAccess borrowing = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE)) {
      borrowing.slice(0, 0); // window 0
      borrowing.slice(0, WINDOW); // window 1: the cache holds leaves 0..7
      final ColumnSlice leaf0 = borrowing.slice(0, 0); // a hit: leaf 0 is now the most recently used
      assertEquals(before.totalLentBuffers() + 2, stats().totalLentBuffers());

      borrowing.slice(0, 2 * WINDOW); // window 2 evicts leaves 1, 2, 3 and 4
      assertEquals(before.totalLentBuffers() + 3, stats().totalLentBuffers(),
          "window 0 lost three slices and window 1 one; both still have a cached slice");
      // Whatever the allocator has idle of this capacity now holds other bytes: had window 0's buffer
      // been returned when its first slice was evicted, leaf 0 would read them.
      final List<ScratchLease> others = new ArrayList<>();
      for (int i = 0; i < 64; i++) {
        final ScratchLease other = ScratchLease.of(WINDOW * ROWS * (long) Long.BYTES, ScratchUse.PROJECTION);
        other.segment().fill((byte) 0x5A);
        others.add(other);
      }
      for (final ScratchLease other : others) {
        other.close();
      }
      assertArrayEquals(expectedLeaf0, laneRows(leaf0), "leaf 0 is cached: its lane is still its own");

      borrowing.slice(0, 3 * WINDOW); // window 3 evicts leaves 5, 6, 7 and 0
      assertEquals(before.totalLentBuffers() + 2, stats().totalLentBuffers(),
          "windows 0 and 1 have no cached slice left: only windows 2 and 3 are borrowed");
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers());
    assertEquals(0, stats().staleReleases() - before.staleReleases());
  }

  @Test
  @DisplayName("a window decoded again replaces its cached slices and returns the lanes they read")
  void aWindowDecodedAgainReturnsTheLanesOfTheSlicesItReplaces() {
    final Fixture f = build();
    final LeafColumnAccess plain = f.store().residentLeafAccess(f.fetcher(), null);
    final ScratchStats before = stats();
    try (LeafColumnAccess borrowing = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE)) {
      borrowing.slice(0, 0);
      borrowing.slice(0, WINDOW);
      borrowing.slice(0, 0);
      borrowing.slice(0, 2 * WINDOW); // leaf 0 is all that is cached of window 0
      final ScratchStats partial = stats();
      assertEquals(before.totalLentBuffers() + 3, partial.totalLentBuffers());

      // Leaf 1 was evicted: the miss decodes window 0 again, into a buffer of its own, and the new
      // slice of leaf 0 replaces the cached one. The 11 cached leaves then lose their oldest three,
      // the rest of window 1.
      final ColumnSlice leaf1 = borrowing.slice(0, 1);
      final ScratchStats again = stats();
      assertEquals(1, borrows(again) - borrows(partial), "the second decode of window 0 borrowed its own buffer");
      assertEquals(before.totalLentBuffers() + 2, again.totalLentBuffers(),
          "the first decode's buffer went back with the slice that was replaced, window 1's with its last slice");
      assertArrayEquals(laneRows(plain.slice(0, 1)), laneRows(leaf1));
      assertArrayEquals(laneRows(plain.slice(0, 0)), laneRows(borrowing.slice(0, 0)));
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers());
    assertEquals(0, stats().staleReleases() - before.staleReleases());
  }

  @Test
  @DisplayName("presence words of evicted slices serve the leaves decoded next and carry nothing over")
  void presenceWordsAreReusedWithinTheAccessAndCarryNoStaleBits() {
    final Fixture f = build();
    try (LeafColumnAccess borrowing = f.store().windowedLeafAccess(f.fetcher(), null, WINDOW, CACHE)) {
      // Column 2 is present on even leaves and all-missing on odd ones, so an array that held a full
      // leaf's bits is rewritten for an empty one and the other way round.
      final Set<long[]> madeBeforeTheFirstEviction = Collections.newSetFromMap(new IdentityHashMap<>());
      final Set<long[]> inUse = Collections.newSetFromMap(new IdentityHashMap<>());
      for (int leaf = 0; leaf < LEAVES; leaf++) {
        final ColumnSlice slice = borrowing.slice(2, leaf);
        final long[] words = slice.presenceWords();
        assertEquals(1, words.length, "64 rows and 37 rows are one presence word");
        final long expected = (leaf & 1) == 1
            ? 0L
            : leaf == LEAVES - 1
                ? (1L << SHORT_ROWS) - 1
                : -1L;
        assertEquals(expected, words[0], "presence of leaf " + leaf);
        // Windows 0, 1 and 2 are decoded before any slice has left the cache of two windows; from
        // window 3 on every leaf is decoded into the words of a slice evicted two windows earlier.
        if (leaf < 3 * WINDOW) {
          madeBeforeTheFirstEviction.add(words);
        } else {
          assertTrue(madeBeforeTheFirstEviction.contains(words),
              "leaf " + leaf + " got presence words of its own although evicted slices had left theirs");
        }
        if (leaf % WINDOW == 0) {
          inUse.clear();
        }
        assertTrue(inUse.add(words), "two leaves of one window share presence words");
      }
      assertEquals(3 * WINDOW, madeBeforeTheFirstEviction.size(), "one array per leaf until the first eviction");
    }
  }

  @Test
  @DisplayName("a window whose segments cannot be fetched or verified returns its buffer")
  void aFailedWindowDecodeReturnsItsBuffer() {
    final Fixture f = build();
    final boolean[] broken = new boolean[2];
    final ColumnSegmentFetcher failing = offsets -> {
      if (broken[0]) {
        throw new IllegalStateException("the segment store is gone");
      }
      final byte[][] segments = f.fetcher().fetchAll(offsets);
      if (broken[1]) {
        // The last byte of the last segment: its checksum no longer matches the descriptor's.
        final byte[] damaged = segments[segments.length - 1].clone();
        damaged[damaged.length - 1] ^= 0x01;
        segments[segments.length - 1] = damaged;
      }
      return segments;
    };
    final ScratchStats before = stats();
    try (LeafColumnAccess borrowing = f.store().windowedLeafAccess(failing, null, WINDOW, CACHE)) {
      borrowing.slice(0, 0);
      broken[0] = true;
      assertThrows(IllegalStateException.class, () -> borrowing.slice(0, WINDOW));
      broken[0] = false;
      broken[1] = true;
      assertThrows(IllegalStateException.class, () -> borrowing.slice(0, WINDOW));
      final ScratchStats failed = stats();
      assertEquals(3, borrows(failed) - borrows(before), "window 0, and a buffer for each attempt at window 1");
      assertEquals(before.totalLentBuffers() + 1, failed.totalLentBuffers(),
          "the failed attempts returned their buffers: only window 0 is borrowed");
      broken[1] = false;
      assertEquals(ROWS, borrowing.slice(0, WINDOW).rowCount(), "the access is usable after a failed decode");
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers());
    assertEquals(0, stats().staleReleases() - before.staleReleases());
  }

  @Test
  @DisplayName("slice arrays over borrowed lanes refuse a fill their cache cannot hold and return their windows on close")
  void sliceArraysRefuseAFillWiderThanTheCacheAndReturnTheirWindowsOnClose() {
    final Fixture f = build();
    final ScratchStats before = stats();
    final WindowedSliceArrays arrays = new WindowedSliceArrays(f.store(), f.fetcher(), null, WINDOW, CACHE);
    assertThrows(IllegalArgumentException.class, () -> arrays.column(0, 0, CACHE - WINDOW + 2));
    final ColumnSlice[] filled = arrays.column(0, 0, CACHE - WINDOW + 1);
    assertEquals(ROWS, filled[0].rowCount());
    assertNotNull(filled[CACHE - WINDOW].lane());
    arrays.release(0, CACHE - WINDOW + 1);
    assertNull(filled[0], "a release drops the range's references");
    assertEquals(before.totalLentBuffers() + 2, stats().totalLentBuffers(),
        "a release keeps the cached windows: the next sub-chunk may start inside one");
    arrays.close();
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "closing returned them");
    arrays.close();
    assertEquals(0, stats().staleReleases() - before.staleReleases());

    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers());
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
