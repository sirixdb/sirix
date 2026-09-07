/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SegmentDictionaryDirectoryNode;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where segments begin and end.
 *
 * <p>
 * The property that carries the class is not the close rule — it is that a page's segment is FIXED
 * the moment the page is adopted, because that page's values are minted later on a pool that asks
 * {@link SegmentBoundaries#segmentOf} without a lock. So a boundary may only ever be placed above
 * every adopted page, and the tests below adopt out of order on purpose.
 * </p>
 */
final class SegmentBoundariesTest {

  private static final long NO_BUDGET = Long.MAX_VALUE;

  private static final long NO_LEAF_CAP = Long.MAX_VALUE;

  @Test
  @DisplayName("a fresh instance has one open segment starting at page 0, and every key reads as it")
  void startsWithOneOpenSegment() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, NO_LEAF_CAP);
    assertEquals(1, boundaries.segmentCount());
    assertEquals(0, boundaries.openSegment());
    assertEquals(0L, boundaries.segmentStart(0));
    assertArrayEquals(new long[] {0L}, boundaries.starts());
    assertEquals(-1L, boundaries.highestAdoptedPageKey(), "nothing adopted yet");
    assertEquals(0, boundaries.segmentOf(0));
    assertEquals(0, boundaries.segmentOf(Long.MAX_VALUE), "the open segment is unbounded above");
  }

  @Test
  @DisplayName("the open segment's FIRST page never closes it, whatever the bytes or the key")
  void aFirstPageNeverCloses() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(1, 1);
    assertEquals(0, boundaries.adopt(5, 4242), "bytes over budget and a key past the cap: still the first page");
    assertArrayEquals(new long[] {0L}, boundaries.starts(), "no boundary: a segment must hold at least one page");
    assertEquals(5L, boundaries.highestAdoptedPageKey());
    // The same holds for the first page of every LATER segment.
    assertEquals(1, boundaries.adopt(6, 4242), "the second page closes segment 0 and is the first of segment 1");
    assertArrayEquals(new long[] {0L, 6L}, boundaries.starts());
    assertEquals(2, boundaries.adopt(7, 4242), "and the next page closes segment 1 in turn");
    assertArrayEquals(new long[] {0L, 6L, 7L}, boundaries.starts());
  }

  @Test
  @DisplayName("a segment closes by minted bytes at the next adoption above every adopted page")
  void closesByBytes() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(100, NO_LEAF_CAP);
    assertEquals(0, boundaries.adopt(0, 0));
    assertEquals(0, boundaries.adopt(1, 99), "one byte short of the budget");
    assertEquals(1, boundaries.adopt(2, 100), "the budget is reached: this page opens segment 1");
    assertArrayEquals(new long[] {0L, 2L}, boundaries.starts());
    assertEquals(1, boundaries.adopt(3, 0), "the bytes passed are the OPEN segment's, which has none yet");
    assertEquals(2, boundaries.adopt(4, 100));
    assertArrayEquals(new long[] {0L, 2L, 4L}, boundaries.starts());
    assertEquals(3, boundaries.segmentCount());
    assertEquals(2, boundaries.openSegment());
    assertEquals(0, boundaries.segmentOf(0));
    assertEquals(0, boundaries.segmentOf(1));
    assertEquals(1, boundaries.segmentOf(2));
    assertEquals(1, boundaries.segmentOf(3));
    assertEquals(2, boundaries.segmentOf(4));
    assertEquals(2, boundaries.segmentOf(1000), "a key above every boundary is the open segment's");
  }

  @Test
  @DisplayName("a segment closes by leaves counted from ITS first page, not from page 0")
  void closesByLeavesFromTheSegmentsFirstPage() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, 4);
    assertEquals(0, boundaries.adopt(10, 0), "the load's first page need not be page 0");
    assertEquals(0, boundaries.adopt(11, 0));
    assertEquals(0, boundaries.adopt(13, 0), "13 - 10 < 4");
    assertEquals(1, boundaries.adopt(14, 0), "14 - 10 == 4: the cap is reached");
    assertArrayEquals(new long[] {0L, 14L}, boundaries.starts());
    assertEquals(1, boundaries.adopt(17, 0), "17 - 14 < 4: the cap counts from the segment's first page");
    assertEquals(2, boundaries.adopt(18, 0));
    assertArrayEquals(new long[] {0L, 14L, 18L}, boundaries.starts());
    assertEquals(0, boundaries.segmentOf(3), "keys below the first adopted page read as segment 0");
  }

  @Test
  @DisplayName("a page at or below the highest adopted key is placed by the boundaries and never closes a segment")
  void aPageBelowTheHighestAdoptedNeverCloses() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(1, NO_LEAF_CAP);
    assertEquals(0, boundaries.adopt(0, 0));
    assertEquals(0, boundaries.adopt(10, 0));
    assertEquals(0, boundaries.adopt(5, 4242), "below the highest adopted page: over budget, but no boundary");
    assertEquals(0, boundaries.adopt(10, 4242), "at the highest adopted page: the same");
    assertArrayEquals(new long[] {0L}, boundaries.starts(),
        "a boundary at 5 or 10 would move page 10 (or a later page 7) to the far side after its adoption");
    assertEquals(1, boundaries.adopt(11, 4242), "the first page ABOVE everything adopted takes the deferred close");
    assertArrayEquals(new long[] {0L, 11L}, boundaries.starts());
    assertEquals(0, boundaries.adopt(3, 4242), "a late page of segment 0 is still segment 0");
    assertEquals(1, boundaries.adopt(11, 4242), "re-adopting the open segment's first page changes nothing");
    assertArrayEquals(new long[] {0L, 11L}, boundaries.starts());
    assertEquals(2, boundaries.adopt(12, 4242));
    assertEquals(12L, boundaries.highestAdoptedPageKey());
  }

  @Test
  @DisplayName("PROPERTY: under any adoption order an adopted page's segment never changes, and starts ascend from 0")
  void anAdoptedPagesSegmentIsFixed() {
    final Random random = new Random(0x5E6B0AD5L);
    for (int round = 0; round < 20; round++) {
      final long budget = 1 + random.nextInt(50);
      final long maxLeaves = 1 + random.nextInt(64);
      final SegmentBoundaries boundaries = new SegmentBoundaries(budget, maxLeaves);
      final int keySpace = 4096;
      final int[] segmentOfAdopted = new int[keySpace];
      Arrays.fill(segmentOfAdopted, -1);
      long highest = -1L;
      long[] previousStarts = boundaries.starts();
      for (int i = 0; i < 3000; i++) {
        // Mostly ascending — a bulk load — with a late page below the high-water mark now and then.
        final long key = random.nextInt(8) == 0
            ? random.nextInt((int) Math.max(1L, highest + 1))
            : Math.min(keySpace - 1, highest + 1 + random.nextInt(3));
        final long bytes = random.nextInt((int) (2 * budget));
        final int segment = boundaries.adopt(key, bytes);
        assertEquals(segment, boundaries.segmentOf(key), "adopt and segmentOf agree, round " + round);
        if (segmentOfAdopted[(int) key] >= 0) {
          assertEquals(segmentOfAdopted[(int) key], segment,
              "page " + key + " changed segment after its adoption, round " + round + " step " + i);
        }
        segmentOfAdopted[(int) key] = segment;
        highest = Math.max(highest, key);
        final long[] starts = boundaries.starts();
        assertEquals(0L, starts[0]);
        for (int s = 1; s < starts.length; s++) {
          assertTrue(starts[s] > starts[s - 1], "starts ascend strictly");
        }
        assertTrue(starts.length >= previousStarts.length, "boundaries are only ever appended");
        assertArrayEquals(previousStarts, Arrays.copyOf(starts, previousStarts.length),
            "an existing boundary never moves");
        if (starts.length > previousStarts.length) {
          assertEquals(1, starts.length - previousStarts.length, "at most one segment closes per adoption");
          assertEquals(key, starts[starts.length - 1], "a boundary sits exactly at the page that opened the segment");
        }
        previousStarts = starts;
      }
      // Every adopted page still answers as it did when adopted.
      for (int key = 0; key < keySpace; key++) {
        if (segmentOfAdopted[key] >= 0) {
          assertEquals(segmentOfAdopted[key], boundaries.segmentOf(key), "page " + key + " after the load");
        }
      }
    }
  }

  @Test
  @DisplayName("segmentOf is the last segment whose start does not exceed the key, for every key")
  void segmentOfMatchesALinearScan() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, 1);
    // A leaf cap of one closes at every adoption above the last, so the boundaries are the keys
    // adopted.
    for (final long key : new long[] {0L, 2L, 4L, 8L, 16L, 17L, 40L}) {
      boundaries.adopt(key, 0);
    }
    final long[] starts = boundaries.starts();
    assertArrayEquals(new long[] {0L, 2L, 4L, 8L, 16L, 17L, 40L}, starts);
    for (long key = 0; key <= 50; key++) {
      int expected = 0;
      for (int s = 0; s < starts.length; s++) {
        if (starts[s] <= key) {
          expected = s;
        }
      }
      assertEquals(expected, boundaries.segmentOf(key), "key " + key);
    }
    for (int s = 0; s < starts.length; s++) {
      assertEquals(starts[s], boundaries.segmentStart(s));
    }
    assertThrows(IllegalArgumentException.class, () -> boundaries.segmentStart(-1));
    assertThrows(IllegalArgumentException.class, () -> boundaries.segmentStart(starts.length));
  }

  @Test
  @DisplayName("starts() is the directory's contract: the persisted node places every key exactly as the boundaries do")
  void startsAreTheDirectorysContract() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, 3);
    for (long key = 0; key < 100; key++) {
      boundaries.adopt(key, 0);
    }
    final long[] starts = boundaries.starts();
    assertEquals(34, starts.length, "100 pages at 3 per segment: 33 closed segments and the open one");
    final SegmentDictionaryDirectoryNode.SlotTable[] unsealed =
        new SegmentDictionaryDirectoryNode.SlotTable[starts.length];
    Arrays.fill(unsealed, SegmentDictionaryDirectoryNode.SlotTable.EMPTY);
    // The directory takes OWNERSHIP of the array it is given, so it gets its own copy.
    final SegmentDictionaryDirectoryNode directory = SegmentDictionaryDirectoryNode.takeOwnership(
        SegmentDictionaryDirectoryNode.DIRECTORY_KEY, boundaries.starts(), unsealed);
    assertEquals(boundaries.segmentCount(), directory.segmentCount());
    for (long key = 0; key < 200; key++) {
      assertEquals(boundaries.segmentOf(key), directory.segmentOf(key), "key " + key);
    }
    assertArrayEquals(starts, directory.segmentStarts());
    // And starts() is a copy: the caller cannot reach into the boundaries through it.
    starts[1] = 4242L;
    assertEquals(3L, boundaries.segmentStart(1));
    assertArrayEquals(boundaries.starts(), directory.segmentStarts());
  }

  @Test
  @DisplayName("the production constants close at 64 MiB of distinct bytes or 2^17 leaves, whichever comes first")
  void productionConstantsClose() {
    final SegmentBoundaries byBytes = new SegmentBoundaries();
    assertEquals(0, byBytes.adopt(0, 0));
    assertEquals(0, byBytes.adopt(1, SegmentBoundaries.SEGMENT_DICTIONARY_BUDGET_BYTES - 1));
    assertEquals(1, byBytes.adopt(2, SegmentBoundaries.SEGMENT_DICTIONARY_BUDGET_BYTES));
    assertEquals(64L << 20, SegmentBoundaries.SEGMENT_DICTIONARY_BUDGET_BYTES);

    final SegmentBoundaries byLeaves = new SegmentBoundaries();
    assertEquals(0, byLeaves.adopt(0, 0));
    assertEquals(0, byLeaves.adopt(SegmentBoundaries.SEGMENT_MAX_LEAVES - 1, 0));
    assertEquals(1, byLeaves.adopt(SegmentBoundaries.SEGMENT_MAX_LEAVES, 0));
    assertEquals(1L << 17, SegmentBoundaries.SEGMENT_MAX_LEAVES);
  }

  @Test
  @DisplayName("negative keys, negative byte counts and non-positive limits are refused")
  void negativesAreRefused() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, NO_LEAF_CAP);
    assertThrows(IllegalArgumentException.class, () -> boundaries.adopt(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> boundaries.adopt(0, -1));
    assertThrows(IllegalArgumentException.class, () -> boundaries.segmentOf(-1));
    assertThrows(IllegalArgumentException.class, () -> new SegmentBoundaries(0, 1));
    assertThrows(IllegalArgumentException.class, () -> new SegmentBoundaries(1, 0));
    assertThrows(IllegalArgumentException.class, () -> new SegmentBoundaries(-1, -1));
    assertEquals(-1L, boundaries.highestAdoptedPageKey(), "a refused adoption adopts nothing");
    assertArrayEquals(new long[] {0L}, boundaries.starts());
  }

  @Test
  @DisplayName("the cap bounds the KEY SPAN: a segment over sparse keys holds fewer leaves, never more")
  void theCapBoundsTheKeySpan() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, 10);
    // Keys 0, 4, 8, 12: four leaves, but the fourth is 12 keys past the segment's first, so the cap
    // closes there. A cap on the COUNT of adopted leaves would have kept all four in segment 0.
    assertEquals(0, boundaries.adopt(0, 0));
    assertEquals(0, boundaries.adopt(4, 0));
    assertEquals(0, boundaries.adopt(8, 0));
    assertEquals(1, boundaries.adopt(12, 0), "the span, not the leaf count, is what the cap bounds");
    assertArrayEquals(new long[] {0L, 12L}, boundaries.starts());
    // And the span is measured from the SEGMENT's first page, so the new segment gets its own 10.
    assertEquals(1, boundaries.adopt(21, 0));
    assertEquals(2, boundaries.adopt(22, 0));
  }

  @Test
  @DisplayName("the segment cap is refused loudly at the boundary that would exceed it, not at commit")
  void theSegmentCapIsRefusedAtClose() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, 1, 3);
    assertEquals(0, boundaries.adopt(0, 0));
    assertEquals(1, boundaries.adopt(1, 0));
    assertEquals(2, boundaries.adopt(2, 0));
    final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> boundaries.adopt(3, 0));
    assertTrue(failure.getMessage().contains("at most 3 segments"), failure.getMessage());
    assertEquals(3, boundaries.segmentCount(), "the refused close opened no segment");
    assertArrayEquals(new long[] {0L, 1L, 2L}, boundaries.starts());
    assertEquals(2, boundaries.segmentOf(3), "and the page it refused still reads as the open segment");
    assertThrows(IllegalArgumentException.class, () -> new SegmentBoundaries(1, 1, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new SegmentBoundaries(1, 1, SegmentDictionaryDirectoryNode.MAX_SEGMENTS + 1),
        "the cap can be lowered for a test, never raised past what the directory can persist");
  }

  @Test
  @DisplayName("a page adopted out of order lands in the segment its key belongs to, and closes nothing")
  void anOutOfOrderPageIsPlacedNotAppended() {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, 4);
    for (long key = 0; key < 12; key++) {
      boundaries.adopt(key, 0);
    }
    assertArrayEquals(new long[] {0L, 4L, 8L}, boundaries.starts());
    assertEquals(11L, boundaries.highestAdoptedPageKey());

    // A page from a segment the writer has long left: a re-adopted copy-on-write copy, or a
    // modification of an already written page. It must land where its KEY says.
    assertEquals(0, boundaries.adopt(1, 0));
    assertEquals(1, boundaries.adopt(5, 0));
    assertEquals(2, boundaries.adopt(11, 0));
    assertArrayEquals(new long[] {0L, 4L, 8L}, boundaries.starts(), "and closes nothing while doing it");
    assertEquals(11L, boundaries.highestAdoptedPageKey(), "the adoption cursor never goes backwards");

    // The deferred close then happens at the next page ABOVE everything adopted.
    assertEquals(3, boundaries.adopt(12, 0));
    assertArrayEquals(new long[] {0L, 4L, 8L, 12L}, boundaries.starts());
  }

  @Test
  @DisplayName("a segment a reader has already been told about never moves under it while the writer adopts")
  void anAdoptedPagesSegmentIsStableAcrossThreads() throws Exception {
    final SegmentBoundaries boundaries = new SegmentBoundaries(NO_BUDGET, 8);
    final int pages = 20_000;
    /** The segment each page was adopted into, {@code -1} until the writer has adopted it. */
    final AtomicLongArray adoptedInto = new AtomicLongArray(pages);
    for (int page = 0; page < pages; page++) {
      adoptedInto.set(page, -1L);
    }
    final AtomicBoolean adopting = new AtomicBoolean(true);
    final AtomicReference<AssertionError> failure = new AtomicReference<>();
    // The flush pool asks for a page's segment long after the writer adopted it. Whatever the writer
    // is doing at that moment, the answer must be the one the page was adopted with.
    final Thread reader = new Thread(() -> {
      while (adopting.get() && failure.get() == null) {
        sweep(boundaries, adoptedInto, failure);
      }
      sweep(boundaries, adoptedInto, failure); // once more after the writer stopped: the last pages
    }, "segment-boundaries-reader");
    reader.start();
    try {
      for (int page = 0; page < pages; page++) {
        adoptedInto.set(page, boundaries.adopt(page, 0));
      }
    } finally {
      adopting.set(false);
      reader.join(60_000);
    }
    assertTrue(!reader.isAlive(), "the reader thread must finish");
    final AssertionError observed = failure.get();
    if (observed != null) {
      throw observed;
    }
    assertEquals(pages / 8, boundaries.segmentCount(), "and the writer really did close segments meanwhile");
  }

  /** Check every page adopted so far still reads as the segment it was adopted into. */
  private static void sweep(final SegmentBoundaries boundaries, final AtomicLongArray adoptedInto,
      final AtomicReference<AssertionError> failure) {
    for (int page = 0; page < adoptedInto.length(); page++) {
      final long expected = adoptedInto.get(page);
      if (expected < 0) {
        continue;
      }
      final int actual = boundaries.segmentOf(page);
      if (actual != expected) {
        failure.compareAndSet(null, new AssertionError("page " + page + " was adopted into segment " + expected
            + " and now reads as " + actual + "; a boundary moved under a page already handed out"));
        return;
      }
    }
  }
}
