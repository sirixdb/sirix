/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.chunked.ChunkedPageGenerator.Body;
import io.sirix.page.chunked.ChunkedPageGenerator.Hash;
import io.sirix.page.chunked.ChunkedPageGenerator.Names;
import io.sirix.page.chunked.ChunkedPageGenerator.ParentKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.PathKeys;
import io.sirix.page.chunked.ChunkedPageGenerator.Recipe;
import io.sirix.page.chunked.ChunkedPageGenerator.Shape;
import io.sirix.page.chunked.ChunkedPageGenerator.Sizes;
import io.sirix.page.chunked.ChunkedPageGenerator.Values;
import io.sirix.page.chunked.ChunkedPageHarness.ChunkedLayout;
import io.sirix.page.chunked.ChunkedSweepCases.Case;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Invariant I1 for the lazy read path: a page whose records are expanded one chunk at a time, in
 * whatever order a reader happens to ask for them, is the same page as one decoded whole.
 *
 * <p>
 * <b>Why the order is random and why the seed is fixed.</b> Expansion is only safe if no record's
 * bytes depend on any other record having been expanded first — the property the plan calls
 * expansion locality. Reading slots in ascending order would exercise exactly the order the eager
 * path uses and prove nothing about locality, so each page is read in a shuffled order; the shuffle
 * is seeded from the page's index so a failure is reproducible rather than a Heisenbug.
 *
 * <p>
 * <b>Why the sweep runs poisoned.</b> The set of readers that must consult the expansion gate
 * cannot be closed by enumeration — it is whatever reaches the heap today plus whatever reaches it
 * next year. So the unexpanded heap is filled with {@code 0xCC} and the comparison against the
 * eagerly decoded twin does the enforcing: a reader that bypasses the gate returns poison and
 * fails, here, deterministically, rather than returning plausible stale bytes somewhere downstream.
 *
 * <p>
 * <b>What is compared.</b> Per slot, the record bytes, which is what a point reader sees. Then,
 * after expanding everything, the FULL slotted segment — header, bitmap, directory and heap
 * including the DeweyID trailers that live between records and belong to no slot's bytes. A
 * slot-by-slot comparison alone would pass a page whose trailers had shifted.
 */
@DisplayName("Chunked body lazy expansion")
final class ChunkedBodyLazyExpansionTest {

  private boolean previouslyEnabled;
  private int previousTarget;
  private boolean previousPoison;
  private boolean previousDiag;

  @BeforeEach
  void setUp() {
    Allocators.getInstance().init(2L * 1024 * 1024 * 1024);
    previouslyEnabled = ChunkedBodyConfig.setEnabledForTesting(false);
    previousTarget = ChunkedBodyConfig.targetChunkBytes();
    previousPoison = ChunkedBodyConfig.setPoisonForTesting(true);
    previousDiag = ChunkedBodyConfig.setDiagForTesting(true);
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    ChunkedBodyConfig.setTargetChunkBytesForTesting(previousTarget);
    ChunkedBodyConfig.setPoisonForTesting(previousPoison);
    ChunkedBodyConfig.setDiagForTesting(previousDiag);
  }

  @Test
  @DisplayName("every generated page reads the same expanded chunk by chunk, in random order, as decoded whole")
  void lazySweep() {
    final ResourceConfiguration plain = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    final ResourceConfiguration dewey =
        new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).useDeweyIDs(true).build();

    final List<Case> cases = ChunkedSweepCases.all();
    ChunkedBodyConfig.resetDiag();
    final TreeMap<Integer, Integer> chunkCounts = new TreeMap<>();
    long slotsRead = 0;
    int lazyPages = 0;
    final long startedAt = System.nanoTime();
    for (int i = 0; i < cases.size(); i++) {
      final Case testCase = cases.get(i);
      ChunkedBodyConfig.setTargetChunkBytesForTesting(testCase.targetChunkBytes());
      final ResourceConfiguration config = testCase.dewey()
          ? dewey
          : plain;
      final MemorySegment wire = ChunkedPageHarness.serializeChunked(config, testCase.recipe());
      final KeyValueLeafPage eager = ChunkedPageHarness.deserialize(config, wire);
      final KeyValueLeafPage lazy = ChunkedPageHarness.deserializeLazily(config, wire);
      try {
        final String what = testCase + " [lazy]";
        final int chunkCount = lazy.chunkCount();
        chunkCounts.merge(chunkCount, 1, Integer::sum);
        if (chunkCount > 0) {
          lazyPages++;
          assertFalse(lazy.isFullyMaterialized(), what + ": nothing was left to expand after a lazy load");
          assertTrue(lazy.pendingChunkBytes() > 0, what + ": a lazy page holds no encoded chunk bytes");
        }

        final int[] slots = ChunkedPageHarness.populatedSlots(eager);
        // A slot's kind and length come out of the directory, which the META section alone builds.
        // Answering them must not expand anything — that is what makes a kind-count scan free.
        for (final int slot : slots) {
          assertEquals(eager.getSlotNodeKindId(slot), lazy.getSlotNodeKindId(slot),
              what + ": slot " + slot + " kind id before any chunk was expanded");
        }
        if (chunkCount > 0) {
          assertFalse(lazy.isFullyMaterialized(), what + ": reading the directory expanded the page's records");
        }

        shuffle(slots, new Random(i));
        for (final int slot : slots) {
          assertArrayEquals(eager.getSlotAsByteArray(slot), lazy.getSlotAsByteArray(slot),
              what + ": slot " + slot + " read in random chunk order");
          // A second door onto the same bytes, decoding a field rather than slicing the record: a
          // gate installed on getSlot alone would return the poison byte as a node kind here.
          assertEquals(eager.getSlotParentKey(slot), lazy.getSlotParentKey(slot),
              what + ": slot " + slot + " parent key read in random chunk order");
        }
        slotsRead += slots.length;

        lazy.ensureAllChunks();
        assertTrue(lazy.isFullyMaterialized(), what + ": chunks outstanding after expanding all of them");
        assertEquals(0, lazy.pendingChunkBytes(), what + ": encoded bytes still held after full expansion");
        ChunkedPageHarness.assertSameSlottedPage(eager, lazy, what);
      } finally {
        eager.close();
        lazy.close();
      }
    }
    final long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    System.out.println("[chunked-lazy] " + cases.size() + " pages, " + lazyPages + " with chunks to expand, "
        + slotsRead + " slots read in randomised chunk order, in " + elapsedMillis + " ms (poison-fill ON)");
    System.out.println("[chunked-lazy] chunks per page: " + chunkCounts);
    System.out.println("[chunked-lazy] diag: lazyLoads=" + ChunkedBodyConfig.lazyLoads() + " chunkMaterializations="
        + ChunkedBodyConfig.chunkMaterializations());

    // A sweep that never left anything unexpanded would pass without exercising a single gate.
    assertTrue(chunkCounts.lastKey() > 1, "no page in the sweep was framed into more than one chunk");
    assertEquals(cases.size(), ChunkedBodyConfig.lazyLoads(), "not every page took the lazy load path");
    assertTrue(ChunkedBodyConfig.chunkMaterializations() > 0, "no chunk was ever expanded");
  }

  /**
   * Expanding one chunk expands one chunk.
   *
   * <p>
   * The sweep above proves the bytes come out right; this proves the laziness is real. Without it a
   * reader that quietly expanded everything on first touch would pass every equality assertion in
   * this file while delivering none of the point-read saving the format exists for.
   */
  @Test
  @DisplayName("touching one slot expands its chunk and leaves the rest encoded and poisoned")
  void oneSlotExpandsOneChunk() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    // A small target over 200 records: enough chunks that "expanded one" and "expanded all" cannot
    // be confused for each other.
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);
    final Recipe recipe = new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.SEQUENTIAL, PathKeys.FEW,
        Values.MIXED, Names.WIDE, Shape.ALTERNATING_HOLES, Sizes.MIXED, 200, false);

    final MemorySegment wire = ChunkedPageHarness.serializeChunked(config, recipe);
    final ChunkedLayout layout = ChunkedPageHarness.parseChunkedLayout(wire);
    assertTrue(layout.chunkCount >= 3, "the recipe framed only " + layout.chunkCount + " chunks");

    final KeyValueLeafPage eager = ChunkedPageHarness.deserialize(config, wire);
    final KeyValueLeafPage lazy = ChunkedPageHarness.deserializeLazily(config, wire);
    try {
      final int[][] ranges = ChunkedPageHarness.chunkHeapRanges(eager, layout);
      for (int c = 0; c < layout.chunkCount; c++) {
        assertTrue(ChunkedPageHarness.isAllPoison(lazy, ranges[c]),
            "chunk " + c + " holds something other than poison before anything was expanded");
      }

      final int encodedBytes = lazy.pendingChunkBytes();
      int expectedEncoded = 0;
      for (int c = 0; c < layout.chunkCount; c++) {
        expectedEncoded += layout.chunkEncLen[c];
      }
      assertEquals(expectedEncoded, encodedBytes, "the page does not hold exactly the chunk table's encoded bytes");
      final long retainedRegionBytes = lazy.getRegionTable() == null
          ? 0L
          : lazy.getRegionTable().retainedFootprintBytes();
      assertEquals(Math.addExact(lazy.getSlottedPage().byteSize() + expectedEncoded, retainedRegionBytes),
          lazy.getActualMemorySize(),
          "cache weight must include the slotted frame, encoded chunks, and retained native regions");

      // Read one slot out of the middle chunk, through the accessor — the only door that gates.
      final int target = 1;
      final int[] slots = ChunkedPageHarness.populatedSlots(eager);
      final int slotInTarget = slots[layout.chunkFirstEntry[target]];
      assertArrayEquals(eager.getSlotAsByteArray(slotInTarget), lazy.getSlotAsByteArray(slotInTarget),
          "the first slot read out of a lazy page");

      assertFalse(lazy.isFullyMaterialized(), "one slot read expanded the whole page");
      assertEquals(encodedBytes - layout.chunkEncLen[target], lazy.pendingChunkBytes(),
          "expanding one chunk did not free exactly that chunk's encoded bytes");
      assertFalse(ChunkedPageHarness.isAllPoison(lazy, ranges[target]), "the expanded chunk is still poison");
      for (int c = 0; c < layout.chunkCount; c++) {
        if (c != target) {
          assertTrue(ChunkedPageHarness.isAllPoison(lazy, ranges[c]),
              "chunk " + c + " was expanded although nothing in it was read");
        }
      }

      lazy.ensureAllChunks();
      ChunkedPageHarness.assertSameSlottedPage(eager, lazy, "after expanding the remaining chunks");
    } finally {
      eager.close();
      lazy.close();
    }
  }

  /**
   * Many readers racing for the same page expand each chunk exactly once.
   *
   * <p>
   * The count is the assertion that matters. Whether the bytes come out right is already covered;
   * what a race breaks is the double-checked locking around the expansion, and a chunk expanded twice
   * would rewrite records another thread is already reading. Counting is how that shows up
   * deterministically — a torn read on x86 would not.
   */
  @Test
  @DisplayName("readers racing for one page expand each chunk exactly once")
  void concurrentReadersExpandEachChunkOnce() throws InterruptedException {
    final ResourceConfiguration config = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);
    final Recipe recipe = new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.MIXED_NULL, PathKeys.FEW,
        Values.MIXED_STRUCTURAL, Names.WIDE, Shape.DENSE, Sizes.MIXED, 512, false);

    final MemorySegment wire = ChunkedPageHarness.serializeChunked(config, recipe);
    final KeyValueLeafPage eager = ChunkedPageHarness.deserialize(config, wire);
    final KeyValueLeafPage lazy = ChunkedPageHarness.deserializeLazily(config, wire);
    try {
      final int chunkCount = lazy.chunkCount();
      assertTrue(chunkCount > 4, "the recipe framed only " + chunkCount + " chunks to race for");
      final int[] slots = ChunkedPageHarness.populatedSlots(eager);
      ChunkedBodyConfig.resetDiag();

      final int readers = 8;
      final Thread[] threads = new Thread[readers];
      final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
      for (int t = 0; t < readers; t++) {
        final int reader = t;
        threads[t] = new Thread(() -> {
          final int[] order = slots.clone();
          // A different order per reader, so the threads collide on chunks rather than queueing
          // behind one another in slot order.
          shuffle(order, new Random(reader));
          try {
            for (final int slot : order) {
              assertArrayEquals(eager.getSlotAsByteArray(slot), lazy.getSlotAsByteArray(slot),
                  "reader " + reader + " read slot " + slot);
            }
          } catch (final Throwable failure) {
            firstFailure.compareAndSet(null, failure);
          }
        }, "lazy-reader-" + t);
      }
      for (final Thread thread : threads) {
        thread.start();
      }
      for (final Thread thread : threads) {
        thread.join();
      }
      if (firstFailure.get() != null) {
        throw new AssertionError("a racing reader disagreed with the eagerly decoded page", firstFailure.get());
      }

      assertEquals(chunkCount, ChunkedBodyConfig.chunkMaterializations(),
          "chunks were expanded more (or fewer) times than the page has");
      assertTrue(lazy.isFullyMaterialized(), "chunks outstanding after every slot was read");
      ChunkedPageHarness.assertSameSlottedPage(eager, lazy, "after 8 readers raced through it");
    } finally {
      eager.close();
      lazy.close();
    }
  }

  /** Fisher-Yates over a seeded source, so a failing order can be replayed. */
  private static void shuffle(final int[] values, final Random random) {
    for (int i = values.length - 1; i > 0; i--) {
      final int j = random.nextInt(i + 1);
      final int swap = values[i];
      values[i] = values[j];
      values[j] = swap;
    }
  }
}
