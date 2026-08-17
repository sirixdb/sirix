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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Invariant I6: chunk expansion is linearizable, and it is mutually atomic with closing the page.
 *
 * <p>
 * <b>What a race here would actually break.</b> Not the bytes — the expansion is deterministic, so
 * two threads expanding the same chunk would write the same values. What breaks is everything
 * around that: a reader observing the materialized bit before the record behind it, a chunk
 * rewritten under a reader already reading it, a page released out from under a decode. None of
 * those show up as a wrong answer on x86, where the hardware hides the ordering bugs the Java
 * memory model permits. So this test asserts on things that DO show up: the exact number of
 * expansions, and whether a close can slip between a guard and the read it protects.
 *
 * <p>
 * Every run is poisoned, so a reader that somehow reaches an unexpanded range reads {@code 0xCC}
 * and fails its comparison instead of returning stale bytes that happen to look like a record.
 */
@DisplayName("Chunked body lazy expansion under concurrency")
final class ChunkedBodyLazyConcurrencyTest {

  /** Readers racing one page. Above the core count on purpose, so threads queue on the monitor. */
  private static final int READERS = 8;

  /** Times the whole shape list is raced. Enough to shake out an ordering bug that needs a window. */
  private static final int ITERATIONS = 12;

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

  /**
   * The shapes with the most to go wrong: many chunks, every elision lever the writer will take, the
   * bitmap holes that decouple slot order from entry order, DeweyID trailers between records, and the
   * degenerate body whose expansion is a raw copy rather than a record walk.
   */
  private static List<Recipe> meatyShapes() {
    return List.of(
        new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.MIXED_NULL, PathKeys.FEW, Values.MIXED_STRUCTURAL,
            Names.WIDE, Shape.DENSE, Sizes.MIXED, 512, false),
        new Recipe(Body.TEMPLATED, Hash.ALL_ZERO, ParentKeys.SEQUENTIAL, PathKeys.DISTINCT, Values.MIXED_WITH_NULLS,
            Names.MANY, Shape.ALTERNATING_HOLES, Sizes.SMALL, 511, false),
        new Recipe(Body.TEMPLATED, Hash.FIRST_ONLY, ParentKeys.ALL_NULL, PathKeys.SINGLE, Values.STRINGS, Names.WIDE,
            Shape.SEEDED_RANDOM, Sizes.ONE_OVERSIZED, 128, false),
        new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.MIXED_NULL, PathKeys.FEW, Values.MIXED, Names.FEW,
            Shape.DENSE, Sizes.MIXED, 200, true),
        new Recipe(Body.DEGENERATE, Hash.NONE_ZERO, ParentKeys.SEQUENTIAL, PathKeys.SINGLE, Values.NUMBERS, Names.ONE,
            Shape.ALTERNATING_HOLES, Sizes.MIXED, 400, false));
  }

  @Test
  @DisplayName("eight readers racing one page expand each chunk exactly once and all read the same bytes")
  void readersRaceOneSharedPage() throws InterruptedException {
    final ResourceConfiguration plain = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    final ResourceConfiguration dewey =
        new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).useDeweyIDs(true).build();
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);

    final List<Recipe> shapes = meatyShapes();
    long racedPages = 0;
    long racedChunks = 0;
    long racedReads = 0;
    final long startedAt = System.nanoTime();
    for (int iteration = 0; iteration < ITERATIONS; iteration++) {
      for (int shapeIdx = 0; shapeIdx < shapes.size(); shapeIdx++) {
        final Recipe recipe = shapes.get(shapeIdx);
        final ResourceConfiguration config = recipe.deweyIds()
            ? dewey
            : plain;
        final MemorySegment wire = ChunkedPageHarness.serializeChunked(config, recipe);
        final KeyValueLeafPage eager = ChunkedPageHarness.deserialize(config, wire);
        final KeyValueLeafPage lazy = ChunkedPageHarness.deserializeLazily(config, wire);
        try {
          final int chunkCount = lazy.chunkCount();
          assertTrue(chunkCount > 2, recipe + ": only " + chunkCount + " chunks, nothing to race for");
          final int[] slots = ChunkedPageHarness.populatedSlots(eager);
          ChunkedBodyConfig.resetDiag();

          // Every reader walks every slot, so the schedules overlap completely; the seeded shuffle
          // is what makes them collide on different chunks at different moments instead of
          // marching in step.
          final long seed = (long) iteration * 1000 + shapeIdx;
          raceReaders(eager, lazy, slots, seed, recipe.toString());

          assertEquals(chunkCount, ChunkedBodyConfig.chunkMaterializations(),
              recipe + ": each chunk must be expanded exactly once, whatever the interleaving");
          assertTrue(lazy.isFullyMaterialized(), recipe + ": chunks outstanding after every slot was read");
          assertEquals(0, lazy.pendingChunkBytes(), recipe + ": encoded bytes still held");
          ChunkedPageHarness.assertSameSlottedPage(eager, lazy, recipe + " after " + READERS + " readers raced");
          racedPages++;
          racedChunks += chunkCount;
          racedReads += (long) READERS * slots.length;
        } finally {
          eager.close();
          lazy.close();
        }
      }
    }
    final long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;
    System.out.println("[chunked-i6] " + racedPages + " shared pages raced by " + READERS + " readers each, "
        + racedChunks + " chunks expanded, " + racedReads + " slot reads, in " + elapsedMillis + " ms (poison ON)");
  }

  /**
   * A page cannot be closed while a reader holds a guard on it, and once it is closed the expansion
   * gate is inert rather than dangerous.
   *
   * <p>
   * This is the eviction interaction: the sweeper closes pages under readers all the time, and the
   * only thing standing between it and a released segment being decoded into is the guard count.
   * Expansion holds the page monitor, close is synchronized on the same monitor, and close refuses
   * outright while a guard is outstanding — so the failure this test would catch is an expansion
   * that runs after the segment went away.
   */
  @Test
  @DisplayName("a guarded reader keeps a page alive through a concurrent close, and a closed page's gate is inert")
  void closeCannotRunUnderAGuardedReader() throws InterruptedException {
    final ResourceConfiguration config = new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE).build();
    ChunkedBodyConfig.setTargetChunkBytesForTesting(64);
    final Recipe recipe = new Recipe(Body.TEMPLATED, Hash.ALTERNATING, ParentKeys.MIXED_NULL, PathKeys.FEW,
        Values.MIXED_STRUCTURAL, Names.WIDE, Shape.DENSE, Sizes.MIXED, 512, false);

    final MemorySegment wire = ChunkedPageHarness.serializeChunked(config, recipe);
    final KeyValueLeafPage eager = ChunkedPageHarness.deserialize(config, wire);
    final KeyValueLeafPage lazy = ChunkedPageHarness.deserializeLazily(config, wire);
    try {
      final int[] slots = ChunkedPageHarness.populatedSlots(eager);
      assertTrue(lazy.tryAcquireGuard(), "a fresh page must be guardable");

      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicBoolean readerDone = new AtomicBoolean();
      final CountDownLatch started = new CountDownLatch(2);

      final Thread reader = new Thread(() -> {
        started.countDown();
        try {
          started.await();
          final int[] order = slots.clone();
          shuffle(order, new Random(4242));
          for (final int slot : order) {
            // Every read here happens while the closer is hammering close(). A close that slipped
            // through would either null the segment (getSlot returns null) or free it under the
            // expansion — both surface as a mismatch, not as silence.
            assertArrayEquals(eager.getSlotAsByteArray(slot), lazy.getSlotAsByteArray(slot),
                "slot " + slot + " read while a close was racing it");
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        } finally {
          readerDone.set(true);
        }
      }, "guarded-reader");

      final Thread closer = new Thread(() -> {
        started.countDown();
        try {
          started.await();
          while (!readerDone.get()) {
            // Refused every time: the guard the reader holds is what refuses it.
            lazy.close();
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      }, "closer");

      reader.start();
      closer.start();
      reader.join(TimeUnit.MINUTES.toMillis(2));
      closer.join(TimeUnit.MINUTES.toMillis(2));
      if (failure.get() != null) {
        throw new AssertionError("a guarded read did not survive a concurrent close", failure.get());
      }

      assertFalse(lazy.isClosed(), "the page was closed while a reader held a guard on it");
      assertTrue(lazy.isFullyMaterialized(), "the reader read every slot without expanding every chunk");
      ChunkedPageHarness.assertSameSlottedPage(eager, lazy, "after a close raced a guarded reader");

      // Guard released: now the close takes, and the gate must go quiet rather than decode into a
      // segment that no longer exists.
      lazy.releaseGuard();
      lazy.close();
      assertTrue(lazy.isClosed(), "the page did not close once its last guard was released");
      assertEquals(0, lazy.pendingChunkBytes(), "a closed page still holds encoded chunk bytes");
      assertNull(lazy.getSlottedPage(), "a closed page still exposes its heap");
      for (final int slot : slots) {
        // Inert, not fatal: nothing left to expand, and nothing to expand it into.
        lazy.ensureChunkFor(slot);
        assertNull(lazy.getSlot(slot), "a closed page returned slot " + slot);
      }
      lazy.ensureAllChunks();
    } finally {
      eager.close();
      lazy.close();
    }
  }

  private static void raceReaders(final KeyValueLeafPage eager, final KeyValueLeafPage lazy, final int[] slots,
      final long seed, final String what) throws InterruptedException {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch startLine = new CountDownLatch(READERS);
    final Thread[] threads = new Thread[READERS];
    for (int t = 0; t < READERS; t++) {
      final int reader = t;
      threads[t] = new Thread(() -> {
        final int[] order = slots.clone();
        shuffle(order, new Random(seed * 31 + reader));
        startLine.countDown();
        try {
          // All eight go at once: without the latch the first thread finishes the page before the
          // last one starts and nothing ever contends.
          startLine.await();
          for (final int slot : order) {
            assertArrayEquals(eager.getSlotAsByteArray(slot), lazy.getSlotAsByteArray(slot),
                what + ": reader " + reader + " slot " + slot);
          }
        } catch (final Throwable t2) {
          failure.compareAndSet(null, t2);
        }
      }, "i6-reader-" + t);
    }
    for (final Thread thread : threads) {
      thread.start();
    }
    for (final Thread thread : threads) {
      thread.join(TimeUnit.MINUTES.toMillis(2));
    }
    if (failure.get() != null) {
      throw new AssertionError(what + ": a racing reader disagreed with the eagerly decoded page", failure.get());
    }
    final List<Thread> stillRunning = new ArrayList<>();
    for (final Thread thread : threads) {
      if (thread.isAlive()) {
        stillRunning.add(thread);
      }
    }
    assertTrue(stillRunning.isEmpty(), what + ": readers never finished — " + stillRunning);
  }

  /** Fisher-Yates over a seeded source, so a failing interleaving starts from a replayable order. */
  private static void shuffle(final int[] values, final Random random) {
    for (int i = values.length - 1; i > 0; i--) {
      final int j = random.nextInt(i + 1);
      final int swap = values[i];
      values[i] = values[j];
      values[j] = swap;
    }
  }
}
