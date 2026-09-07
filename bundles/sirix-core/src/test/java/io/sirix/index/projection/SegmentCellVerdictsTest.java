/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The verdict memo: a whole segment settled by one sweep where the dictionary allows it, and one
 * evaluation per distinct {@code (segment, id)} where it does not — whatever the number of rows or
 * workers that ask.
 */
final class SegmentCellVerdictsTest {

  /** Counts evaluations, so the test can assert the memo actually memoises. */
  private static final class CountingView implements SegmentCellVerdicts.CellMatcher {
    private final AtomicInteger evaluations = new AtomicInteger();

    @Override
    public Boolean matches(final long cell) {
      evaluations.incrementAndGet();
      final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
      final int id = ProjectionIndexRowGroupPage.idOfCell(cell);
      if (id == 0) {
        return null; // names no entry
      }
      // "Matches" iff the id is even — arbitrary but deterministic, and different per segment so a
      // verdict read from the wrong segment's table would be visible.
      return (id + segment) % 2 == 0;
    }
  }

  @Test
  @DisplayName("a distinct cell is evaluated once, however many rows carry it")
  void aCellIsEvaluatedOnce() {
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts =
        new SegmentCellVerdicts(view, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2);
    final long cell = ProjectionIndexRowGroupPage.packSegmentCell(1, 4);
    for (int row = 0; row < 1000; row++) {
      assertFalse(verdicts.matches(cell), "(1 + 4) is odd, so this cell rejects");
    }
    assertEquals(1, view.evaluations.get(), "1000 rows, one evaluation");
  }

  @Test
  @DisplayName("the same id in two segments is two verdicts, not one")
  void segmentsDoNotShareVerdicts() {
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts =
        new SegmentCellVerdicts(view, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2);
    assertTrue(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 4)), "(0 + 4) is even");
    assertFalse(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(1, 4)), "(1 + 4) is odd");
    assertEquals(2, view.evaluations.get(), "the same id in two segments is two distinct values");
  }

  @Test
  @DisplayName("a cell naming no entry never matches")
  void anUnresolvableCellRejects() {
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts =
        new SegmentCellVerdicts(view, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2);
    assertFalse(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 0)));
  }

  /**
   * Runs {@code workers} threads over every cell of every segment, returning the disagreement count.
   */
  private static int hammer(final SegmentCellVerdicts verdicts, final int segments, final int ids, final int workers)
      throws Exception {
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(workers);
    final AtomicInteger disagreements = new AtomicInteger();
    for (int w = 0; w < workers; w++) {
      Thread.ofPlatform().start(() -> {
        try {
          start.await();
          for (int segment = 0; segment < segments; segment++) {
            for (int id = 1; id <= ids; id++) {
              final boolean expected = (id + segment) % 2 == 0;
              if (verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(segment, id)) != expected) {
                disagreements.incrementAndGet();
              }
            }
          }
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertTrue(done.await(60, TimeUnit.SECONDS), "workers must finish");
    return disagreements.get();
  }

  @Test
  @DisplayName("the memo converges: a race may redo a cell, but a settled cell is never read again")
  void concurrentReadersAgree() throws Exception {
    final int segments = 4;
    final int ids = 2_000; // past the initial table, so the growth path runs under contention
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts =
        new SegmentCellVerdicts(view, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, segments);
    final int workers = 16;
    final int distinct = segments * ids;

    assertEquals(0, hammer(verdicts, segments, ids, workers), "a cell's verdict must not depend on who read it");
    final int firstPass = view.evaluations.get();
    assertTrue(firstPass >= distinct, "every distinct cell is evaluated at least once: " + firstPass);
    // The read runs OUTSIDE the monitor, so workers that reach an unsettled cell together all
    // evaluate it. That is the deliberate trade — the read fetches a page and decodes a block, and
    // serialising it behind one lock is what made a parallel scan run on one core. The redundancy it
    // buys is bounded by the number of racing workers, never by the number of ROWS.
    assertTrue(firstPass <= (long) distinct * workers,
        "redundancy must be bounded by the workers racing, not unbounded: " + firstPass + " for " + distinct
            + " distinct cells across " + workers + " workers");

    // Convergence is the property that makes the trade sound: once settled, a cell is never read
    // again, however many workers ask. A memo that failed to publish would show up here as a second
    // pass that costs as much as the first.
    assertEquals(0, hammer(verdicts, segments, ids, workers), "a settled memo must still agree");
    assertEquals(firstPass, view.evaluations.get(),
        "a fully settled memo must evaluate NOTHING on a second pass over the same cells");
  }

  /** {@code (id + segment) % 2 == 0} as a bitset over ids {@code 1..entries}, the sweep's answer. */
  private static long[] sweepOf(final int segment, final int entries) {
    final long[] bits = new long[(entries + 64 >>> 6) + 1];
    for (int id = 1; id <= entries; id++) {
      if ((id + segment) % 2 == 0) {
        bits[id >>> 6] |= 1L << (id & 63);
      }
    }
    return bits;
  }

  @Test
  @DisplayName("a swept segment never consults the per-cell matcher")
  void aSweptSegmentIsSettledInOnePass() {
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts =
        new SegmentCellVerdicts(view, cell -> sweepOf(ProjectionIndexRowGroupPage.segmentOfCell(cell), 200),
            ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2);
    for (int segment = 0; segment < 2; segment++) {
      for (int id = 1; id <= 200; id++) {
        assertEquals((id + segment) % 2 == 0,
            verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(segment, id)),
            "segment " + segment + " id " + id);
      }
    }
    assertEquals(0, view.evaluations.get(), "the sweep settles every id; nothing falls through to a cell read");
  }

  @Test
  @DisplayName("a refused sweep falls back per cell, and its partial table is never read as settled")
  void aRefusedSweepFallsBackPerCell() {
    final CountingView view = new CountingView();
    // Segment 0 sweeps; segment 1 refuses. The refusal must not leave segment 1's cells answering
    // from an unevaluated table entry, which reads identically to a settled REJECT.
    final SegmentCellVerdicts verdicts = new SegmentCellVerdicts(view,
        cell -> ProjectionIndexRowGroupPage.segmentOfCell(cell) == 0
            ? sweepOf(0, 200)
            : null,
        ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2);
    // Touch one cell of segment 1 first, so a table exists there before the ids under test are asked.
    assertTrue(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(1, 3)), "(1 + 3) is even");
    for (int id = 1; id <= 200; id++) {
      assertEquals((id + 1) % 2 == 0, verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(1, id)),
          "segment 1 id " + id + " must be evaluated, not read off an unsettled entry");
    }
    assertEquals(200, view.evaluations.get(), "every distinct id of the refused segment is evaluated exactly once");
    for (int id = 1; id <= 200; id++) {
      assertEquals(id % 2 == 0, verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, id)),
          "segment 0 id " + id);
    }
    assertEquals(200, view.evaluations.get(), "the swept segment added no evaluations");
  }

  @Test
  @DisplayName("a sweep that throws is a refusal, not a failure")
  void aThrowingSweepFallsBack() {
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts = new SegmentCellVerdicts(view, cell -> {
      throw new IllegalStateException("this segment sealed no dictionary for the column");
    }, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2);
    assertTrue(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 4)), "(0 + 4) is even");
    assertFalse(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 5)), "(0 + 5) is odd");
    assertEquals(2, view.evaluations.get(), "both cells took the per-cell path");
  }

  @Test
  @DisplayName("a segment is swept once, however many workers arrive together")
  void aSegmentIsSweptOnce() throws Exception {
    final AtomicInteger sweeps = new AtomicInteger();
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts = new SegmentCellVerdicts(view, cell -> {
      sweeps.incrementAndGet();
      return sweepOf(ProjectionIndexRowGroupPage.segmentOfCell(cell), 200);
    }, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 4);
    final int workers = 8;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(workers);
    final AtomicInteger disagreements = new AtomicInteger();
    for (int worker = 0; worker < workers; worker++) {
      final int segment = worker % 4;
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int id = 1; id <= 200; id++) {
            if (verdicts.matches(
                ProjectionIndexRowGroupPage.packSegmentCell(segment, id)) != ((id + segment) % 2 == 0)) {
              disagreements.incrementAndGet();
            }
          }
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }
    start.countDown();
    assertTrue(done.await(30, TimeUnit.SECONDS), "workers must finish");
    assertEquals(0, disagreements.get(), "every worker must read the verdict its own segment's sweep produced");
    assertEquals(4, sweeps.get(), "four segments, four sweeps — two workers per segment share one");
    assertEquals(0, view.evaluations.get(), "no cell read at all");
  }

  /**
   * A store over a plain map: what the buffer manager's cache does between queries, minus eviction.
   */
  private static final class MapStore implements SegmentCellVerdicts.TableStore {
    private final Map<Integer, byte[]> tables = new HashMap<>();
    private final int[] entryCounts;
    private final AtomicInteger loads = new AtomicInteger();
    private final AtomicInteger stores = new AtomicInteger();

    MapStore(final int... entryCounts) {
      this.entryCounts = entryCounts;
    }

    @Override
    public synchronized byte @Nullable [] load(final int segment) {
      loads.incrementAndGet();
      return tables.get(segment);
    }

    @Override
    public int entryCount(final int segment) {
      return segment < entryCounts.length
          ? entryCounts[segment]
          : -1;
    }

    @Override
    public synchronized void store(final int segment, final byte[] table) {
      stores.incrementAndGet();
      tables.put(segment, table);
    }

    synchronized byte @Nullable [] table(final int segment) {
      return tables.get(segment);
    }
  }

  private static SegmentCellVerdicts over(final CountingView view, final MapStore store, final int segments) {
    return new SegmentCellVerdicts(view, null, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, segments, store);
  }

  @Test
  @DisplayName("ACROSS QUERIES: a second instance over the same store inherits every settled verdict")
  void aStoreSharesSettledVerdictsAcrossInstances() {
    final MapStore store = new MapStore(200, 200);
    final CountingView first = new CountingView();
    final SegmentCellVerdicts query1 = over(first, store, 2);
    for (int segment = 0; segment < 2; segment++) {
      for (int id = 1; id <= 200; id++) {
        assertEquals((id + segment) % 2 == 0, query1.matches(ProjectionIndexRowGroupPage.packSegmentCell(segment, id)));
      }
    }
    assertEquals(400, first.evaluations.get(), "the first query settles every distinct cell once");
    assertEquals(2, store.stores.get(), "one table published per segment, on the miss");

    // The next query over the same (dictionary, op, literal) — a fresh instance, a fresh matcher —
    // finds every cell settled and reads the dictionary for NONE of them.
    final CountingView second = new CountingView();
    final SegmentCellVerdicts query2 = over(second, store, 2);
    for (int segment = 0; segment < 2; segment++) {
      for (int id = 1; id <= 200; id++) {
        assertEquals((id + segment) % 2 == 0, query2.matches(ProjectionIndexRowGroupPage.packSegmentCell(segment, id)),
            "segment " + segment + " id " + id + " must come from the shared table");
      }
    }
    assertEquals(0, second.evaluations.get(), "a settled table answers the second query without a read");
    assertEquals(2, store.stores.get(), "a table found in the store is adopted, not replaced");
    // The hot-path entry point hands out the very shared array, so a leaf's rows test it directly.
    assertSame(store.table(1), query2.tableForLeaf(ProjectionIndexRowGroupPage.packSegmentCell(1, 7)));
  }

  @Test
  @DisplayName("a shared table is allocated at the dictionary's full size, so no query ever outgrows it")
  void aSharedTableIsSizedToTheDictionary() {
    final MapStore store = new MapStore(5000, -1);
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts = over(view, store, 2);
    final byte[] shared = store.table(0);
    assertNotNull(shared, "the miss allocates the table up front");
    assertEquals(5001, shared.length, "ids are 1-based mints, so entryCount + 1 slots");
    assertNull(store.table(1), "a segment that sealed no dictionary gets no shared table");

    // The highest id the dictionary holds lands in the shared array itself: had the table been
    // allocated short, the memo would have grown a private copy and the store would keep an array
    // nothing settles into any more.
    assertTrue(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 5000)), "(0 + 5000) is even");
    assertSame(shared, verdicts.tableForLeaf(ProjectionIndexRowGroupPage.packSegmentCell(0, 1)),
        "the memo must still be the shared array after the highest id");
    assertTrue(SegmentCellVerdicts.isMatch(shared[5000]), "and its verdict is in the shared array");

    // The segment without a store-backed table still memoises, privately.
    for (int row = 0; row < 1000; row++) {
      assertTrue(verdicts.matches(ProjectionIndexRowGroupPage.packSegmentCell(1, 3)), "(1 + 3) is even");
    }
    assertEquals(2, view.evaluations.get(), "one read for id 5000 of segment 0, one for the private segment");
  }

  @Test
  @DisplayName("a sweep publishes its fully settled table over whatever partial one was shared")
  void aSweepSupersedesTheSharedTable() {
    final MapStore store = new MapStore(200, 200);
    // A previous query settled one cell of segment 0 per cell and left its partial table behind.
    final CountingView earlier = new CountingView();
    final SegmentCellVerdicts partial = over(earlier, store, 2);
    assertFalse(partial.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 3)), "(0 + 3) is odd");
    final byte[] before = store.table(0);
    assertNotNull(before);

    final CountingView view = new CountingView();
    final SegmentCellVerdicts swept =
        new SegmentCellVerdicts(view, cell -> sweepOf(ProjectionIndexRowGroupPage.segmentOfCell(cell), 200),
            ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2, store);
    // Adopted the partial table, so id 3 answers without a sweep; the first UNSETTLED id sweeps.
    assertFalse(swept.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 3)));
    assertTrue(swept.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, 4)), "(0 + 4) is even");
    final byte[] after = store.table(0);
    assertNotNull(after);
    assertTrue(after != before, "the store now holds the swept table, not the partial one");
    assertSame(after, swept.tableForLeaf(ProjectionIndexRowGroupPage.packSegmentCell(0, 1)));

    // The query after that reads every id of segment 0 off the swept table: no sweep, no cell read.
    final CountingView next = new CountingView();
    final AtomicInteger sweeps = new AtomicInteger();
    final SegmentCellVerdicts later = new SegmentCellVerdicts(next, cell -> {
      sweeps.incrementAndGet();
      return sweepOf(ProjectionIndexRowGroupPage.segmentOfCell(cell), 200);
    }, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, 2, store);
    for (int id = 1; id <= 200; id++) {
      assertEquals(id % 2 == 0, later.matches(ProjectionIndexRowGroupPage.packSegmentCell(0, id)), "id " + id);
    }
    assertEquals(0, sweeps.get(), "a fully settled table needs no sweep");
    assertEquals(0, next.evaluations.get(), "and no cell read");
  }

  @Test
  @DisplayName("two instances settling one shared table under contention still converge")
  void sharedTablesConvergeUnderContention() throws Exception {
    final int segments = 4;
    final int ids = 2_000;
    final MapStore store = new MapStore(ids, ids, ids, ids);
    final CountingView first = new CountingView();
    final SegmentCellVerdicts query1 = over(first, store, segments);
    assertEquals(0, hammer(query1, segments, ids, 16));
    final int settledByFirst = first.evaluations.get();
    assertTrue(settledByFirst >= segments * ids && settledByFirst <= (long) segments * ids * 16, "" + settledByFirst);

    final CountingView second = new CountingView();
    final SegmentCellVerdicts query2 = over(second, store, segments);
    assertEquals(0, hammer(query2, segments, ids, 16), "the shared table answers every worker identically");
    assertEquals(0, second.evaluations.get(), "the second query's workers evaluate NOTHING — all settled");
  }
}
