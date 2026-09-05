/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lazy per-cell verdict: one evaluation per distinct {@code (segment, id)}, whatever the number
 * of rows or workers that ask for it.
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

  @Test
  @DisplayName("under the parallel scan the lock-free memo still evaluates each cell once")
  void concurrentReadersAgree() throws Exception {
    final int segments = 4;
    final int ids = 2_000; // past the initial table, so the growth path runs under contention
    final CountingView view = new CountingView();
    final SegmentCellVerdicts verdicts =
        new SegmentCellVerdicts(view, ProjectionIndexScan.Op.STR_CONTAINS, new byte[] {'x'}, segments);

    final int workers = 16;
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
    assertEquals(0, disagreements.get(), "a cell's verdict must not depend on who read it");
    assertTrue(view.evaluations.get() >= segments * ids, "every distinct cell is evaluated");
    assertTrue(view.evaluations.get() < segments * ids * 3L,
        "and a benign race may redo a few, but nowhere near once per reader: "
            + view.evaluations.get() + " for " + (segments * ids) + " distinct cells across " + workers + " workers");
  }
}
