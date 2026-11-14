package io.sirix.query.scan;

import io.sirix.index.projection.GroupBuffer;
import io.sirix.index.projection.GroupLanes;
import io.sirix.index.projection.GroupTableSpill;
import io.sirix.index.projection.GroupWorkspace;
import io.sirix.index.projection.NumericGroupAggTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What an aborted pass owes its restart: every chunk of its tables back in the scan's
 * {@link GroupWorkspace}. The restart builds its tables from those chunks, and
 * {@link SirixVectorizedExecutor.GroupPasses#restart} counts the workspace as available when it
 * re-reads the budget, so a chunk an aborted pass kept would be memory the restart has to borrow a
 * second time. The finished workers' final tables are never flushed and still own their stripes and
 * index chunks; they must hand them back with the spill's own tables.
 */
final class GroupAbortedPassReleaseTest {
  @Test
  void failedFlushReturnsItsLocalAndAnEarlyWorkerReturnIsStillOwned() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupTableSpill spill = new GroupTableSpill(PARTITIONS, SHIFT, GroupAbortedPassReleaseTest::denseTable, 0,
          PARTITIONS, 50_000L).inWorkspace(workspace);
      final NumericGroupAggTable[] tables = {spill.freshLocal(), spill.freshLocal()};
      final GroupBuffer firstZero = tables[0].acquireZero(0L);
      final GroupBuffer secondZero = tables[1].acquireZero(0L);
      GroupLanes.increment(firstZero, 0);
      GroupLanes.increment(secondZero, 0);
      for (final NumericGroupAggTable local : tables) {
        for (int key = 1; key <= 128; key++) {
          local.acquire(key, key);
        }
      }
      GroupTableSpill.setSimulateOutOfMemoryOnFlushForTesting(true);
      try {
        assertThrows(OutOfMemoryError.class, () -> spill.flush(tables[0]));
        assertTrue(tables[0].released(), "the failed flush consumes its local table");
        assertEquals(128, spill.groupsAbandoned());
        // Worker 1 leaves its scan early; it never reaches result-index construction.
        SirixVectorizedExecutor.releaseAbortedPass(spill, tables, new int[2][][]);
        assertEquals(0L, chunksOut(workspace));
        assertThrows(IllegalStateException.class, () -> GroupLanes.get(firstZero, 0));
        assertThrows(IllegalStateException.class, () -> GroupLanes.get(secondZero, 0));
      } finally {
        GroupTableSpill.setSimulateOutOfMemoryOnFlushForTesting(false);
      }
    }
  }


  private static final int PARTITIONS = 32;

  private static final int SHIFT = 59;

  /** Eight lanes per stripe (key + 6 slot + aux), i.e. above {@code DENSE_INDEX_MIN_STRIDE}. */
  private static NumericGroupAggTable denseTable() {
    return new NumericGroupAggTable(1, 1 << 14, true, 0L, 0);
  }

  /** Chunks the workspace has handed out and not been given back. */
  private static long chunksOut(final GroupWorkspace workspace) {
    return workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven();
  }

  @Test
  @DisplayName("releasing an aborted pass hands every chunk of its tables back to the scan's workspace")
  void abortedPassHandsEveryChunkBackForTheRestart() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupTableSpill spill = new GroupTableSpill(PARTITIONS, SHIFT, GroupAbortedPassReleaseTest::denseTable, 0,
          PARTITIONS, 50_000L).inWorkspace(workspace);

      // Two workers finished their chunk before the pass aborted: never flushed, so their tables
      // still own every stripe and index chunk they took.
      final NumericGroupAggTable[] tables = new NumericGroupAggTable[2];
      for (int t = 0; t < tables.length; t++) {
        final NumericGroupAggTable local = spill.freshLocal();
        final long base = (long) t * 20_000L;
        for (long key = 1L; key <= 20_000L; key++) {
          local.acquire(base + key, base + key);
        }
        spill.noteAbandonedLocal(local.size());
        tables[t] = local;
      }
      final long abandoned = spill.groupsAbandoned();
      assertEquals(40_000L, abandoned, "the estimate has already read the abandoned locals");
      final long held = chunksOut(workspace);
      assertTrue(held > 0L, "the workers' tables hold chunks of the scan's workspace");
      final long borrowed = workspace.borrowedBytes();

      SirixVectorizedExecutor.releaseAbortedPass(spill, tables, new int[tables.length][][]);

      assertEquals(0L, chunksOut(workspace),
          "a chunk the aborted pass took is still out: the restart would have to borrow it again");
      assertEquals(borrowed, workspace.borrowedBytes(),
          "the chunks stay the scan's own, which is what the restart's budget re-read counts as available");
      assertEquals(abandoned, spill.groupsAbandoned(), "the counters the estimate reads survive the release");
      for (final NumericGroupAggTable table : tables) {
        assertTrue(table == null, "the aborted pass keeps no reference to its tables");
      }

      // The restart: the same two workers over the same keys build their tables from what was handed
      // back.
      final GroupTableSpill restart = new GroupTableSpill(PARTITIONS, SHIFT, GroupAbortedPassReleaseTest::denseTable, 0,
          PARTITIONS, 50_000L).inWorkspace(workspace);
      final long carved = workspace.chunksCarved();
      for (int t = 0; t < tables.length; t++) {
        final NumericGroupAggTable local = restart.freshLocal();
        final long base = (long) t * 20_000L;
        for (long key = 1L; key <= 20_000L; key++) {
          local.acquire(base + key, base + key);
        }
        tables[t] = local;
      }
      assertEquals(held, chunksOut(workspace), "the restart's tables are as large as the aborted pass's");
      assertEquals(carved, workspace.chunksCarved(), "and every one of their chunks is one the aborted pass held");
      assertEquals(borrowed, workspace.borrowedBytes(), "nothing more was borrowed from the allocator");
      SirixVectorizedExecutor.releaseAbortedPass(restart, tables, new int[tables.length][][]);
    }
  }
}
