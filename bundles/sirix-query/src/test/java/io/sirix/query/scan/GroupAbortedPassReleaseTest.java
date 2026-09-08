package io.sirix.query.scan;

import io.sirix.index.projection.GroupTableSpill;
import io.sirix.index.projection.LongChunkPool;
import io.sirix.index.projection.NumericGroupAggTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The release order an aborted pass owes its restart: a per-scan recycler is invisible to
 * {@link LongChunkPool#retainedBytes()}, so every chunk still sitting in one when
 * {@link SirixVectorizedExecutor.GroupPasses#restart} forces its collection is measured as live
 * heap and subtracted from the refreshed budget. The finished workers' final tables must therefore
 * hand their payload and index chunks back BEFORE the spill drains, not after.
 */
final class GroupAbortedPassReleaseTest {

  private static final int PARTITIONS = 32;

  private static final int SHIFT = 59;

  /** Eight lanes per stripe (key + 6 slot + aux), i.e. above {@code DENSE_INDEX_MIN_STRIDE}. */
  private static NumericGroupAggTable denseTable() {
    return new NumericGroupAggTable(1, 1 << 14, true, 0L, 0);
  }

  @Test
  @DisplayName("releasing an aborted pass leaves neither per-scan recycler holding a chunk")
  void abortedPassLeavesNoChunkForTheBudgetRefreshToMeasure() {
    final int poolBefore = GroupTableSpill.setChunkPoolForTesting(1);
    final int retainBefore = LongChunkPool.setRetainForTesting(0);
    try {
      final GroupTableSpill spill =
          new GroupTableSpill(PARTITIONS, SHIFT, GroupAbortedPassReleaseTest::denseTable, 0, PARTITIONS, 50_000L);
      final LongChunkPool payload = spill.chunkPool();
      final LongChunkPool probes = spill.probeChunkPool();
      assertNotNull(payload, "the pool is switched on for this test");
      assertNotNull(probes, "a stripe above the dense crossing gets an index recycler");
      assertFalse(payload.isShared(), "retention off: the payload pool is per-scan and never added back");
      assertFalse(probes.isShared(), "the index recycler is always per-scan and never added back");

      // Two workers finished their chunk before the pass aborted: never flushed, so their tables
      // still own every payload and index chunk they took.
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

      SirixVectorizedExecutor.releaseAbortedPass(spill, tables, new int[tables.length][][]);

      assertEquals(0, payload.pooled(), "no payload chunk survives into the restart's budget measurement: " + payload);
      assertEquals(0, probes.pooled(), "no index chunk survives into the restart's budget measurement: " + probes);
      assertEquals(abandoned, spill.groupsAbandoned(), "the counters the estimate reads survive the release");
      for (final NumericGroupAggTable table : tables) {
        assertTrue(table == null, "the aborted pass keeps no reference to its tables");
      }
    } finally {
      LongChunkPool.setRetainForTesting(retainBefore);
      GroupTableSpill.setChunkPoolForTesting(poolBefore);
    }
  }

  @Test
  @DisplayName("a shared payload pool keeps its chunks; only the per-scan index recycler drains")
  void aSharedPayloadPoolIsRetainedWhileTheIndexRecyclerDrains() {
    final int poolBefore = GroupTableSpill.setChunkPoolForTesting(1);
    final int retainBefore = LongChunkPool.setRetainForTesting(1);
    try {
      final GroupTableSpill spill =
          new GroupTableSpill(PARTITIONS, SHIFT, GroupAbortedPassReleaseTest::denseTable, 0, PARTITIONS, 50_000L);
      final LongChunkPool payload = spill.chunkPool();
      final LongChunkPool probes = spill.probeChunkPool();
      assertNotNull(probes, "a stripe above the dense crossing gets an index recycler");
      assertTrue(payload.isShared(), "retention on: the payload pool outlives the scan and IS added back");

      final NumericGroupAggTable[] tables = { spill.freshLocal() };
      for (long key = 1L; key <= 20_000L; key++) {
        tables[0].acquire(key, key);
      }

      SirixVectorizedExecutor.releaseAbortedPass(spill, tables, new int[1][][]);

      assertTrue(payload.pooled() > 0, "the shared pool keeps the aborted pass's chunks for the restart: " + payload);
      assertEquals(0, probes.pooled(), "the per-scan index recycler still drains: " + probes);
    } finally {
      LongChunkPool.setRetainForTesting(retainBefore);
      GroupTableSpill.setChunkPoolForTesting(poolBefore);
    }
  }
}
