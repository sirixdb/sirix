/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Grant exhaustion must not strand new chunks or leave a table referring to returned chunks. */
final class GroupGrowthFailureTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedGrowthReturnsEveryChunkAndTheWorkspaceCanBeReused(final boolean dense) {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      for (int round = 0; round < 2; round++) {
        final NumericGroupAggTable table = new NumericGroupAggTable(2, 16, false, -1L).attachWorkspace(workspace);
        if (dense) {
          table.useDenseIndex();
        }
        assertThrows(OutOfMemoryError.class, () -> {
          for (int key = 1; key < 100_000; key++) {
            table.acquire(key, key);
          }
        });
        assertTrue(table.rehashes() > 0, "the failure must follow real growth");
        table.release();
        assertEquals(0L, workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven(),
            "a failed replacement or old table still holds chunks");
        assertTrue(workspace.peakUsedBytes() <= workspace.grantedBytes());
      }
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes());
      allocator.shutdown();
    }
  }
}
