/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class GroupZeroRetirementTest {
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void repeatedZeroKeyWorkersAndCompletedPassesReuseTheirNativeStorage(final boolean stripe, final boolean dense) {
    final int previousStripe = GroupTableSpill.setStripeSpillForTesting(stripe
        ? 1
        : 0);
    final String previousDense = System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, Boolean.toString(dense));
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      long warmedChunks = 0L;
      long warmedBytes = 0L;
      for (int pass = 0; pass < 512; pass++) {
        final GroupTableSpill spill =
            new GroupTableSpill(1, 64, hint -> new NumericGroupAggTable(128, 16, false, -1L), 2L, 0, 1, 2L).inWorkspace(
                workspace);
        try {
          for (int worker = 0; worker < 4; worker++) {
            final NumericGroupAggTable local = spill.freshLocal();
            final GroupBuffer zero = local.acquireZero(worker);
            GroupLanes.increment(zero, 0);
            GroupLanes.increment(zero, 2);
            GroupLanes.add(zero, 3, 3L);
            final long one = local.acquireAccumulator(1L, worker, 128, 4);
            GroupKernelLanes.increment(one, 0);
            GroupKernelLanes.increment(one, 2);
            GroupKernelLanes.add(one, 3, 5L);
            spill.flush(local);
            assertThrows(IllegalStateException.class, () -> GroupLanes.get(zero, 0));
          }
          final NumericGroupAggTable result = spill.takeOrCreate(0, () -> new NumericGroupAggTable(128, 16));
          try {
            assertEquals(2, result.sizeIncludingZero());
            assertEquals(4L, GroupLanes.get(result.zeroSlot(), 0));
            assertEquals(4L, GroupLanes.get(result.zeroSlot(), 2));
            assertEquals(12L, GroupLanes.get(result.zeroSlot(), 3));
            final long one = result.acquireAccumulator(1L, 0L, 128, 4);
            assertEquals(4L, GroupKernelLanes.get(one, 0));
            assertEquals(20L, GroupKernelLanes.get(one, 3));
          } finally {
            result.retire();
          }
        } finally {
          spill.close();
        }
        assertEquals(0L, workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven());
        if (pass == 15) {
          warmedChunks = workspace.chunksCarved();
          warmedBytes = workspace.borrowedBytes();
        }
      }
      assertEquals(warmedChunks, workspace.chunksCarved());
      assertEquals(warmedBytes, workspace.borrowedBytes());
      assertTrue(workspace.peakUsedBytes() <= 1L << 20);
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes());
      allocator.shutdown();
      GroupTableSpill.setStripeSpillForTesting(previousStripe);
      if (previousDense == null) {
        System.clearProperty(GroupTableSpill.DENSE_INDEX_PROPERTY);
      } else {
        System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, previousDense);
      }
    }
  }

  @Test
  void releasePreservesZeroValuesAndFinalRetirementInvalidatesThem() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final NumericGroupAggTable table = new NumericGroupAggTable(1, 16).attachWorkspace(workspace);
      final GroupBuffer original = table.acquireZero(7L);
      GroupLanes.set(original, 0, 11L);
      final long record = original.accumulator(0, 1, 4);
      table.release();
      assertEquals(11L, GroupLanes.get(table.zeroSlot(), 0));
      assertEquals(7L, GroupLanes.get(table.zeroSlot(), 1));
      table.retire();
      final long given = workspace.chunksGiven();
      table.retire();
      assertEquals(given, workspace.chunksGiven());
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(original, 0));
      final NumericGroupAggTable next = new NumericGroupAggTable(1, 16).attachWorkspace(workspace);
      final GroupBuffer reused = next.acquireZero(13L);
      assertEquals(original.address, reused.address);
      assertNotEquals(original.generation, reused.generation);
      GroupLanes.set(reused, 0, 17L);
      if (GroupKernelLanes.DEBUG) {
        assertThrows(IllegalStateException.class, () -> GroupKernelLanes.set(record, 0, 19L));
      }
      assertEquals(17L, GroupLanes.get(reused, 0));
      next.retire();
      final NumericGroupAggTable empty = new NumericGroupAggTable(1, 16).attachWorkspace(workspace);
      empty.retire();
      assertThrows(IllegalStateException.class, empty::zeroSlot);
      assertEquals(0L, workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven());
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void abortedSpillsReturnTheirZeroKeyTables() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final GroupTableSpill spill =
          new GroupTableSpill(1, 64, hint -> new NumericGroupAggTable(1, 16, false, 1L), 1L, 0, 1, 1L).inWorkspace(
              workspace);
      try {
        final NumericGroupAggTable local = spill.freshLocal();
        final GroupBuffer zero = local.acquireZero(0L);
        GroupLanes.increment(zero, 0);
        spill.flush(local);
        spill.releaseTables();
        assertEquals(0L, workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven());
        assertThrows(IllegalStateException.class, () -> GroupLanes.get(zero, 0));
      } finally {
        spill.close();
      }
    } finally {
      allocator.shutdown();
    }
  }
}
