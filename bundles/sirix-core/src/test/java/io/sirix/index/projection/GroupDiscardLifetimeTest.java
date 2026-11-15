/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class GroupDiscardLifetimeTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void filteredWorkerFlushesReuseTheirDiscardStorage(final boolean dense) {
    final String previousDense = System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, Boolean.toString(dense));
    final int previousStripe = GroupTableSpill.setStripeSpillForTesting(1);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final GroupTableSpill spill =
          new GroupTableSpill(2, 63, hint -> new NumericGroupAggTable(1, 16, false, 1L), 1L, 1, 2, 2L).inWorkspace(
              workspace);
      try {
        final long keptKey = keyInPartition(1);
        final long droppedKey = keyInPartition(0);
        long warmedChunks = 0L;
        long warmedBytes = 0L;
        for (int round = 0; round < 10_000; round++) {
          final NumericGroupAggTable local = spill.freshLocal();
          final long kept = local.acquireAccumulator(keptKey, round, 1, 4);
          GroupKernelLanes.increment(kept, 0);
          GroupKernelLanes.increment(kept, 2);
          GroupKernelLanes.add(kept, 3, 4L);
          final long dropped = local.acquireAccumulator(droppedKey, round, 1, 4);
          GroupKernelLanes.increment(dropped, 0);
          final GroupBuffer droppedZero = local.acquireZero(round);
          GroupLanes.increment(droppedZero, 0);
          spill.flush(local);
          if (round == 127) {
            warmedChunks = workspace.chunksCarved();
            warmedBytes = workspace.borrowedBytes();
          }
        }
        assertEquals(warmedChunks, workspace.chunksCarved());
        assertEquals(warmedBytes, workspace.borrowedBytes());
        assertTrue(workspace.chunksReused() >= 20_000L);
        assertFalse(spill.aborted());
        final NumericGroupAggTable result = spill.takeOrCreate(1, () -> new NumericGroupAggTable(1, 16, false, 1L));
        try {
          assertEquals(1, result.size());
          assertFalse(result.hasZeroKey());
          final long kept = result.acquireAccumulator(keptKey, 0L, 1, 4);
          assertEquals(10_000L, GroupKernelLanes.get(kept, 0));
          assertEquals(10_000L, GroupKernelLanes.get(kept, 2));
          assertEquals(40_000L, GroupKernelLanes.get(kept, 3));
        } finally {
          result.release();
        }
      } finally {
        spill.close();
      }
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
  void returningAndReusingBothDiscardBuffersInvalidatesTheirOldViews() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final NumericGroupAggTable original = filteredTable(workspace);
      final int dropped = original.acquire(keyInPartition(0), 0L);
      assertEquals(NumericGroupAggTable.DISCARD_HANDLE, dropped);
      final GroupBuffer oldDiscard = original.storageAtAccBase(dropped);
      final GroupBuffer oldZero = original.acquireZero(0L);
      final long oldDiscardRecord = original.accumulatorAt(dropped, 1, 4);
      final long oldZeroRecord = oldZero.accumulator(0, 1, 4);
      original.release();
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(oldDiscard, 1));
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(oldZero, 0));
      final long returned = workspace.chunksGiven();
      original.release();
      assertEquals(returned, workspace.chunksGiven());
      assertThrows(IllegalStateException.class, () -> original.setPassRange(63, 1, 2));
      final NumericGroupAggTable next = filteredTable(workspace);
      final GroupBuffer newDiscard = next.storageAtAccBase(next.acquire(keyInPartition(0), 0L));
      final GroupBuffer newZero = next.acquireZero(0L);
      assertEquals(oldDiscard.address, newDiscard.address);
      assertEquals(oldZero.address, newZero.address);
      assertNotEquals(oldDiscard.generation, newDiscard.generation);
      assertNotEquals(oldZero.generation, newZero.generation);
      GroupLanes.set(newDiscard, 1, 17L);
      GroupLanes.set(newZero, 0, 19L);
      assertThrows(IllegalStateException.class, () -> GroupLanes.set(oldDiscard, 1, 99L));
      assertThrows(IllegalStateException.class, () -> GroupLanes.set(oldZero, 0, 99L));
      if (GroupKernelLanes.DEBUG) {
        assertThrows(IllegalStateException.class, () -> GroupKernelLanes.set(oldDiscardRecord, 0, 99L));
        assertThrows(IllegalStateException.class, () -> GroupKernelLanes.set(oldZeroRecord, 0, 99L));
      }
      assertEquals(17L, GroupLanes.get(newDiscard, 1));
      assertEquals(19L, GroupLanes.get(newZero, 0));
      next.release();
      final NumericGroupAggTable unfiltered = new NumericGroupAggTable(1, 16).attachWorkspace(workspace);
      final GroupBuffer ordinaryZero = unfiltered.acquireZero(23L);
      GroupLanes.set(ordinaryZero, 0, 29L);
      unfiltered.release();
      assertEquals(29L, GroupLanes.get(ordinaryZero, 0));
      assertEquals(23L, GroupLanes.get(ordinaryZero, 1));
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void failedSecondDiscardAllocationReturnsTheFirstChunk() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final NumericGroupAggTable table = new NumericGroupAggTable(4094, 16, false, -1L).attachWorkspace(workspace);
      for (int attempt = 0; attempt < 2; attempt++) {
        assertThrows(OutOfMemoryError.class, () -> table.setPassRange(63, 1, 2));
        assertEquals(0L, workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven());
        assertEquals(1L, workspace.chunksCarved());
      }
      table.release();
      assertEquals(0L, workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven());
    } finally {
      allocator.shutdown();
    }
  }

  private static NumericGroupAggTable filteredTable(final GroupWorkspace workspace) {
    final NumericGroupAggTable table = new NumericGroupAggTable(1, 16, false, 1L).attachWorkspace(workspace);
    table.setPassRange(63, 1, 2);
    return table;
  }

  private static long keyInPartition(final int partition) {
    for (long key = 1L; key < 100L; key++) {
      if (NumericGroupAggTable.partitionOf(key, 63) == partition) {
        return key;
      }
    }
    throw new AssertionError("no key in partition " + partition);
  }
}
