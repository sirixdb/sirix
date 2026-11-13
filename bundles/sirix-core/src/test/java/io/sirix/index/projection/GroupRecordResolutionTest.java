/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ways a kernel reaches a group's accumulator agree with one another, and a table that can no
 * longer be probed says so on every one of them.
 */
final class GroupRecordResolutionTest {

  private static final int COLUMNS = 2;

  @Test
  void theOneStepAcquisitionNamesTheRecordTheHandleNames() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = new NumericGroupAggTable(COLUMNS, 16, true).attachWorkspace(workspace);
      for (int key = 1; key <= 50_000; key++) {
        // First sight through the one step, which inserts and may grow the table under the row.
        GroupKernelLanes.add(table.acquireAccumulator(key, key, COLUMNS, 4), 0, key);
      }
      assertTrue(table.rehashes() > 0, "the acquisitions grew the table");
      assertEquals(50_000, table.size());
      for (int key = 1; key <= 50_000; key++) {
        final int handle = table.acquire(key, -1L);
        final long byHandle = table.accumulatorAt(handle, COLUMNS, 4);
        assertEquals(key, GroupKernelLanes.get(byHandle, 0), "the count the one step folded");
        assertEquals(key, GroupKernelLanes.get(byHandle, 1), "the ordinal of the first sight, not of this lookup");
        GroupKernelLanes.increment(table.acquireAccumulator(key, -1L, COLUMNS, 4), 0);
        assertEquals(key + 1L, GroupLanes.get(table.storageAtAccBase(handle), table.offsetAtAccBase(handle)),
            "one step, handle and chunk reach the same lane");
      }
      assertEquals(50_000, table.size(), "looking a group up inserts nothing");
      table.release();
    }
  }

  @Test
  void aKeyOutsideThePassRangeFoldsIntoTheDiscardBlock() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = new NumericGroupAggTable(COLUMNS, 64, false).attachWorkspace(workspace);
      final int shift = 62; // four partitions
      table.setPassRange(shift, 1, 2);
      int owned = 0;
      for (int key = 1; key <= 4_000; key++) {
        final boolean inPass = NumericGroupAggTable.partitionOf(key, shift) == 1;
        final long oneStepRecord = table.acquireAccumulator(key, key, COLUMNS, 4);
        GroupKernelLanes.increment(oneStepRecord, 0);
        final int handle = table.acquire(key, key);
        assertEquals(inPass, handle != NumericGroupAggTable.DISCARD_HANDLE);
        if (inPass) {
          owned++;
          assertEquals(1L, GroupKernelLanes.get(table.accumulatorAt(handle, COLUMNS, 4), 0));
        } else {
          assertEquals(GroupKernelLanes.get(oneStepRecord, 0),
              GroupKernelLanes.get(table.accumulatorAt(handle, COLUMNS, 4), 0),
              "the discard handle resolves to the block the one step folded into");
        }
      }
      assertTrue(owned > 0 && owned < 4_000, "the range splits the keys");
      assertEquals(owned, table.size(), "only the pass's own groups were inserted");
      table.release();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aReleasedTableRefusesEveryAcquisition(final boolean dense) {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table =
          new NumericGroupAggTable(COLUMNS, 16, false, -1L, 1).attachWorkspace(workspace);
      if (dense) {
        table.useDenseIndex();
      }
      final long[] identity = {42L};
      final int handle = table.acquireExact(7L, 1L, identity, 0);
      assertEquals(7L, table.keyAtAccBase(handle));
      table.release();
      // The group it held is not found again: a released table's probes all miss, and the miss is
      // where its state is examined.
      assertThrows(IllegalStateException.class, () -> table.acquireExact(7L, 2L, identity, 0));
      assertThrows(IllegalStateException.class, () -> table.acquire(7L, 2L));
      assertThrows(IllegalStateException.class, () -> table.acquireAccumulator(7L, 2L, COLUMNS, 4));
      assertThrows(IndexOutOfBoundsException.class, () -> table.accumulatorAt(handle, COLUMNS, 4));
      assertThrows(IndexOutOfBoundsException.class, () -> table.storageAtAccBase(handle));
    }
  }

  // The chain is the call under test: debug mode refuses in storageAtAccBase, else in accumulator.
  @SuppressWarnings("java:S5778")
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aTableOfAClosedWorkspaceRefusesToProbe(final boolean dense) {
    final GroupWorkspace workspace = new GroupWorkspace();
    final NumericGroupAggTable table = new NumericGroupAggTable(COLUMNS, 16, false).attachWorkspace(workspace);
    if (dense) {
      table.useDenseIndex();
    }
    final int handle = table.acquire(7L, 1L);
    workspace.close();
    // Its chunks went back to the allocator: not even the key it holds may be read again.
    assertThrows(IllegalStateException.class, () -> table.acquire(7L, 2L));
    assertThrows(IllegalStateException.class, () -> table.acquireAccumulator(7L, 2L, COLUMNS, 4));
    assertThrows(IllegalStateException.class, () -> table.keyAtAccBase(handle));
    assertThrows(IllegalStateException.class, () -> table.storageAtAccBase(handle).accumulator(1, COLUMNS, 4));
  }

  @Test
  void aFailedGrowthLeavesATableThatCanOnlyBeReleased() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final NumericGroupAggTable table = new NumericGroupAggTable(COLUMNS, 16, false).attachWorkspace(workspace);
      assertThrows(OutOfMemoryError.class, () -> {
        for (int key = 1; key < 1_000_000; key++) {
          table.acquire(key, key);
        }
      });
      assertTrue(table.rehashes() > 0, "the failure followed real growth");
      // Growth had handed chunks back as it emptied them, so the groups are not all there any more:
      // none is found, and nothing further is accepted.
      assertThrows(OutOfMemoryError.class, () -> table.acquire(1L, 1L));
      assertThrows(OutOfMemoryError.class, () -> table.acquireAccumulator(1L, 1L, COLUMNS, 4));
      assertThrows(OutOfMemoryError.class, () -> table.keyAtBucket(0));
      table.release();
      assertEquals(0L, workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven(),
          "every chunk went back exactly once");
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      allocator.shutdown();
    }
  }

  @Test
  void growthHandsItsChunksBackWhileItWalksThemAndLosesNoGroup() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = new NumericGroupAggTable(COLUMNS, 16, true).attachWorkspace(workspace);
      final int groups = 200_000;
      for (int key = 1; key <= groups; key++) {
        GroupKernelLanes.add(table.acquireAccumulator(key, key, COLUMNS, 4), 0, key);
      }
      assertTrue(table.storageChunkCount() > 8, "the table spans many full chunks");
      assertTrue(workspace.chunksReused() > 0, "chunks a growth emptied served a grown table");
      for (int key = 1; key <= groups; key++) {
        final long groupRecord = table.accumulatorAt(table.acquire(key, -1L), COLUMNS, 4);
        assertEquals(key, GroupKernelLanes.get(groupRecord, 0));
        assertEquals(key, GroupKernelLanes.get(groupRecord, 1));
      }
      assertEquals(groups, table.size());
      final long live = workspace.chunksCarved() + workspace.chunksReused() - workspace.chunksGiven();
      assertEquals(table.storageChunkCount(), live,
          "the table holds its own chunks and no chunk of a table it grew from");
      table.release();
    }
  }

  @Test
  void aBlockIsResolvedOnceForAWholeScan() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer missing = workspace.newBlock(2 + 4 * COLUMNS);
      final NumericGroupAggTable table = new NumericGroupAggTable(COLUMNS, 16, false).attachWorkspace(workspace);
      // What a kernel does with its missing-key accumulator: one resolution, outside the row loop.
      final long missingRecord = missing.accumulator(0, COLUMNS, 4);
      assertEquals(missingRecord, missing.accumulator(0, COLUMNS, 4), "the same footprint is the same record");
      if (GroupKernelLanes.DEBUG) {
        // A debug record names its footprint, not only where it starts.
        assertNotEquals(missingRecord, missing.record(0, 2));
        final long twoLaneRecord = missing.record(0, 2);
        assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.get(twoLaneRecord, 2));
      }
      for (int row = 0; row < 100_000; row++) {
        GroupKernelLanes.increment(missingRecord, 0);
        GroupKernelLanes.increment(table.acquireAccumulator(row % 5_000 + 1L, row, COLUMNS, 4), 0);
      }
      assertEquals(100_000L, GroupLanes.get(missing, 0), "the record outlived a hundred thousand other resolutions");
      table.release();
    }
  }
}
