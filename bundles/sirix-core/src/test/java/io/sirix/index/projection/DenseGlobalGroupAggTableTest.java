/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DenseGlobalGroupAggTableTest {
  @Test
  void concurrentWorkersFoldOneNativeSlotAndCreateOneRosterEntry() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final DenseGlobalGroupAggTable table = new DenseGlobalGroupAggTable(workspace, 1, 2, -1L, true);
      table.initIds(1, 3);
      table.requireFullyInitialized();
      final List<FutureTask<DenseGlobalGroupAggTable.IdRoster>> tasks = new ArrayList<>(4);
      final List<Thread> workers = new ArrayList<>(4);
      for (int worker = 1; worker <= 4; worker++) {
        final long value = worker;
        final FutureTask<DenseGlobalGroupAggTable.IdRoster> task = new FutureTask<>(() -> {
          final DenseGlobalGroupAggTable.IdRoster roster = new DenseGlobalGroupAggTable.IdRoster(1);
          final MemorySegment[] values = {MemorySegment.ofArray(new long[] {value})};
          final long[] bases = {0L};
          final long[][] presence = {{1L}};
          final long[] decline = {0L};
          for (int row = 0; row < 128; row++) {
            table.foldLane(1L, value + row * 4L, values, bases, presence, 1, 0, 0, 0, roster, decline);
          }
          assertEquals(0L, decline[0]);
          return roster;
        });
        tasks.add(task);
        final Thread thread = new Thread(task, "dense-native-fold-" + worker);
        workers.add(thread);
        thread.start();
      }
      int created = 0;
      try {
        for (final FutureTask<DenseGlobalGroupAggTable.IdRoster> task : tasks) {
          created += task.get().size();
        }
      } finally {
        for (final Thread worker : workers) {
          worker.join();
        }
      }
      assertEquals(1, created);
      assertTrue(table.lanes().isNative());
      assertTrue(table.zeroSlot().isNative());
      assertArrayEquals(new long[] {512L, 1L, 512L, 1280L, 1L, 4L},
          GroupLanes.toArray(table.lanes(), table.baseOf(1), table.slotWidth()));
      assertTrue(workspace.usedBytes() > 0L);
      assertTrue(workspace.usedBytes() <= workspace.grantedBytes());
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes());
      allocator.shutdown();
    }
  }

  @Test
  void zeroSlotAndNormalSlotStaySeparateAndCloseWithTheirWorkspace() {
    final GroupWorkspace workspace = new GroupWorkspace();
    final DenseGlobalGroupAggTable table = new DenseGlobalGroupAggTable(workspace, 1, 2, -1L, true);
    try {
      table.initIds(1, 3);
      final MemorySegment[] values = {MemorySegment.ofArray(new long[] {17L})};
      final long[] bases = {0L};
      final long[][] presence = {{1L}};
      final DenseGlobalGroupAggTable.IdRoster roster = new DenseGlobalGroupAggTable.IdRoster(1);
      final long[] decline = {0L};
      table.foldLane(0L, 9L, values, bases, presence, 1, 0, 0, 0, roster, decline);
      table.foldLane(2L, 11L, values, bases, presence, 1, 0, 0, 0, roster, decline);
      assertArrayEquals(new long[] {1L, 9L, 1L, 17L, 17L, 17L}, GroupLanes.toArray(table.zeroSlot(), 0, 6));
      assertArrayEquals(new long[] {1L, 11L, 1L, 17L, 17L, 17L}, GroupLanes.toArray(table.lanes(), 6, 6));
      final GroupBuffer lanes = table.lanes();
      assertThrows(IndexOutOfBoundsException.class, () -> lanes.accumulator(1, 1, 4));
      assertThrows(IndexOutOfBoundsException.class, () -> lanes.accumulator(0, 2, 4));
      table.foldLane(3L, 1L, values, bases, presence, 1, 0, 0, 0, roster, decline);
      assertEquals(1L, decline[0]);
      workspace.close();
      final GroupBuffer closedZeroSlot = table.zeroSlot();
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(closedZeroSlot, 0));
      final GroupBuffer closedLanes = table.lanes();
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(closedLanes, 0));
      assertThrows(IllegalStateException.class,
          () -> table.foldLane(1L, 1L, values, bases, presence, 1, 0, 0, 0, roster, decline));
    } finally {
      workspace.close();
    }
  }

  @Test
  void optionalDenseTableDeclinesBeforeMappingWhenTheAllocatorIsSmallerThanTheTable() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      assertNull(DenseGlobalGroupAggTable.tryCreate(workspace, 1, 5_000_000, -1L, true, 4));
      assertEquals(0L, workspace.usedBytes());
      assertEquals(0L, workspace.borrowedBytes());
      assertEquals(0L, workspace.grantedBytes());
      final NumericGroupAggTable hash = new NumericGroupAggTable(1, 16, false, -1L).attachWorkspace(workspace);
      final int handle = hash.acquire(7L, 0L);
      final GroupBuffer accumulator = hash.storageAtAccBase(handle);
      final int base = hash.offsetAtAccBase(handle);
      GroupLanes.set(accumulator, base + 3, 42L);
      assertEquals(42L, GroupLanes.get(accumulator, base + 3));
      hash.release();
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      allocator.shutdown();
    }
  }

  @Test
  void optionalAdmissionIncludesRoundedRegionsAndEveryAuxiliaryAccumulator() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(768L << 10);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      assertEquals(96L << 10, allocator.aggregationBudget().capacity());
      assertEquals(96L << 10, DenseGlobalGroupAggTable.bytesFor(1, 1365) + 682L * 48L);
      assertNull(DenseGlobalGroupAggTable.tryCreate(workspace, 1, 1365, -1L, true, 682));
      assertEquals(0L, workspace.usedBytes());
      assertEquals(0L, workspace.grantedBytes());
      final DenseGlobalGroupAggTable table = DenseGlobalGroupAggTable.tryCreate(workspace, 1, 1365, -1L, true, 681);
      assertNotNull(table);
      for (int worker = 0; worker < 681; worker++) {
        ProjectionIndexByteScan.newGroupAggAcc(workspace, 1, Long.MAX_VALUE);
      }
      assertEquals(96L << 10, workspace.usedBytes());
      assertEquals(workspace.grantedBytes(), workspace.usedBytes());
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      allocator.shutdown();
    }
  }

  @Test
  void optionalAdmissionUsesTheRemainingTailAndTheExistingGrant() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      workspace.newBlock(8191);
      final DenseGlobalGroupAggTable table = DenseGlobalGroupAggTable.tryCreate(workspace, 1, 64, -1L, true, 4);
      assertNotNull(table);
      for (int worker = 0; worker < 4; worker++) {
        ProjectionIndexByteScan.newGroupAggAcc(workspace, 1, Long.MAX_VALUE);
      }
      assertEquals(128L << 10, workspace.usedBytes());
      assertNull(DenseGlobalGroupAggTable.tryCreate(workspace, 1, 1365, -1L, true, 1));
      assertEquals(128L << 10, workspace.usedBytes());
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void denseSlotsCannotBypassTheAggregationGrant() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      assertThrows(OutOfMemoryError.class, () -> new DenseGlobalGroupAggTable(workspace, 1, 50_000, -1L, true));
      assertEquals(0L, workspace.usedBytes());
    } finally {
      allocator.shutdown();
    }
  }
}
