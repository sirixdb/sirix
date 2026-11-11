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
      assertThrows(IndexOutOfBoundsException.class, () -> table.lanes().accumulator(1, 1, 4));
      assertThrows(IndexOutOfBoundsException.class, () -> table.lanes().accumulator(0, 2, 4));
      table.foldLane(3L, 1L, values, bases, presence, 1, 0, 0, 0, roster, decline);
      assertEquals(1L, decline[0]);
      workspace.close();
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(table.zeroSlot(), 0));
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(table.lanes(), 0));
      assertThrows(IllegalStateException.class,
          () -> table.foldLane(1L, 1L, values, bases, presence, 1, 0, 0, 0, roster, decline));
    } finally {
      workspace.close();
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
