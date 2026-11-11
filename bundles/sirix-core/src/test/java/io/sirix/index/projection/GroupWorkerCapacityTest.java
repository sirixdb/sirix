/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class GroupWorkerCapacityTest {
  private String previousHint;
  private String previousDense;
  private int previousFlush;
  private int previousBatch;
  private int previousStripe;

  @BeforeEach
  void useProductionWorkerGeometry() {
    previousHint = System.setProperty(GroupTableSpill.WORKER_HINT_PROPERTY, "65536");
    previousDense = System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, "true");
    previousFlush = GroupTableSpill.setFlushGroupsForTesting(65536);
    previousBatch = GroupTableSpill.setSubChunkLeavesForTesting(64);
    previousStripe = GroupTableSpill.setStripeSpillForTesting(1);
  }

  @AfterEach
  void restoreGeometry() {
    GroupTableSpill.setStripeSpillForTesting(previousStripe);
    GroupTableSpill.setSubChunkLeavesForTesting(previousBatch);
    GroupTableSpill.setFlushGroupsForTesting(previousFlush);
    restore(GroupTableSpill.WORKER_HINT_PROPERTY, previousHint);
    restore(GroupTableSpill.DENSE_INDEX_PROPERTY, previousDense);
  }

  private static void restore(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  @Test
  void aMillionRowsWithTwoKeysFitsAOneGiBAllocatorWithSixteenRequestedWorkers() throws Exception {
    aggregateTwoKeys(1L << 30, false);
  }

  @Test
  void aSmallerAllocatorShrinksTheWorkerGeometryForInterleavedTables() throws Exception {
    System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, "false");
    aggregateTwoKeys(64L << 20, true);
  }

  private static void aggregateTwoKeys(final long allocatorBytes, final boolean smallerGeometry) throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(allocatorBytes);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      assertTrue(workspace.preparePlan(16, 1, 7) > 0L);
      assertTrue(workspace.plannedWorkers() > 0 && workspace.plannedWorkers() < 16);
      assertEquals(allocatorBytes / 8, workspace.grantedBytes());
      if (smallerGeometry) {
        assertTrue(workspace.workerTableHint() < 65536);
        assertTrue(workspace.batchLeaves() < 64);
      }
      final long budget = GroupTableSpill.groupBudget(7, workspace);
      final GroupTableSpill spill = new GroupTableSpill(1, 64, hint -> new NumericGroupAggTable(1, hint, false, 1L), 2L,
          0, 1, budget).inWorkspace(workspace);
      final ExecutorService pool = Executors.newFixedThreadPool(workspace.plannedWorkers());
      try {
        final int workers = workspace.plannedWorkers();
        final List<Future<?>> tasks = new ArrayList<>(workers);
        for (int worker = 0; worker < workers; worker++) {
          final int firstRow = worker;
          tasks.add(pool.submit(() -> {
            final NumericGroupAggTable local = spill.freshLocal();
            try {
              for (int row = firstRow; row < 1_000_000; row += workers) {
                final long key = (row & 1) + 1L;
                final int handle = local.acquire(key, row);
                final GroupBuffer storage = local.storageAtAccBase(handle);
                final int base = local.offsetAtAccBase(handle);
                GroupLanes.increment(storage, base);
                GroupLanes.increment(storage, base + 2);
                GroupLanes.add(storage, base + 3, key == 1L
                    ? 3L
                    : 5L);
              }
              spill.flush(local);
            } finally {
              if (!local.released()) {
                local.release();
              }
            }
          }));
        }
        for (final Future<?> task : tasks) {
          task.get();
        }
        final NumericGroupAggTable result = spill.takeOrCreate(0, () -> new NumericGroupAggTable(1, 16, false, 1L));
        try {
          assertEquals(2, result.size());
          assertGroup(result, 1L, 500_000L, 1_500_000L);
          assertGroup(result, 2L, 500_000L, 2_500_000L);
          assertTrue(workspace.peakUsedBytes() <= workspace.grantedBytes());
        } finally {
          result.release();
        }
      } finally {
        pool.close();
        spill.close();
      }
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes());
      allocator.shutdown();
    }
  }

  private static void assertGroup(final NumericGroupAggTable table, final long key, final long count, final long sum) {
    final int handle = table.acquire(key, 0L);
    final GroupBuffer storage = table.storageAtAccBase(handle);
    final int base = table.offsetAtAccBase(handle);
    assertEquals(count, GroupLanes.get(storage, base));
    assertEquals(count, GroupLanes.get(storage, base + 2));
    assertEquals(sum, GroupLanes.get(storage, base + 3));
  }

  @Test
  void anAllowanceBelowTheSmallestGeometryDeclinesWithoutTakingMemory() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      assertEquals(0L, workspace.preparePlan(16, 32, 7));
      assertEquals(0, workspace.plannedWorkers());
      assertEquals(0L, workspace.grantedBytes());
      assertEquals(0L, workspace.borrowedBytes());
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
    } finally {
      allocator.shutdown();
    }
  }
}
