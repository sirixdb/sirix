/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GroupWorkspaceGrantTest {
  private String previousWorkerHint;
  private String previousFlushGroups;
  private int previousBatch;

  @BeforeEach
  void useSmallWorkerTablesForTheTinyGrantFixture() {
    previousWorkerHint = System.setProperty(GroupTableSpill.WORKER_HINT_PROPERTY, "128");
    previousFlushGroups = System.setProperty(GroupTableSpill.FLUSH_GROUPS_PROPERTY, "128");
    previousBatch = GroupTableSpill.setSubChunkLeavesForTesting(1);
  }

  @AfterEach
  void restoreWorkerHint() {
    GroupTableSpill.setSubChunkLeavesForTesting(previousBatch);
    if (previousFlushGroups == null) {
      System.clearProperty(GroupTableSpill.FLUSH_GROUPS_PROPERTY);
    } else {
      System.setProperty(GroupTableSpill.FLUSH_GROUPS_PROPERTY, previousFlushGroups);
    }
    if (previousWorkerHint == null) {
      System.clearProperty(GroupTableSpill.WORKER_HINT_PROPERTY);
    } else {
      System.setProperty(GroupTableSpill.WORKER_HINT_PROPERTY, previousWorkerHint);
    }
  }

  @Test
  void twoScansPlanFromExclusiveCapacityAndReuseAfterClose() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
    final GroupWorkspace first = GroupWorkspace.forScan(allocator);
    final long usable = first.preparePlan(1, 2, 13);
    final long capacity = first.grantedBytes();
    final CountDownLatch starting = new CountDownLatch(1);
    final FutureTask<Long> second = new FutureTask<>(() -> {
      starting.countDown();
      try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
        assertEquals(usable, workspace.preparePlan(1, 2, 13));
        final GroupBuffer chunk = workspace.takeChunk(128);
        GroupLanes.set(chunk, 0, 42L);
        assertEquals(42L, GroupLanes.get(chunk, 0));
        assertTrue(workspace.peakUsedBytes() <= workspace.grantedBytes());
        return workspace.grantedBytes();
      }
    });
    final Thread waiter = new Thread(second, "group-workspace-admission-test");
    try {
      waiter.start();
      assertTrue(starting.await(10, TimeUnit.SECONDS));
      assertEquals(capacity, allocator.aggregationBudget().grantedBytes());
      first.close();
      assertEquals(capacity, second.get(10, TimeUnit.SECONDS));
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes());
    } finally {
      first.close();
      waiter.interrupt();
      waiter.join();
      allocator.shutdown();
    }
  }

  @Test
  void aTinyBudgetRejectsTheMinimumWorkingSetBeforeMapping() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      assertEquals(0L, workspace.preparePlan(1, 1, 13));
      assertEquals(0, workspace.plannedWorkers());
      assertEquals(0L, workspace.borrowedBytes());
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void aScanWithoutPassPlanningUsesItsFinalRegionCapacity() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      for (int block = 0; block < 16; block++) {
        final GroupBuffer buffer = workspace.newBlock(8192);
        GroupLanes.set(buffer, 8191, block);
      }
      assertEquals(1L << 20, workspace.grantedBytes());
      assertEquals(workspace.grantedBytes(), workspace.peakUsedBytes());
      assertThrows(OutOfMemoryError.class, () -> workspace.newBlock(1));
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void boundedPlansAndExplicitGroupOverridesCannotExceedTheGrantedBytes() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
    final long previous = GroupTableSpill.setGroupBudgetForTesting(Long.MAX_VALUE);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final long usable = workspace.preparePlan(2, 4, 13);
      final long ordinary = GroupTableSpill.groupBudget(13, workspace);
      final long bounded = GroupTableSpill.boundedGroupBudget(13, workspace);
      assertEquals(ordinary, bounded);
      assertTrue(bounded > 0L);
      assertTrue(bounded * GroupTableSpill.bytesPerGroup(13) <= usable);
      assertTrue(usable < workspace.grantedBytes(), "worker and rounding overhead is reserved");
      assertEquals(usable, workspace.preparePlan(2, 4, 13), "headroom is not divided again");
    } finally {
      GroupTableSpill.setGroupBudgetForTesting(previous);
      allocator.shutdown();
    }
  }
}
