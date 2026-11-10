/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.projection.GroupTableSpill;
import io.sirix.index.projection.GroupWorkspace;
import io.sirix.index.projection.ProjectionIndexRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class GroupPassGrantCapacityTest {
  @Test
  void partitionTailsAndCompletedReplayBothFitTheActualGrant() {
    final String previousHint = System.setProperty(GroupTableSpill.WORKER_HINT_PROPERTY, "128");
    final String previousFlush = System.setProperty(GroupTableSpill.FLUSH_GROUPS_PROPERTY, "128");
    final int previousBatch = GroupTableSpill.setSubChunkLeavesForTesting(1);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final int partitions = 32;
      final int stride = 13;
      workspace.preparePlan(1, partitions, stride);
      final long budget = GroupTableSpill.groupBudget(stride, workspace);
      for (final boolean bounded : new boolean[] {false, true}) {
        for (final boolean completed : new boolean[] {false, true}) {
          final ProjectionIndexRegistry.Handle handle =
              new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
          if (completed) {
            handle.noteCompletedGroupScan(17L, 10_000L, 1);
          }
          final SirixVectorizedExecutor.GroupPasses plan =
              new SirixVectorizedExecutor.GroupPasses(handle, 17L, budget, partitions, stride, bounded, workspace);
          assertTrue(plan.passes() > 1, "this grant cannot hold all partition tails at once");
          final int active = GroupTableSpill.largestPassShare(partitions, plan.passes());
          assertTrue(plan.passBudget() * GroupTableSpill.bytesPerGroup(stride) <= workspace.usableGroupBytes(active));
          if (completed) {
            assertTrue(GroupTableSpill.expectedLargestPass(10_000L, plan.passes(), partitions) <= plan.passBudget());
          }
        }
      }
    } finally {
      restore(GroupTableSpill.WORKER_HINT_PROPERTY, previousHint);
      restore(GroupTableSpill.FLUSH_GROUPS_PROPERTY, previousFlush);
      GroupTableSpill.setSubChunkLeavesForTesting(previousBatch);
      allocator.shutdown();
    }
  }

  private static void restore(final String key, final String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }
}
