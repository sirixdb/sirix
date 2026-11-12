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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GroupPassGrantCapacityTest {
  @Test
  void aPlanStartsWithOnePassAndEveryPassFitsTheActualGrant() {
    final String previousHint = System.setProperty(GroupTableSpill.WORKER_HINT_PROPERTY, "128");
    final String previousFlush = System.setProperty(GroupTableSpill.FLUSH_GROUPS_PROPERTY, "128");
    final int previousBatch = GroupTableSpill.setSubChunkLeavesForTesting(1);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final int partitions = 32;
      final int stride = 13;
      workspace.preparePlan(1, partitions, stride);
      final long budget = GroupTableSpill.groupBudget(stride, workspace);
      // The grant is smaller than the full chunk tails of the split: what one pass holds is bounded by
      // the partitions its groups fill, not by a tail reserved for every partition beforehand.
      assertEquals(0L, workspace.usableGroupBytes(partitions), "this grant cannot hold all partition tails at once");
      final long onePass = workspace.usableGroups(partitions);
      assertTrue(onePass > partitions, "one pass over the whole split holds groups: " + onePass);
      for (final boolean bounded : new boolean[] {false, true}) {
        // A blind plan, a count one pass holds, and a count it cannot hold.
        for (final long completedGroups : new long[] {0L, onePass / 2, onePass * 4}) {
          final ProjectionIndexRegistry.Handle handle =
              new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
          if (completedGroups > 0L) {
            handle.noteCompletedGroupScan(17L, completedGroups, 1);
          }
          final SirixVectorizedExecutor.GroupPasses plan =
              new SirixVectorizedExecutor.GroupPasses(handle, 17L, budget, partitions, stride, bounded, workspace);
          final String shape = "bounded=" + bounded + " groups=" + completedGroups + " passes=" + plan.passes();
          final int active = GroupTableSpill.largestPassShare(partitions, plan.passes());
          assertTrue(plan.passBudget() > 0L && plan.passBudget() <= workspace.usableGroups(active),
              "a pass may hold only what its grant holds: " + shape);
          if (completedGroups == 0L) {
            assertEquals(1, plan.passes(), "a plan that knows no count scans its input once: " + shape);
          } else {
            final long largest = GroupTableSpill.expectedLargestPass(completedGroups, plan.passes(), partitions);
            assertTrue(largest <= plan.passBudget(), "the largest pass holds its share of the count: " + shape);
          }
          if (completedGroups > onePass) {
            assertTrue(plan.passes() > 1, "more groups than one pass holds take more passes: " + shape);
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
