/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The share of the allocator one grouped scan plans its groups against: the rule, and the figures
 * it is fed. Group tables live in allocator memory, so the per-pass group budget derives from the
 * allocator's budget and headroom and no longer from the heap's.
 */
@Isolated
final class AggregationShareTest {

  private static final long GIB = 1L << 30;
  private static final long MIB = 1L << 20;

  @BeforeAll
  static void initializeAllocator() {
    assertTrue(Allocators.getInstance() instanceof FrameSlotAllocator);
    Allocators.getInstance().init(8 * GIB);
  }

  /** The headroom test reads the process allocator: let dropped workspaces go back first. */
  @BeforeEach
  void settle() throws InterruptedException {
    GroupWorkspaceTest.letDroppedWorkspacesReturn();
  }

  @AfterEach
  void restore() {
    AggregationShare.setBudgetForTesting(-1L);
    AggregationShare.setHeadroomForTesting(-1L);
    GroupTableSpill.setGroupBudgetForTesting(-1L);
  }

  @Test
  void theExecutionFloorIsUsedWithoutDividingHeadroomAgain() {
    assertEquals(8, AggregationShare.BUDGET_DIVISOR);
    assertEquals(2 * GIB, AggregationShare.plannedBytes(16 * GIB, 16 * GIB));
    assertEquals(2 * GIB, AggregationShare.plannedBytes(16 * GIB, 2 * GIB));
    assertEquals(GIB, AggregationShare.plannedBytes(16 * GIB, GIB));
    assertEquals(0L, AggregationShare.plannedBytes(16 * GIB, 0L));
    assertEquals(0L, AggregationShare.plannedBytes(0L, 16 * GIB));
    assertEquals(0L, AggregationShare.plannedBytes(16 * GIB, -1L));
  }

  @Test
  void boundedTopKCannotSpendBeyondItsExclusiveExecutionAllowance() {
    for (long headroom = 0; headroom <= 16 * GIB; headroom += MIB * 127) {
      assertEquals(AggregationShare.plannedBytes(16 * GIB, headroom),
          AggregationShare.plannedBoundedBytes(16 * GIB, headroom));
    }
    assertEquals(0L, AggregationShare.plannedBoundedBytes(16 * GIB, -5L));
  }

  @Test
  void theGroupBudgetIsTheShareAtTheBytesAGroupCosts() {
    final int stride = GroupTableSpill.WIDEST_CHARGED_STRIDE;
    final long perGroup = GroupTableSpill.bytesPerGroup(stride);
    AggregationShare.setBudgetForTesting(16 * GIB);
    AggregationShare.setHeadroomForTesting(4 * GIB);
    assertEquals(2 * GIB / perGroup, GroupTableSpill.groupBudget(stride));
    assertEquals(GroupTableSpill.groupBudgetFor(16 * GIB, 4 * GIB, stride), GroupTableSpill.groupBudget(stride));
    assertEquals(GroupTableSpill.groupBudgetFor(16 * GIB, 16 * GIB, stride), GroupTableSpill.groupBudgetCeiling(stride),
        "the ceiling is the budget of an uncommitted allocator");

    // The floor and the cap of the planner still bound the figure.
    AggregationShare.setHeadroomForTesting(0L);
    assertEquals(0L, GroupTableSpill.groupBudget(stride), "no group floor can invent capacity");
    AggregationShare.setBudgetForTesting(1L << 50);
    AggregationShare.setHeadroomForTesting(1L << 50);
    assertEquals(1L << 26, GroupTableSpill.groupBudget(stride), "and never more than the cap");

    // A narrower stripe is charged less per group and so admits more groups from the same share.
    AggregationShare.setBudgetForTesting(16 * GIB);
    AggregationShare.setHeadroomForTesting(GIB);
    assertTrue(GroupTableSpill.groupBudget(3) >= GroupTableSpill.groupBudget(stride));
  }

  @Test
  void headroomIsWhatTheAllocatorHasNotCommittedPlusWhatTheScanItselfHolds() {
    final long budget = AggregationShare.budgetBytes();
    assertEquals(Allocators.getInstance().getMaxBufferSize(), budget);
    final long idle = AggregationShare.headroomBytes(null);
    assertTrue(idle <= budget, "never more than the budget");
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      // Four megabytes of chunks: the regions are committed now, and they are this scan's own.
      for (int i = 0; i < 32; i++) {
        workspace.takeChunk(1 << 14);
      }
      final long held = workspace.borrowedBytes();
      assertTrue(held >= 4 * MIB);
      final long othersSee = AggregationShare.headroomBytes(null);
      final long scanSees = AggregationShare.headroomBytes(workspace);
      assertEquals(Math.min(budget, othersSee + held), scanSees,
          "the scan counts its own workspace: its next pass is built from the chunks the last one handed back");
      assertTrue(othersSee <= idle, "to every other scan the regions are committed");
    }
  }
}
