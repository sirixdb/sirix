package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BoundedGroupBudgetTest {
  private static final long MIB = 1L << 20;
  private static final long GIB = 1L << 30;

  @Test
  void denseRecordsFitFewerPassesWithoutRaisingOtherConsumers() {
    final long shared = HeapHeadroom.plannedShareBytes(14 * GIB, 7607 * MIB);
    final long distinct = GroupDistinctAccumulator.defaultMaxValuesFor(14 * GIB, 7607 * MIB);
    assertEquals(80, GroupTableSpill.boundedBytesPerGroup(7));
    assertEquals(88, GroupTableSpill.boundedBytesPerGroup(8));
    assertEquals(112, GroupTableSpill.boundedBytesPerGroup(11));
    final long large = GroupTableSpill.boundedGroupBudgetFor(14 * GIB, 7607 * MIB, 11);
    assertEquals(2, GroupTableSpill.passesFor(100_007_737L, large, 1024));
    assertEquals(1,
        GroupTableSpill.passesFor(56_384_822L, GroupTableSpill.boundedGroupBudgetFor(14 * GIB, 7245 * MIB, 8), 1024));
    assertEquals(1,
        GroupTableSpill.passesFor(24_555_637L, GroupTableSpill.boundedGroupBudgetFor(14 * GIB, 5841 * MIB, 7), 1024));
    assertEquals(shared, HeapHeadroom.plannedShareBytes(14 * GIB, 7607 * MIB));
    assertEquals(distinct, GroupDistinctAccumulator.defaultMaxValuesFor(14 * GIB, 7607 * MIB));
    assertEquals(7,
        GroupTableSpill.passesFor(100_007_737L, GroupTableSpill.groupBudgetFor(14 * GIB, 7607 * MIB, 11), 1024));
  }

  @Test
  void pressureKeepsAReserveAndForcesMorePasses() {
    long previous = 0;
    for (long available = GIB; available <= 14 * GIB; available += GIB / 4) {
      final long budget = GroupTableSpill.boundedGroupBudgetFor(14 * GIB, available, 11);
      assertTrue(budget >= previous, "more headroom cannot reduce the allowance");
      assertTrue(budget * 112 <= available * 3 / 4, "one quarter remains outside the allowance");
      assertTrue(budget * 112 <= 7 * GIB, "half-heap ceiling");
      previous = budget;
    }
    final long tight = GroupTableSpill.boundedGroupBudgetFor(14 * GIB, GIB, 11);
    assertTrue(GroupTableSpill.passesFor(100_007_737L, tight, 1024) > 2);
    assertEquals(1L << 20, GroupTableSpill.boundedGroupBudgetFor(14 * GIB, 0, 11));
    assertEquals(1L << 26, GroupTableSpill.boundedGroupBudgetFor(Long.MAX_VALUE, Long.MAX_VALUE, 11));
    assertThrows(IllegalArgumentException.class, () -> GroupTableSpill.boundedGroupBudgetFor(-1, 0, 11));
    assertThrows(IllegalArgumentException.class, () -> GroupTableSpill.boundedGroupBudgetFor(GIB, -1, 11));
    assertThrows(IllegalArgumentException.class, () -> GroupTableSpill.boundedGroupBudgetFor(GIB, GIB, 0));
  }

  @Test
  void unsupportedLayoutsAndOperatorOverridesKeepTheirBudget() {
    for (int stride = 3; stride < GroupTableSpill.DENSE_INDEX_MIN_STRIDE; stride++) {
      assertEquals(GroupTableSpill.groupBudgetFor(14 * GIB, 7 * GIB, stride),
          GroupTableSpill.boundedGroupBudgetFor(14 * GIB, 7 * GIB, stride));
    }
    final int oldSpill = GroupTableSpill.setStripeSpillForTesting(0);
    try {
      assertEquals(GroupTableSpill.groupBudgetFor(14 * GIB, 7 * GIB, 11),
          GroupTableSpill.boundedGroupBudgetFor(14 * GIB, 7 * GIB, 11));
    } finally {
      GroupTableSpill.setStripeSpillForTesting(oldSpill);
    }
    final long oldBudget = GroupTableSpill.setGroupBudgetForTesting(31);
    try {
      assertEquals(31, GroupTableSpill.boundedGroupBudget(11));
      assertEquals(31, GroupTableSpill.boundedGroupBudgetCeiling(11));
    } finally {
      GroupTableSpill.setGroupBudgetForTesting(oldBudget);
    }
  }
}
