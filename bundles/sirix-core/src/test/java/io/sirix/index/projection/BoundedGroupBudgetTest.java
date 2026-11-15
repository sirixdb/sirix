package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BoundedGroupBudgetTest {
  private static final long GIB = 1L << 30;

  @Test
  void boundedAndOrdinaryPlansRespectTheSameByteCapacityForEveryWidth() {
    for (int stride = 3; stride <= 64; stride++) {
      for (long available = 0; available <= 14 * GIB; available += GIB / 4) {
        final long groups = GroupTableSpill.boundedGroupBudgetFor(14 * GIB, available, stride);
        assertEquals(GroupTableSpill.groupBudgetFor(14 * GIB, available, stride), groups);
        assertTrue(groups * GroupTableSpill.boundedBytesPerGroup(stride) <= Math.min(available, 14 * GIB / 8));
      }
    }
    assertEquals(0L, GroupTableSpill.boundedGroupBudgetFor(14 * GIB, 0, 11));
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
