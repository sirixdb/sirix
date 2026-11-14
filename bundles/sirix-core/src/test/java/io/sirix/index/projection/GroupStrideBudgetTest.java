/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Full aggregation footprints, including wide records and growth/merge peaks. */
final class GroupStrideBudgetTest {

  private static final long GIB = 1L << 30;

  @Test
  @DisplayName("strideFor is the stride the table actually builds, for every shape the planner names")
  void strideForMatchesTheTable() {
    for (int aggColumns = 0; aggColumns <= 6; aggColumns++) {
      for (final boolean withAux : new boolean[] {false, true}) {
        for (int idWidth = 0; idWidth <= 4; idWidth++) {
          final NumericGroupAggTable table = new NumericGroupAggTable(aggColumns, 64, withAux, -1L, idWidth);
          assertEquals(table.stride(), NumericGroupAggTable.strideFor(aggColumns, withAux, idWidth),
              "aggColumns=" + aggColumns + " withAux=" + withAux + " idWidth=" + idWidth);
        }
      }
    }
  }

  @Test
  void everyWidthPaysForItsFullRecordAndPeakIndexFootprint() {
    long previous = 0;
    for (int stride = 1; stride <= 16_384; stride++) {
      final long charge = GroupTableSpill.bytesPerGroup(stride);
      assertTrue(charge >= 2L * ((long) stride + 3) * Long.BYTES);
      if (stride > GroupTableSpill.DENSE_INDEX_MIN_STRIDE) {
        assertTrue(charge >= previous, "within one layout, a wider stripe never costs less");
      }
      final long groups = GroupTableSpill.groupBudgetFor(8L * GIB, GIB, stride);
      assertTrue(groups * charge <= GIB, "planned bytes fit for stride " + stride);
      previous = charge;
    }
  }

  @Test
  void wideStripesCannotBeUnderchargedByTheFormerKillSwitch() {
    final String previous = System.setProperty(GroupTableSpill.STRIDE_BUDGET_PROPERTY, "false");
    try {
      assertEquals(256L, GroupTableSpill.bytesPerGroup(13));
      assertTrue(GroupTableSpill.bytesPerGroup(64) >= 64L * Long.BYTES);
    } finally {
      if (previous == null) {
        System.clearProperty(GroupTableSpill.STRIDE_BUDGET_PROPERTY);
      } else {
        System.setProperty(GroupTableSpill.STRIDE_BUDGET_PROPERTY, previous);
      }
    }
  }

  @Test
  void interleavedRehashChargesOldNewAndStillLiveSourceStripes() {
    final String previous = System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, "false");
    try {
      for (final int stride : new int[] {3, 4, 13, 64}) {
        for (int capacity = 16; capacity <= 1 << 20; capacity <<= 1) {
          final long groupsAtGrowth = capacity * 3L / 4 + 1;
          final long peakLanes = (capacity + capacity * 2L + groupsAtGrowth) * stride;
          assertTrue(peakLanes * Long.BYTES <= groupsAtGrowth * GroupTableSpill.bytesPerGroup(stride));
        }
      }
    } finally {
      if (previous == null) {
        System.clearProperty(GroupTableSpill.DENSE_INDEX_PROPERTY);
      } else {
        System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, previous);
      }
    }
  }

  @Test
  void unavailableCapacityCannotManufactureAMillionGroups() {
    assertEquals(0L, GroupTableSpill.groupBudgetFor(8L * GIB, 0L, 3));
    assertEquals(0L, GroupTableSpill.groupBudgetFor(8L * GIB, 119L, 3));
    assertEquals(1L, GroupTableSpill.groupBudgetFor(8L * GIB, 120L, 3));
    assertEquals(1L << 26, GroupTableSpill.groupBudgetFor(1L << 40, 1L << 40, 3));
  }

  @Test
  @DisplayName("a nonsensical stride is refused rather than dividing by zero")
  void anInvalidStrideIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> GroupTableSpill.bytesPerGroup(0));
    assertThrows(IllegalArgumentException.class, () -> GroupTableSpill.bytesPerGroup(-1));
    assertThrows(IllegalArgumentException.class, () -> NumericGroupAggTable.strideFor(-1, false, 0));
    assertThrows(IllegalArgumentException.class, () -> NumericGroupAggTable.strideFor(0, false, -1));
  }
}
