/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-group cost a grouped pass is planned against.
 *
 * <p>
 * A pass budget is a group COUNT, and it used to be derived by dividing a heap share by a flat 128
 * bytes whatever the query's stripe was. That figure is right for a stripe of eight lanes and wrong
 * for every other: {@code GROUP BY x ORDER BY count(*)} occupies THREE lanes, so it was charged
 * 2.67x what it costs and split into up to 2.67x the passes its memory required — and every surplus
 * pass is a full rescan of the table.
 * </p>
 *
 * <p>
 * The property these tests pin is the SAFETY of the correction, not merely its arithmetic: the
 * stride may only ever lower the charge. A wide stripe genuinely costs more than 128 bytes, and
 * charging it honestly would hand a working plan more passes than it takes today, so the honest
 * figure is clamped away for wide shapes and applied only where it helps.
 * </p>
 */
final class GroupStrideBudgetTest {

  private static final long GIB = 1L << 30;

  /** {@code min(maxMemory/8, headroom/4)} — the share both forms divide. */
  private static final long SHARE = 8L * GIB / 8L;

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
  @DisplayName("the shapes that motivated this: count-only is three lanes, q32's composite is thirteen")
  void theTwoShapesThatMatter() {
    // GROUP BY <one column> ORDER BY count(*) DESC LIMIT 10 - the group-by family.
    assertEquals(3, NumericGroupAggTable.strideFor(0, false, 0));
    assertEquals(48L, GroupTableSpill.bytesPerGroup(3), "three lanes at the capacity slack");
    // GROUP BY WatchID, ClientIP with sum() and avg(): two aggregate columns, a two-lane identity.
    assertEquals(13, NumericGroupAggTable.strideFor(2, false, 2));
    assertEquals(128L, GroupTableSpill.bytesPerGroup(13), "wide stripes keep the flat charge");
  }

  @Test
  @DisplayName("the charge rises with the stride up to the flat figure and never past it")
  void theChargeIsClampedAtTheFlatFigure() {
    long previous = 0L;
    for (int stride = 1; stride <= 64; stride++) {
      final long charge = GroupTableSpill.bytesPerGroup(stride);
      assertTrue(charge >= previous, "monotone in the stride at " + stride);
      assertTrue(charge <= 128L, "never above the flat figure at " + stride);
      assertTrue(charge > 0L, "positive at " + stride);
      previous = charge;
    }
    assertEquals(128L, GroupTableSpill.bytesPerGroup(8), "eight lanes is exactly the flat figure");
    assertEquals(128L, GroupTableSpill.bytesPerGroup(9), "and past it the clamp holds");
    assertTrue(GroupTableSpill.bytesPerGroup(7) < 128L, "seven lanes is charged less");
  }

  @Test
  @DisplayName("THE SAFETY PROPERTY: no shape is ever charged more than it was, so none takes more passes")
  void noShapeEverGetsASmallerBudget() {
    final long flat = GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB);
    for (int stride = 1; stride <= 64; stride++) {
      final long strided = GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB, stride);
      assertTrue(strided >= flat, "stride " + stride + " planned " + strided + " groups against the flat " + flat
          + ": a shape " + "charged MORE than before would take more passes than it takes today");
    }
  }

  @Test
  @DisplayName("a narrow stripe buys the passes back: three lanes plan 2.67x the groups")
  void aNarrowStripePlansMoreGroups() {
    final long flat = GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB);
    final long narrow = GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB, 3);
    assertEquals(SHARE / 128L, flat, "the flat form is unchanged");
    assertEquals(SHARE / 48L, narrow, "three lanes at the slack is 48 bytes");
    // The ratio IS the byte ratio, 128/48 = 8/3, and it is the pass ratio: the same group count
    // divided by 8/3 the groups per pass needs 8/3 the fewer passes.
    // Exact but for the two floor divisions, which can each lose one group.
    assertTrue(Math.abs(8L * flat - 3L * narrow) <= 8L,
        "the budget ratio is the charge ratio 128/48: 8 x " + flat + " against 3 x " + narrow);
    assertTrue(narrow > 2L * flat, "at least twice the groups: " + narrow + " vs " + flat);
  }

  @Test
  @DisplayName("the wide shapes keep the numbers they were measured with, to the group")
  void wideShapesAreUntouched() {
    for (int stride = 8; stride <= 64; stride++) {
      assertEquals(GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB, stride),
          GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB), "stride " + stride + " must be the old plan");
    }
  }

  @Test
  @DisplayName("the floor and the cap still bind, whatever the stride")
  void theFloorAndCapStillBind() {
    assertEquals(1L << 20, GroupTableSpill.groupBudgetFor(8L * GIB, 0L, 3), "no headroom still floors at 2^20");
    assertEquals(1L << 26, GroupTableSpill.groupBudgetFor(1L << 40, 1L << 40, 3), "a huge heap still caps at 2^26");
    assertEquals(1L << 26, GroupTableSpill.groupBudgetFor(1L << 40, 1L << 40), "and the flat form caps identically");
  }

  @Test
  @DisplayName("the kill switch restores the flat charge for every stride")
  void theKillSwitchRestoresTheFlatCharge() {
    final String previous = System.setProperty(GroupTableSpill.STRIDE_BUDGET_PROPERTY, "false");
    try {
      for (int stride = 1; stride <= 64; stride++) {
        assertEquals(128L, GroupTableSpill.bytesPerGroup(stride), "stride " + stride + " charged flat");
        assertEquals(GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB),
            GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB, stride), "stride " + stride + " budgets flat");
      }
    } finally {
      if (previous == null) {
        System.clearProperty(GroupTableSpill.STRIDE_BUDGET_PROPERTY);
      } else {
        System.setProperty(GroupTableSpill.STRIDE_BUDGET_PROPERTY, previous);
      }
    }
    assertEquals(48L, GroupTableSpill.bytesPerGroup(3), "and the switch is restored");
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
