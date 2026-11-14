package io.sirix.index.projection;

import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NumericGroupSumTableTest {

  @Test
  void compactStripesPreserveCountsSumsIdentityAndSourceAcrossGrowth() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = NumericGroupAggTable.sumsOnly(2, 16, true, 1L, 2).attachWorkspace(workspace);
      assertEquals(10, table.stride());
      assertEquals(6, table.slotWidth());
      final GroupBuffer stripe = workspace.newBlock(table.stride());
      for (int round = 0; round < 2; round++) {
        for (int group = 1; group <= 6_000; group++) {
          GroupLanes.set(stripe, 0, group % 127 + 1);
          GroupLanes.set(stripe, 1, 1);
          GroupLanes.set(stripe, 2, 10 - round);
          GroupLanes.set(stripe, 3, round == 0 || group % 2 == 0
              ? 1
              : 0);
          GroupLanes.set(stripe, 4, round == 0
              ? group
              : group % 2 == 0
                  ? -group
                  : 0);
          GroupLanes.set(stripe, 5, group % 3 == 0
              ? 1
              : 0);
          GroupLanes.set(stripe, 6, 0); // An unread sum is never accumulated.
          GroupLanes.set(stripe, 7, 100 + round);
          GroupLanes.set(stripe, 8, group);
          GroupLanes.set(stripe, 9, ~group);
          table.mergeStripes(stripe, 0, GroupLanes.count(stripe));
        }
      }
      assertEquals(6_000, table.size());
      assertTrue(table.rehashes() > 0);
      assertTrue(table.hasProbeKeyCollision());
      final long[] identity = new long[2];
      for (int group = 1; group <= 6_000; group++) {
        identity[0] = group;
        identity[1] = ~group;
        final int handle = table.acquireExact(group % 127 + 1, 0L, identity, 0);
        // A winner is copied out of its table before it is expanded: the expansion is heap to heap.
        final long[] compact =
            GroupLanes.toArray(table.storageAtAccBase(handle), table.offsetAtAccBase(handle), table.slotWidth());
        final long[] expanded = NumericGroupAggTable.expandSumsAccumulator(compact, 0, 2);
        assertEquals(2L, expanded[0]);
        assertEquals(9L, expanded[1]);
        assertEquals(group % 2 == 0
            ? 2L
            : 1L, expanded[2]);
        assertEquals(group % 2 == 0
            ? 0L
            : group, expanded[3]);
        assertEquals(group % 3 == 0
            ? 2L
            : 0L, expanded[6]);
        assertEquals(0L, expanded[7]);
        assertEquals(100L, table.auxAtAccBase(handle));
      }
    }
  }

  @Test
  void compactMergeChecksExactOverflowAndRejectsAnOrdinaryLayout() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = NumericGroupAggTable.sumsOnly(1, 16, false, 1L, 0).attachWorkspace(workspace);
      final GroupBuffer first = lanes(workspace, 7L, 1L, 2L, 1L, Long.MAX_VALUE);
      final GroupBuffer second = lanes(workspace, 7L, 1L, 1L, 1L, 1L);
      table.mergeStripes(first, 0, GroupLanes.count(first));
      final int secondLanes = GroupLanes.count(second);
      assertThrows(ArithmeticException.class, () -> table.mergeStripes(second, 0, secondLanes));
      // Equal slot widths alone do not make ordinary and compact blocks compatible.
      final NumericGroupAggTable compact =
          NumericGroupAggTable.sumsOnly(2, 16, false, 1L, 0).attachWorkspace(workspace);
      final NumericGroupAggTable ordinary = new NumericGroupAggTable(1, 16, false, 1L).attachWorkspace(workspace);
      assertEquals(compact.slotWidth(), ordinary.slotWidth());
      final IllegalStateException refused = assertThrows(IllegalStateException.class,
          () -> NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {ordinary}, 0, 64, compact));
      // The layout flag is the ONLY difference here, so a diagnostic that omits it names no cause at all.
      assertTrue(refused.getMessage().contains("sumsOnly false/true"),
          "the refusal must name the mismatch: " + refused.getMessage());
    }
  }

  /**
   * A block of {@code workspace} holding {@code values}: a stripe run in the memory the tables merge
   * from.
   */
  private static GroupBuffer lanes(final GroupWorkspace workspace, final long... values) {
    final GroupBuffer block = workspace.newBlock(values.length);
    GroupLanes.copy(values, 0, block, 0, values.length);
    return block;
  }
}
