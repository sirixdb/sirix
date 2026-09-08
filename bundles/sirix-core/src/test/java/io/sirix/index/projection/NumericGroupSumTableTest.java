package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NumericGroupSumTableTest {

  @Test
  void compactStripesPreserveCountsSumsIdentityAndSourceAcrossGrowth() {
    final NumericGroupAggTable table = NumericGroupAggTable.sumsOnly(2, 16, true, 1L, 2);
    assertEquals(10, table.stride());
    assertEquals(6, table.slotWidth());
    final long[] stripe = new long[table.stride()];
    for (int round = 0; round < 2; round++) {
      for (int group = 1; group <= 6_000; group++) {
        stripe[0] = group % 127 + 1;
        stripe[1] = 1;
        stripe[2] = 10 - round;
        stripe[3] = round == 0 || group % 2 == 0
            ? 1
            : 0;
        stripe[4] = round == 0
            ? group
            : group % 2 == 0
                ? -group
                : 0;
        stripe[5] = group % 3 == 0
            ? 1
            : 0;
        stripe[6] = 0; // An unread sum is never accumulated.
        stripe[7] = 100 + round;
        stripe[8] = group;
        stripe[9] = ~group;
        table.mergeStripes(stripe, 0, stripe.length);
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
      final long[] expanded =
          NumericGroupAggTable.expandSumsAccumulator(table.storageAtAccBase(handle), table.offsetAtAccBase(handle), 2);
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

  @Test
  void compactMergeChecksExactOverflowAndRejectsAnOrdinaryLayout() {
    final NumericGroupAggTable table = NumericGroupAggTable.sumsOnly(1, 16, false, 1L, 0);
    final long[] first = {7L, 1L, 2L, 1L, Long.MAX_VALUE};
    final long[] second = {7L, 1L, 1L, 1L, 1L};
    table.mergeStripes(first, 0, first.length);
    assertThrows(ArithmeticException.class, () -> table.mergeStripes(second, 0, second.length));
    // Equal slot widths alone do not make ordinary and compact blocks compatible.
    final NumericGroupAggTable compact = NumericGroupAggTable.sumsOnly(2, 16, false, 1L, 0);
    final NumericGroupAggTable ordinary = new NumericGroupAggTable(1, 16, false, 1L);
    assertEquals(compact.slotWidth(), ordinary.slotWidth());
    assertThrows(IllegalStateException.class,
        () -> NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {ordinary}, 0, 64, compact));
  }
}
