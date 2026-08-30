package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-pass group budget and the grouped COUNT(DISTINCT) ceiling plan against the heap HEADROOM
 * (maximum heap minus what was live after the last collection), not the maximum heap alone: a query
 * late in a leg, after earlier queries retained fills, fingerprint chains and payload windows, must
 * split into more passes instead of dying in a worker (q32 at 100M/8 GB, second try inside a leg).
 */
final class HeapHeadroomBudgetTest {
  private static final long GIB = 1L << 30;

  @Test
  @DisplayName("the group budget is the smaller of an eighth of the heap and a quarter of the headroom")
  void groupBudgetFollowsTheHeadroom() {
    final long eighthOfHeap = 8L * GIB / 8L / 128L; // 8 GB heap: 8,388,608 groups
    assertEquals(eighthOfHeap, GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB), "an empty heap plans an eighth");
    // 5.9 GB live of 8 GB: 2.1 GB headroom, a quarter of it at 128 B per group
    final long headroom = 8L * GIB - (long) (5.9 * GIB);
    assertEquals(headroom / 4L / 128L, GroupTableSpill.groupBudgetFor(8L * GIB, headroom),
        "a leg-late query plans against what is left");
    assertTrue(GroupTableSpill.groupBudgetFor(8L * GIB, headroom) < eighthOfHeap);
    assertEquals(1L << 20, GroupTableSpill.groupBudgetFor(8L * GIB, 0L), "the floor holds with no headroom");
    assertEquals(1L << 26, GroupTableSpill.groupBudgetFor(1L << 40, 1L << 40), "the cap holds on a huge heap");
  }

  @Test
  @DisplayName("the grouped COUNT(DISTINCT) ceiling follows the headroom the same way")
  void distinctCeilingFollowsTheHeadroom() {
    final long eighthOfHeap = GroupDistinctAccumulator.defaultMaxValuesFor(8L * GIB, 8L * GIB);
    final long headroom = 8L * GIB - (long) (5.9 * GIB);
    final long late = GroupDistinctAccumulator.defaultMaxValuesFor(8L * GIB, headroom);
    assertTrue(late < eighthOfHeap, "less headroom, lower ceiling");
    assertTrue(late >= 1L << 24, "the floor holds");
  }

  @Test
  @DisplayName("one shared headroom figure feeds every derived budget")
  void oneSharedFigureFeedsEveryBudget() {
    // Two different (maxMemory, headroom) pairs whose SHARE is identical must yield identical
    // budgets. That is the property a second copy of min(maxMemory/8, headroom/4) in a consumer
    // would break, and the reason the arithmetic lives in exactly one place: the group table, the
    // grouped COUNT(DISTINCT) ceiling and the column store's retained-fill budget compete for one
    // heap, so a raised share must raise all three together.
    assertEquals(HeapHeadroom.plannedShareBytes(8L * GIB, 8L * GIB),
        HeapHeadroom.plannedShareBytes(16L * GIB, 4L * GIB), "the share is min(maxMemory / 8, headroom / 4)");
    assertEquals(GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB),
        GroupTableSpill.groupBudgetFor(16L * GIB, 4L * GIB),
        "the per-pass group budget is a function of the share alone");
    assertEquals(GroupDistinctAccumulator.defaultMaxValuesFor(8L * GIB, 8L * GIB),
        GroupDistinctAccumulator.defaultMaxValuesFor(16L * GIB, 4L * GIB),
        "so is the grouped COUNT(DISTINCT) ceiling");
    assertEquals(0L, HeapHeadroom.plannedShareBytes(8L * GIB, 0L), "no headroom, no share");
    // Monotone in the headroom — the link R1 shortens: a query-scope exit that releases retained
    // fills raises the headroom, which raises the share, which raises the per-pass group budget and
    // therefore lowers the number of hash-range passes. (End to end the drop needs a group state
    // above the 2^20-group floor, i.e. the 100M leg; the budget -> pass-count half of the chain is
    // GroupHashRangePassTest's.)
    assertTrue(GroupTableSpill.groupBudgetFor(8L * GIB, 2L * GIB) <= GroupTableSpill.groupBudgetFor(8L * GIB, 8L * GIB),
        "more headroom must never plan FEWER groups per pass");
  }

  @Test
  @DisplayName("the residency budget is that same share, capped by the static fill budget")
  void theResidencyBudgetFollowsTheSameShare() {
    final long priorHeadroom = HeapHeadroom.setHeadroomForTesting(-1L);
    final boolean priorResidency = ProjectionColumnStore.setResidencyHeadroomForTesting(true);
    final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE >> 1);
    try {
      HeapHeadroom.setHeadroomForTesting(4L * GIB);
      final long share = ProjectionColumnStore.sampleHeadroomShare();
      assertEquals(HeapHeadroom.plannedShareBytes(), share, "the store samples the shared figure, not its own");
      assertEquals(share, ProjectionColumnStore.residencyBudgetBytes(),
          "below the static budget the share IS the residency budget");

      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(share / 2);
      assertEquals(share / 2, ProjectionColumnStore.residencyBudgetBytes(), "the static budget still caps it");

      HeapHeadroom.setHeadroomForTesting(0L);
      ProjectionColumnStore.sampleHeadroomShare();
      assertEquals(0L, ProjectionColumnStore.residencyBudgetBytes(),
          "a heap with no headroom retains nothing — the windowed lanes serve");

      ProjectionColumnStore.setResidencyHeadroomForTesting(false);
      assertEquals(share / 2, ProjectionColumnStore.residencyBudgetBytes(),
          "the kill switch restores the static per-store budget");
    } finally {
      ProjectionColumnStore.setResidencyHeadroomForTesting(priorResidency);
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
      HeapHeadroom.setHeadroomForTesting(priorHeadroom);
      ProjectionColumnStore.sampleHeadroomShare();
    }
  }

  @Test
  @DisplayName("the derived budgets read the headroom seam")
  void derivedBudgetsReadTheHeadroom() {
    final long priorBudget = GroupTableSpill.setGroupBudgetForTesting(-1L);
    final long priorHeadroom = HeapHeadroom.setHeadroomForTesting(-1L);
    try {
      final long maxMemory = Runtime.getRuntime().maxMemory();
      HeapHeadroom.setHeadroomForTesting(maxMemory);
      final long roomy = GroupTableSpill.groupBudget();
      HeapHeadroom.setHeadroomForTesting(0L);
      final long starved = GroupTableSpill.groupBudget();
      assertTrue(starved <= roomy, "no headroom must never plan MORE groups per pass");
      assertEquals(GroupTableSpill.groupBudgetFor(maxMemory, 0L), starved);
      assertEquals(GroupTableSpill.groupBudgetFor(maxMemory, maxMemory), roomy);
      assertTrue(HeapHeadroom.liveAfterLastGc() >= 0L);
    } finally {
      HeapHeadroom.setHeadroomForTesting(priorHeadroom);
      GroupTableSpill.setGroupBudgetForTesting(priorBudget);
    }
  }
}
