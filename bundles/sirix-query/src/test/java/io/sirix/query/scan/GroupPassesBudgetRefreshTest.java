package io.sirix.query.scan;

import io.sirix.index.projection.GroupTableSpill;
import io.sirix.index.projection.NumericGroupAggTable;
import io.sirix.index.projection.ProjectionIndexRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The need-based budget refresh of a hash-range pass plan: a plan asks the collector for a fresh
 * budget only when the ceiling a clean heap yields would plan fewer passes, once per plan, and
 * re-plans on the fit alone when the refresh widened the budget. The collector itself is behind a
 * seam ({@link SirixVectorizedExecutor.GroupPasses#setBudgetRefreshForTesting}): a forced
 * collection inside a unit test would measure the test JVM, not the plan.
 */
final class GroupPassesBudgetRefreshTest {

  private static final int PARTITIONS = 32;
  private static final int SHIFT = 59;
  private static final long FINGERPRINT = 0x5EEDL;

  @AfterEach
  void restoreTheCollector() {
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(-1L, null);
  }

  @Test
  void boundedPlanRecoversFromAnUnderestimateWithoutWideningTheAllowance() {
    final long budget = GroupTableSpill.boundedGroupBudgetFor(14L << 30, 1L << 30, 11);
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(budget, () -> budget);
    final ProjectionIndexRegistry.Handle handle = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    handle.noteCompletedGroupScan(FINGERPRINT, 1_000L, 1);
    final SirixVectorizedExecutor.GroupPasses plan =
        new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, budget, 1024, 11, true);
    assertEquals(1, plan.passes(), "the stale small estimate initially fits");
    final GroupTableSpill spill = new GroupTableSpill(1024, 54, () -> new NumericGroupAggTable(2, 16), 0, 1024, budget);
    spill.noteLeavesScanned(10);
    spill.noteAbandonedLocal(10_000_000);
    plan.restart(spill, 100);
    assertEquals(100_000_000L, plan.plannedGroups());
    assertEquals(budget, plan.budget());
    assertEquals(budget, plan.passBudget());
    assertTrue(plan.passes() > 2, "tight headroom must increase passes after an underestimated plan");
    assertTrue(GroupTableSpill.expectedLargestPass(plan.plannedGroups(), plan.passes(), 1024) <= budget);
    assertFalse(plan.seededCompleted(), "the failed completed-plan seed must be discarded");
    spill.releaseTables();
  }

  @Test
  @DisplayName("a bounded plan refuses to replay a completed pass count the current headroom cannot hold")
  void boundedPlanRefusesAReplayedPassCountPastTheCurrentBudget() {
    // Execution A, q32 at 100M: 7,607 MiB of headroom plans 53.4M groups per pass; the memoed estimate
    // seeds two balanced passes and the scan completes them, memoing (100,007,737, 2).
    final long wide = GroupTableSpill.boundedGroupBudgetFor(14L << 30, 7607L << 20, 11);
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(wide, () -> wide);
    final ProjectionIndexRegistry.Handle handle = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    handle.noteObservedGroups(FINGERPRINT, 100_007_737L);
    final SirixVectorizedExecutor.GroupPasses first =
        new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, wide, 1024, 11, true);
    assertEquals(2, first.passes());
    assertFalse(first.seededCompleted());
    for (int partition = 0; partition < 1024; partition++) {
      first.notePartition(partition, partition < 825
          ? 97_664
          : 97_663);
    }
    first.complete();
    final ProjectionIndexRegistry.Handle.CompletedGroupScan memo = handle.completedGroupScanFor(FINGERPRINT);
    assertEquals(100_007_737L, memo.groups());
    assertEquals(2, memo.passes());
    // Execution B: live state grew, 4 GiB of headroom plans 28.8M groups per pass and a collection
    // cannot widen it. Two passes of 50M would charge 5.5 GiB of tables into 4 GiB; the count implies
    // four, and four it is, at a pass budget inside the three-quarter allowance.
    final long tight = GroupTableSpill.boundedGroupBudgetFor(14L << 30, 4L << 30, 11);
    assertTrue(GroupTableSpill.expectedLargestPass(memo.groups(), memo.passes(), 1024) > tight);
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(tight, () -> tight);
    final SirixVectorizedExecutor.GroupPasses second =
        new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, tight, 1024, 11, true);
    assertTrue(second.seededCompleted());
    assertEquals(4, second.passes());
    assertEquals(tight, second.passBudget());
    assertTrue(GroupTableSpill.boundedBytesPerGroup(11) * second.passBudget() <= (4L << 30) / 4 * 3);
    assertEquals(100_007_737L, second.plannedGroups());
    // The shared quarter share keeps its calibrated replay: the same memo at the same budget replays
    // the two passes because 50M stays within twice 28.8M.
    final SirixVectorizedExecutor.GroupPasses unbounded =
        new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, tight, 1024, 11, false);
    assertEquals(2, unbounded.passes());
    // At the headroom the paired study measured, both policies plan the same two passes.
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(wide, () -> wide);
    assertEquals(2, new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, wide, 1024, 11, true).passes());
    assertEquals(2, new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, wide, 1024, 11, false).passes());
  }

  @Test
  @DisplayName("a refresh is worth a collection only when the clean-heap ceiling plans fewer passes")
  void refreshWorthItTruthTable() {
    // Nothing known about the shape: nothing to plan, nothing to refresh.
    assertFalse(SirixVectorizedExecutor.GroupPasses.refreshWorthIt(0L, 1_000L, 10_000L, PARTITIONS));
    // The heap is already clean: the budget reads the ceiling.
    assertFalse(SirixVectorizedExecutor.GroupPasses.refreshWorthIt(5_000L, 1_000L, 1_000L, PARTITIONS));
    assertFalse(SirixVectorizedExecutor.GroupPasses.refreshWorthIt(5_000L, 1_000L, 900L, PARTITIONS));
    // A shape that fits one pass at the collapsed budget already: one pass either way.
    assertFalse(SirixVectorizedExecutor.GroupPasses.refreshWorthIt(1_000L, 2_000L, 10_000L, PARTITIONS));
    // 5,000 groups at 1,000 plan six passes (six partitions hold 938); at 1,050 still six — no pass
    // saved.
    assertFalse(SirixVectorizedExecutor.GroupPasses.refreshWorthIt(5_000L, 1_000L, 1_050L, PARTITIONS));
    // At 1,100 seven partitions hold 1,094: five passes — one pass saved is worth a fifth of a second.
    assertTrue(SirixVectorizedExecutor.GroupPasses.refreshWorthIt(5_000L, 1_000L, 1_100L, PARTITIONS));
    // q32's shape: 100M at the collapsed 2.0M plans thirty-two passes, at the 12.58M ceiling eight.
    assertTrue(SirixVectorizedExecutor.GroupPasses.refreshWorthIt(100_000_000L, 2_026_894L, 12_582_912L, PARTITIONS));
  }

  @Test
  @DisplayName("an abort at a collapsed budget refreshes once, then re-plans on the fit alone")
  void abortRefreshesOnceAndReplansOnTheFit() {
    final AtomicInteger collections = new AtomicInteger();
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(10_000L, () -> {
      collections.incrementAndGet();
      return 10_000L;
    });
    final ProjectionIndexRegistry.Handle handle = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    final long refreshesBefore = SirixVectorizedExecutor.GroupPasses.budgetRefreshCount();
    final SirixVectorizedExecutor.GroupPasses plan =
        new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, 1_000L, PARTITIONS);
    // Nothing memoed for the shape: one pass at the budget as read, no collection.
    assertEquals(1, plan.passes());
    assertEquals(1_000L, plan.budget());
    assertEquals(0, collections.get());
    assertEquals(0L, plan.plannedGroups(), "a blind plan expects no count: shared tables start at the worker hint");

    // The pass aborted having seen 2,500 groups (all in never-flushed worker tables) over half the
    // leaves: 5,000 estimated. Six passes at 1,000; ONE at the 10,000 a clean heap yields.
    final GroupTableSpill first =
        new GroupTableSpill(PARTITIONS, SHIFT, () -> new NumericGroupAggTable(1, 1 << 8), 0, PARTITIONS, 1_000L);
    first.noteLeavesScanned(50);
    first.noteAbandonedLocal(2_500);
    plan.restart(first, 100);
    assertEquals(1, collections.get(), "one collection");
    assertEquals(refreshesBefore + 1L, SirixVectorizedExecutor.GroupPasses.budgetRefreshCount());
    assertEquals(10_000L, plan.budget());
    assertEquals(1, plan.passes(), "the widened budget holds the estimate in one pass: no floor above the fit");
    assertEquals(10_000L, plan.passBudget());
    assertEquals(5_000L, handle.observedGroupsFor(FINGERPRINT));
    assertEquals(5_000L, plan.plannedGroups(), "the restarted pass set is planned from the abort estimate");
    assertFalse(plan.seededCompleted());

    // A second abort of the same execution: the estimate has grown to 20,000 and a ceiling of 100,000
    // WOULD save a pass, but a plan collects once — the floor (twice the passes) and the fit decide.
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(100_000L, () -> {
      collections.incrementAndGet();
      return 100_000L;
    });
    final GroupTableSpill second =
        new GroupTableSpill(PARTITIONS, SHIFT, () -> new NumericGroupAggTable(1, 1 << 8), 0, PARTITIONS, 10_000L);
    second.noteLeavesScanned(50);
    second.noteAbandonedLocal(10_000);
    plan.restart(second, 100);
    assertEquals(1, collections.get(), "a plan collects once");
    assertEquals(refreshesBefore + 1L, SirixVectorizedExecutor.GroupPasses.budgetRefreshCount());
    assertEquals(10_000L, plan.budget());
    assertEquals(2, plan.passes(), "20,000 at 10,000: two passes of sixteen partitions, and twice the one pass");
    assertEquals(20_000L, handle.observedGroupsFor(FINGERPRINT), "the memo keeps the maximum estimate");
    assertEquals(20_000L, plan.plannedGroups());
  }

  @Test
  @DisplayName("a clean heap costs nothing: no collection, the abort floor and the fit plan the passes")
  void cleanHeapAbortsPlanWithoutACollection() {
    final AtomicInteger collections = new AtomicInteger();
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(1_000L, () -> {
      collections.incrementAndGet();
      return 1_000L;
    });
    final ProjectionIndexRegistry.Handle handle = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    final long refreshesBefore = SirixVectorizedExecutor.GroupPasses.budgetRefreshCount();
    final SirixVectorizedExecutor.GroupPasses plan =
        new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, 1_000L, PARTITIONS);
    final GroupTableSpill spill =
        new GroupTableSpill(PARTITIONS, SHIFT, () -> new NumericGroupAggTable(1, 1 << 8), 0, PARTITIONS, 1_000L);
    spill.noteLeavesScanned(50);
    spill.noteAbandonedLocal(2_500);
    plan.restart(spill, 100);
    assertEquals(0, collections.get());
    assertEquals(refreshesBefore, SirixVectorizedExecutor.GroupPasses.budgetRefreshCount());
    assertEquals(1_000L, plan.budget());
    // 5,000 estimated at 1,000: six partitions hold 938 — six balanced passes (above the floor of two).
    assertEquals(6, plan.passes());
    assertEquals(1_000L, plan.passBudget());
    // A second abort doubles the floor: the estimate barely grew (6,000 → still six by the fit), so
    // the floor of twelve decides — an estimate that keeps falling short cannot cost a scan per pass.
    final GroupTableSpill again =
        new GroupTableSpill(PARTITIONS, SHIFT, () -> new NumericGroupAggTable(1, 1 << 8), 0, 6, 1_000L);
    again.noteLeavesScanned(100);
    again.noteAbandonedLocal(1_125);
    assertEquals(6_000L, again.estimatedTotalGroups(100));
    plan.restart(again, 100);
    assertEquals(12, plan.passes());
  }

  @Test
  @DisplayName("a memoed shape refreshes at construction, so the first pass set already fits the clean budget")
  void memoedShapeRefreshesAtConstruction() {
    final AtomicInteger collections = new AtomicInteger();
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(10_000L, () -> {
      collections.incrementAndGet();
      return 10_000L;
    });
    // An abort-time estimate memoed by an earlier execution.
    final ProjectionIndexRegistry.Handle observed = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    observed.noteObservedGroups(FINGERPRINT, 5_000L);
    final SirixVectorizedExecutor.GroupPasses fromEstimate =
        new SirixVectorizedExecutor.GroupPasses(observed, FINGERPRINT, 1_000L, PARTITIONS);
    assertEquals(1, collections.get());
    assertEquals(10_000L, fromEstimate.budget());
    assertEquals(1, fromEstimate.passes());
    assertEquals(10_000L, fromEstimate.passBudget());
    assertFalse(fromEstimate.seededCompleted());
    assertEquals(5_000L, fromEstimate.plannedGroups(), "planned from the memoed estimate");

    // A completed scan: six passes completed at the collapsed budget; the refreshed budget holds the
    // exact count in one, and the completed count only ever caps.
    final ProjectionIndexRegistry.Handle completed = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    completed.noteCompletedGroupScan(FINGERPRINT, 5_000L, 6);
    final SirixVectorizedExecutor.GroupPasses fromCompleted =
        new SirixVectorizedExecutor.GroupPasses(completed, FINGERPRINT, 1_000L, PARTITIONS);
    assertEquals(2, collections.get());
    assertEquals(10_000L, fromCompleted.budget());
    assertEquals(1, fromCompleted.passes());
    assertTrue(fromCompleted.seededCompleted());
    assertEquals(10_000L, fromCompleted.passBudget(), "the pass budget never drops below the plan's budget");
    assertEquals(5_000L, fromCompleted.plannedGroups(), "planned from the completed count");

    // An unknown shape has nothing to judge the refresh by: no collection at construction.
    final ProjectionIndexRegistry.Handle unknown = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    new SirixVectorizedExecutor.GroupPasses(unknown, FINGERPRINT, 1_000L, PARTITIONS);
    assertEquals(2, collections.get());
  }

  @Test
  @DisplayName("a completed pass set that aborted anyway memoes the count forcing the pass count that completed")
  void completedSeedThatAbortedMemoesTheForcingCount() {
    SirixVectorizedExecutor.GroupPasses.setBudgetRefreshForTesting(1_000L, () -> 1_000L);
    final ProjectionIndexRegistry.Handle handle = new ProjectionIndexRegistry.Handle(new String[] {"a"}, List.of());
    // The memo says 3,000 groups completed in two passes; at the tolerant 1,100 the count implies
    // three (eleven partitions hold 1,032) — two completed, and two passes of 1,500 stay within twice
    // the budget, so two it is.
    handle.noteCompletedGroupScan(FINGERPRINT, 3_000L, 2);
    final SirixVectorizedExecutor.GroupPasses plan =
        new SirixVectorizedExecutor.GroupPasses(handle, FINGERPRINT, 1_000L, PARTITIONS);
    assertEquals(2, plan.passes());
    assertTrue(plan.seededCompleted());
    assertTrue(plan.passBudget() > 1_500L && plan.passBudget() < 1_900L, "pass budget: " + plan.passBudget());
    // The seeded pass set aborted at a spill that saw 1,800 groups in its half over every leaf:
    // 3,600 estimated, four passes at 1,000 (eight partitions hold 900), above the floor of three.
    final GroupTableSpill spill =
        new GroupTableSpill(PARTITIONS, SHIFT, () -> new NumericGroupAggTable(1, 1 << 8), 0, 16, 1_000L);
    spill.noteLeavesScanned(100);
    spill.noteAbandonedLocal(1_800);
    plan.restart(spill, 100);
    assertEquals(4, plan.passes());
    assertFalse(plan.seededCompleted());
    // Four passes completed and summed to 3,000 again. The count alone would re-seed the two passes
    // that just aborted, so the memo records the smallest count forcing four passes at the tolerant
    // budget of 1,100 instead: three passes hold eleven partitions, 1,100 × 32 / 11 + 1 = 3,201.
    for (int partition = 0; partition < PARTITIONS; partition++) {
      plan.notePartition(partition, partition < 24
          ? 94
          : 93);
    }
    plan.complete();
    final ProjectionIndexRegistry.Handle.CompletedGroupScan memo = handle.completedGroupScanFor(FINGERPRINT);
    assertEquals(4, memo.passes());
    assertEquals(3_201L, memo.groups());
    assertEquals(4,
        SirixVectorizedExecutor.GroupPasses.seededPasses(memo.groups(), memo.passes(), 1_000L, PARTITIONS, false));
  }
}
