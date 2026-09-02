package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The balanced hash-range pass arithmetic: any pass count up to the partition count, consecutive
 * ranges differing by at most one partition, the fewest passes whose largest share fits a budget, and
 * the abort-time estimate that counts the never-flushed worker tables.
 */
final class GroupTableSpillPassPlanTest {

  @Test
  @DisplayName("every pass count partitions the key space into consecutive shares differing by at most one")
  void balancedRangesCoverThePartitionsOnce() {
    for (final int partitions : new int[] {32, 64}) {
      for (int passes = 1; passes <= partitions; passes++) {
        assertEquals(0, GroupTableSpill.passLo(partitions, passes, 0));
        assertEquals(partitions, GroupTableSpill.passHi(partitions, passes, passes - 1));
        int largest = 0;
        int smallest = Integer.MAX_VALUE;
        for (int pass = 0; pass < passes; pass++) {
          final int lo = GroupTableSpill.passLo(partitions, passes, pass);
          final int hi = GroupTableSpill.passHi(partitions, passes, pass);
          assertTrue(hi > lo, "empty pass " + pass + " of " + passes + " over " + partitions);
          if (pass > 0) {
            assertEquals(GroupTableSpill.passHi(partitions, passes, pass - 1), lo, "gap before pass " + pass);
          }
          largest = Math.max(largest, hi - lo);
          smallest = Math.min(smallest, hi - lo);
        }
        assertTrue(largest - smallest <= 1, passes + " passes over " + partitions + ": " + smallest + ".." + largest);
        assertEquals(GroupTableSpill.largestPassShare(partitions, passes), largest, "largest share of " + passes);
      }
    }
  }

  @Test
  @DisplayName("the fewest passes whose largest share fits: one while the count fits, else the first count that does")
  void passesForIsTheSmallestFittingCount() {
    final long budget = 12_582_912L;
    // 50M groups: four passes of eight partitions hold 12.5M and fit; at 55M they hold 13.75M and the
    // fifth pass (seven partitions, 12.03M) is needed — where power-of-two passes would jump to eight.
    assertEquals(4, GroupTableSpill.passesFor(50_000_000L, budget, 32));
    assertEquals(5, GroupTableSpill.passesFor(55_000_000L, budget, 32));
    // 28M: two even passes hold 14M; three balanced passes (11, 11, 10 partitions) hold 9.6M.
    assertEquals(3, GroupTableSpill.passesFor(28_000_000L, budget, 32));
    // 100M: eight passes of four partitions hold 12.5M and fit.
    assertEquals(8, GroupTableSpill.passesFor(100_000_000L, budget, 32));
    // 110M: nine or ten passes still need a four-partition share (13.75M); eleven passes of three fit.
    assertEquals(11, GroupTableSpill.passesFor(110_000_000L, budget, 32));
    assertEquals(1, GroupTableSpill.passesFor(budget, budget, 32));
    assertEquals(2, GroupTableSpill.passesFor(budget + 1L, budget, 32));
    assertEquals(1, GroupTableSpill.passesFor(0L, budget, 32));
    assertEquals(1, GroupTableSpill.passesFor(5L, Long.MAX_VALUE, 32));
    // Beyond one pass per partition the count stays at the partition count: the abort machinery decides.
    assertEquals(32, GroupTableSpill.passesFor(budget * 33L, budget, 32));
    for (long groups = 1L; groups <= 4_096L; groups += 7L) {
      for (final long b : new long[] {1L, 3L, 32L, 100L, 1_000L}) {
        final int passes = GroupTableSpill.passesFor(groups, b, 32);
        assertTrue(passes >= 1 && passes <= 32);
        if (passes == 1) {
          assertTrue(groups <= b);
        } else if (passes < 32) {
          assertTrue(GroupTableSpill.expectedLargestPass(groups, passes, 32) <= b, groups + "@" + b + " -> " + passes);
          assertTrue(GroupTableSpill.expectedLargestPass(groups, passes - 1, 32) > b,
              groups + "@" + b + ": " + (passes - 1) + " passes would have done");
        }
      }
    }
  }

  @Test
  @DisplayName("the count forcing a pass count is the smallest for which the arithmetic answers it")
  void groupsForcingPassesInvertsPassesFor() {
    for (final long budget : new long[] {32L, 1_000L, 12_582_912L}) {
      for (int passes = 2; passes <= 32; passes++) {
        final long forcing = GroupTableSpill.groupsForcingPasses(passes, budget, 32);
        assertTrue(GroupTableSpill.passesFor(forcing, budget, 32) >= passes, passes + "@" + budget + ": " + forcing);
        assertTrue(GroupTableSpill.passesFor(forcing - 1L, budget, 32) < passes, passes + "@" + budget + ": " + (forcing - 1L));
      }
    }
    assertEquals(0L, GroupTableSpill.groupsForcingPasses(1, 1_000L, 32));
    assertEquals(Long.MAX_VALUE, GroupTableSpill.groupsForcingPasses(2, Long.MAX_VALUE, 32));
  }

  @Test
  @DisplayName("the abort-time estimate counts abandoned worker tables and the pass's share of the key space")
  void estimateCountsAbandonedTablesAndTheShare() {
    // A pass owning 8 of 32 partitions saw 1,000 groups over a quarter of the leaves: 16,000 estimated.
    final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, false, 0L, 0),
        8, 16, 3_000L);
    spill.noteLeavesScanned(25);
    spill.noteAbandonedLocal(600);
    spill.noteAbandonedLocal(400);
    spill.noteAbandonedLocal(0);
    assertEquals(1_000L, spill.groupsAbandoned());
    assertEquals(16_000L, spill.estimatedTotalGroups(100));
    // 16,000 at a budget of 3,000: a share of six partitions holds 3,000 exactly — six passes.
    assertEquals(6, spill.recommendedPasses(100, 3_000L));
    // At a wider budget the same abort recommends fewer passes; the caller decides the floor.
    assertEquals(1, spill.recommendedPasses(100, 16_000L));
    assertEquals(32, spill.recommendedPasses(100, 1L));
  }

  @Test
  @DisplayName("releasing an aborted pass drops its shared tables and keeps every counter the estimate needs")
  void releaseDropsTheTablesAndKeepsTheCounters() {
    final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, false, 0L, 0),
        0, 32, 50L);
    final NumericGroupAggTable local = spill.freshLocal();
    for (long key = 1; key <= 120; key++) {
      local.acquire(key, key);
    }
    spill.noteLeavesScanned(10);
    spill.flush(local);
    assertTrue(spill.aborted(), "120 groups over a budget of 50 must abort the pass");
    assertEquals(120L, spill.groupsSpilled());
    int heldBefore = 0;
    for (int p = 0; p < 32; p++) {
      // A fresh table would count 0 here: 120 in total proves every group sat in a shared table.
      heldBefore += spill.takeOrCreate(p, () -> new NumericGroupAggTable(1, 16, false, 0L, 0)).size();
    }
    assertEquals(120, heldBefore, "every spilled group sits in exactly one shared partition table");
    // Rebuild the shared state (takeOrCreate detached it) and release it the way an abort does.
    final GroupTableSpill again = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, false, 0L, 0),
        0, 32, 50L);
    again.noteLeavesScanned(10);
    again.noteAbandonedLocal(30);
    final NumericGroupAggTable local2 = again.freshLocal();
    for (long key = 1; key <= 120; key++) {
      local2.acquire(key, key);
    }
    again.flush(local2);
    final long estimateBefore = again.estimatedTotalGroups(40);
    final long releasesBefore = GroupTableSpill.releaseCount();
    again.releaseTables();
    assertEquals(releasesBefore + 1L, GroupTableSpill.releaseCount());
    for (int p = 0; p < 32; p++) {
      assertEquals(0, again.takeOrCreate(p, () -> new NumericGroupAggTable(1, 16, false, 0L, 0)).size(),
          "partition " + p + " must be dropped by the release");
    }
    assertEquals(120L, again.groupsSpilled(), "the spilled counter survives the release");
    assertEquals(30L, again.groupsAbandoned(), "the abandoned counter survives the release");
    assertEquals(10L, again.leavesScanned(), "the leaves counter survives the release");
    assertEquals(estimateBefore, again.estimatedTotalGroups(40), "the estimate is a function of the counters only");
    assertEquals(600L, estimateBefore, "(120 + 30) / (10/40) over the whole range");
    assertTrue(again.aborted(), "the abort verdict survives the release");
  }
}
