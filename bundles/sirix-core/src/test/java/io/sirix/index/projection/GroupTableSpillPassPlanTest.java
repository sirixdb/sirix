package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  @Test
  @DisplayName("a flushed worker table is released into the spill's pool and the next fresh table takes from it")
  void flushRecyclesTheWorkerTable() {
    final int previous = GroupTableSpill.setChunkPoolForTesting(1);
    try {
      // 2^12 entries hinted: every chunk has the pool's full length from the first insertion.
      final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 1 << 12, true, 0L, 0));
      final LongChunkPool pool = spill.chunkPool();
      assertTrue(pool != null, "the pool is on");
      assertEquals(NumericGroupAggTable.fullChunkLanes(new NumericGroupAggTable(1, 16, true).stride()),
          pool.chunkLanes(), "the pool recycles the factory layout's full chunk");
      final NumericGroupAggTable local = spill.freshLocal();
      assertSame(pool, local.chunkPool(), "a fresh worker table draws from the spill's pool");
      for (long key = 1; key <= 3_000; key++) {
        local.acquire(key, key);
      }
      int allocated = 0;
      for (int c = 0; c < local.storageChunkCount(); c++) {
        if (local.storageChunkOrNull(c) != null) {
          allocated++;
        }
      }
      assertTrue(allocated > 1, "3000 groups span several chunks");
      final long givesBefore = pool.gives();
      final long droppedBefore = pool.dropped();
      spill.flush(local);
      assertTrue(local.released(), "flush releases the worker table it merged");
      // Counted as gives, not as a pooled delta: the merge's partition tables TAKE from the same
      // pool, and a shared pool that earlier scans filled serves those takes as hits.
      assertTrue(pool.gives() - givesBefore + pool.dropped() - droppedBefore >= allocated,
          "the flushed table's chunks were handed to the pool: " + pool);
      for (int p = 0; p < 32; p++) {
        final NumericGroupAggTable shared = spill.takeOrCreate(p, () -> new NumericGroupAggTable(1, 16, true, 0L, 0));
        assertSame(pool, shared.chunkPool(), "shared partition table " + p + " draws from the pool too");
      }
      final long hitsBefore = pool.hits();
      final NumericGroupAggTable next = spill.freshLocal();
      next.acquire(77L, 0L);
      assertTrue(pool.hits() > hitsBefore, "the next worker table's first chunk is a recycled one");
    } finally {
      GroupTableSpill.setChunkPoolForTesting(previous);
    }
  }

  @Test
  @DisplayName("releasing an aborted pass drains a PER-SCAN pool: a forced collection must not measure recycled chunks")
  void releaseTablesDrainsThePool() {
    final int previous = GroupTableSpill.setChunkPoolForTesting(1);
    final int retainBefore = LongChunkPool.setRetainForTesting(0);
    try {
      final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 1 << 12, true, 0L, 0),
          0, 32, 50L);
      final NumericGroupAggTable local = spill.freshLocal();
      for (long key = 1; key <= 3_000; key++) {
        local.acquire(key, key);
      }
      spill.flush(local);
      assertTrue(spill.aborted());
      final LongChunkPool pool = spill.chunkPool();
      assertTrue(!pool.isShared(), "retention off: the spill owns its pool");
      assertTrue(pool.pooled() > 0, "the flushed table's chunks sit in the pool before the release: " + pool);
      spill.releaseTables();
      assertEquals(0, pool.pooled(), "nothing pooled survives the release");
    } finally {
      LongChunkPool.setRetainForTesting(retainBefore);
      GroupTableSpill.setChunkPoolForTesting(previous);
    }
  }

  @Test
  @DisplayName("the shared pool outlives the spill: an aborted pass's tables and a finished pass's chunks are the next spill's tables")
  void sharedPoolOutlivesTheSpill() {
    final int previous = GroupTableSpill.setChunkPoolForTesting(1);
    final int retainBefore = LongChunkPool.setRetainForTesting(1);
    try {
      final GroupTableSpill first = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 1 << 12, true, 0L, 0),
          0, 32, 50L);
      final LongChunkPool pool = first.chunkPool();
      assertTrue(pool.isShared(), "retention on: the spill draws from the JVM-lifetime pool");
      final NumericGroupAggTable local = first.freshLocal();
      for (long key = 1; key <= 3_000; key++) {
        local.acquire(key, key);
      }
      first.flush(local);
      assertTrue(first.aborted());
      final NumericGroupAggTable shared = first.takeOrCreate(0, () -> new NumericGroupAggTable(1, 16, true, 0L, 0));
      first.flush(shared); // a shared table exists again for releaseTables to hand back
      final int pooledBeforeRelease = pool.pooled();
      first.releaseTables();
      assertTrue(pool.pooled() >= pooledBeforeRelease, "the release keeps (and grows) the shared pool: " + pool);
      assertTrue(pool.pooled() > 0, "the aborted pass's chunks are retained: " + pool);
      assertTrue(LongChunkPool.retainedBytes() >= (long) pool.pooled() * pool.chunkLanes() * Long.BYTES,
          "the retained bytes account for this pool");

      final GroupTableSpill second = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 1 << 12, true, 0L, 0),
          0, 32, 50L);
      assertSame(pool, second.chunkPool(), "the next spill of the same layout shares the pool");
      final long hitsBefore = pool.hits();
      final long missesBefore = pool.misses();
      final NumericGroupAggTable next = second.freshLocal();
      next.acquire(77L, 0L);
      assertTrue(pool.hits() > hitsBefore, "the next spill's first chunk is a retained one: " + pool);
      assertEquals(missesBefore, pool.misses(), "nothing was allocated for it");
    } finally {
      LongChunkPool.setRetainForTesting(retainBefore);
      GroupTableSpill.setChunkPoolForTesting(previous);
    }
  }

  @Test
  @DisplayName("the shared pool's capacity is the largest scan's demand under the retain ceiling")
  void sharedPoolCapacityFollowsDemandUnderTheCeiling() {
    final int lanes = 96; // a layout no other test shares, so the capacity starts at this test's demand
    final LongChunkPool small = LongChunkPool.shared(lanes, 3);
    assertTrue(small.isShared());
    assertTrue(small.maxChunks() >= 3, "capacity raised to the first demand: " + small);
    final LongChunkPool larger = LongChunkPool.shared(lanes, 7);
    assertSame(small, larger, "one pool per chunk length");
    assertTrue(larger.maxChunks() >= 7, "capacity follows the larger demand: " + larger);
    final LongChunkPool smallerAgain = LongChunkPool.shared(lanes, 2);
    assertTrue(smallerAgain.maxChunks() >= 7, "a smaller demand never lowers it: " + smallerAgain);
    final LongChunkPool huge = LongChunkPool.shared(lanes, Integer.MAX_VALUE);
    assertEquals(LongChunkPool.retainCeilingChunks(lanes), huge.maxChunks(),
        "the retain ceiling bounds the capacity whatever the demand");
  }

  @Test
  @DisplayName("the kill switch leaves every table without a pool")
  void killSwitchDisablesThePool() {
    final int previous = GroupTableSpill.setChunkPoolForTesting(0);
    try {
      final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 1 << 12, true, 0L, 0));
      assertTrue(spill.chunkPool() == null);
      final NumericGroupAggTable local = spill.freshLocal();
      assertTrue(local.chunkPool() == null);
      local.acquire(1L, 1L);
      final long givesBefore = LongChunkPool.totalGives();
      spill.flush(local);
      assertTrue(local.released(), "flush still releases the table (to the collector)");
      assertEquals(givesBefore, LongChunkPool.totalGives(), "no pool received a chunk");
      assertTrue(spill.takeOrCreate(0, () -> new NumericGroupAggTable(1, 16, true, 0L, 0)).chunkPool() == null);
    } finally {
      GroupTableSpill.setChunkPoolForTesting(previous);
    }
  }

  @Test
  @DisplayName("the pool cap is twice a pass's resident state and never below 64 or above 2^22 chunks")
  void poolCapacityTracksTheBudget() {
    final int stride = 18;
    final int lanes = NumericGroupAggTable.fullChunkLanes(stride);
    final int small = GroupTableSpill.poolCapacityChunks(1L, 1, stride, lanes);
    assertEquals(64, small, "floor");
    final int q32 = GroupTableSpill.poolCapacityChunks(12_582_912L, 1 << 18, stride, lanes);
    // (12.58M + 64 × 262144) groups × 18 lanes × 4/3 × 2 / lanes per chunk
    final double expected = (12_582_912.0 + 64.0 * (1 << 18)) * stride * (4.0 / 3.0) * 2.0 / lanes;
    assertEquals((int) Math.ceil(expected), q32);
    assertTrue(q32 > small);
    assertEquals(1 << 22, GroupTableSpill.poolCapacityChunks(Long.MAX_VALUE, 1 << 18, stride, lanes), "ceiling");
  }

  @Test
  @DisplayName("consecutive flushes start at distinct, far-apart partitions for every power-of-two count")
  void flushStartsSpreadOverThePartitions() {
    for (final int partitions : new int[] {1, 2, 4, 8, 16, 32, 64, 256, 1024}) {
      final int stride = GroupTableSpill.flushStride(partitions);
      assertEquals(1, stride & 1, "an odd stride is coprime to " + partitions);
      final boolean[] seen = new boolean[partitions];
      int previous = -1;
      for (long ordinal = 0; ordinal < partitions; ordinal++) {
        final int start = GroupTableSpill.flushStart(ordinal, partitions);
        assertTrue(start >= 0 && start < partitions, "start " + start + " over " + partitions);
        assertFalse(seen[start], "flush " + ordinal + " over " + partitions + " restarts at " + start);
        seen[start] = true;
        if (previous >= 0 && partitions >= 8) {
          final int forward = (start - previous + partitions) & (partitions - 1);
          final int distance = Math.min(forward, partitions - forward);
          assertTrue(distance * 10 >= partitions * 3,
              "flushes " + (ordinal - 1) + " and " + ordinal + " over " + partitions + " start " + distance + " apart");
        }
        previous = start;
      }
      // The (partitions+1)-th flush wraps to the first start: the walk is periodic, not drifting.
      assertEquals(GroupTableSpill.flushStart(0, partitions), GroupTableSpill.flushStart(partitions, partitions));
    }
    assertEquals(19, GroupTableSpill.flushStride(32), "32 partitions: stride 19 (0.618 × 32 = 19.8, odd)");
    assertEquals(0, GroupTableSpill.flushStart(0, 32));
    assertEquals(19, GroupTableSpill.flushStart(1, 32));
    assertEquals(6, GroupTableSpill.flushStart(2, 32));
  }

  @Test
  @DisplayName("a shared table hint is the planned share plus skew, bounded by the pass budget's share")
  void sharedHintIsThePlannedShareBoundedByTheBudget() {
    assertEquals(3_150, GroupTableSpill.sharedTableHint(96_000L, 32, 0, 32, Long.MAX_VALUE), "3,000 + 5 %");
    assertEquals(1_000, GroupTableSpill.sharedTableHint(96_000L, 32, 8, 16, 8_000L), "8,000 over 8 partitions");
    assertEquals(-1, GroupTableSpill.sharedTableHint(0L, 32, 0, 32, Long.MAX_VALUE), "a blind plan has no hint");
    assertEquals(16, GroupTableSpill.sharedTableHint(10L, 32, 0, 32, Long.MAX_VALUE), "floor");
    assertEquals(Integer.MAX_VALUE, GroupTableSpill.sharedTableHint(Long.MAX_VALUE, 1, 0, 1, Long.MAX_VALUE),
        "a huge count saturates instead of overflowing");
    final int previous = GroupTableSpill.setPresizeSharedForTesting(0);
    try {
      assertEquals(-1, GroupTableSpill.sharedTableHint(96_000L, 32, 0, 32, Long.MAX_VALUE), "kill switch");
    } finally {
      GroupTableSpill.setPresizeSharedForTesting(previous);
    }
  }

  @Test
  @DisplayName("the skew allowance is refused when it alone would double the shared table's capacity")
  void sharedHintNeverDoublesTheTableForItsSkewAllowance() {
    // 100M rows over 32 partitions: the share fits a 2^22-bucket table (it grows at 3,145,728) and the 5 %
    // allowance asked for 2^23 — the observed q32 doubling. The hint stays inside the smaller table with
    // eight roots of headroom above the share.
    final long expected = 99_997_672L;
    final long share = expected / 32; // 3,124,927
    final int hint = GroupTableSpill.sharedTableHint(expected, 32, 0, 4, Long.MAX_VALUE);
    assertEquals(1 << 22, NumericGroupAggTable.capacityFor(hint), "the allowance must not buy a doubled table");
    assertTrue(hint > share, "the hint still covers the share: " + hint);
    assertTrue(hint - share >= 8L * (long) Math.sqrt((double) share), "eight roots of headroom: " + (hint - share));
    assertTrue(hint <= 3_145_728, "inside the smaller table's growth threshold: " + hint);
    // Where the share ITSELF needs the larger table, the allowance stays: nothing to refuse.
    final long big = 3_200_000L * 32;
    final int bigHint = GroupTableSpill.sharedTableHint(big, 32, 0, 4, Long.MAX_VALUE);
    assertEquals(3_200_000 + 160_000, bigHint, "share + 5 % when the share already needs 2^23");
    assertEquals(1 << 23, NumericGroupAggTable.capacityFor(bigHint));
    // Where the allowance stays inside the share's power of two, it is kept unchanged.
    assertEquals(3_150, GroupTableSpill.sharedTableHint(96_000L, 32, 0, 32, Long.MAX_VALUE), "3,000 + 5 %");
    // The capacity rule the hint reasons with IS the constructor's rule.
    assertEquals(16, NumericGroupAggTable.capacityFor(1));
    assertEquals(1 << 22, NumericGroupAggTable.capacityFor(3_145_728), "exactly 3/4 of 2^22 still fits 2^22");
    assertEquals(1 << 23, NumericGroupAggTable.capacityFor(3_145_729), "one past 3/4 of 2^22 doubles");
  }

  /** A factory that records the hint every table was asked for. */
  private static IntFunction<NumericGroupAggTable> recordingFactory(final List<Integer> hints) {
    return hint -> {
      hints.add(hint);
      return new NumericGroupAggTable(1, hint, true, 0L, 0);
    };
  }

  @Test
  @DisplayName("shared tables are created at the plan's share and take no rehash; a blind or short plan grows them")
  void sharedTablesAreCreatedAtThePlannedShare() {
    final int previous = GroupTableSpill.setPresizeSharedForTesting(1);
    try {
      final int groups = 96_000;
      // Planned exactly: 3,000 per partition, hinted 3,150 — every shared table holds its share as built.
      final List<Integer> hints = new ArrayList<>();
      final GroupTableSpill planned = new GroupTableSpill(32, 59, recordingFactory(hints), groups, 0, 32, Long.MAX_VALUE);
      assertEquals(3_150, planned.sharedHint());
      hints.clear();
      final long presizedBefore = GroupTableSpill.presizedSharedCount();
      final NumericGroupAggTable local = planned.freshLocal();
      assertEquals(List.of(GroupTableSpill.WORKER_TABLE_HINT), hints, "a worker table is asked for at the worker hint");
      for (long key = 1; key <= groups; key++) {
        local.acquire(key, key);
      }
      hints.clear();
      planned.flush(local);
      assertEquals(32, hints.size(), "one shared table per partition");
      for (final int hint : hints) {
        assertEquals(3_150, hint, "every shared table is asked for at the planned share");
      }
      assertEquals(presizedBefore + 32L, GroupTableSpill.presizedSharedCount());
      int held = 0;
      for (int p = 0; p < 32; p++) {
        held += planned.takeOrCreate(p, () -> new NumericGroupAggTable(1, 16, true, 0L, 0)).size();
      }
      assertEquals(groups, held, "every group sits in exactly one shared table");
      assertEquals(0L, planned.sharedRehashes(), "a pre-size that held took no rehash under the locks");

      // Planned a hundredfold short: the same groups grow the tables — and the witness counts it.
      final List<Integer> shortHints = new ArrayList<>();
      final GroupTableSpill planShort = new GroupTableSpill(32, 59, recordingFactory(shortHints), 32 * 30, 0, 32,
          Long.MAX_VALUE);
      assertEquals(31, planShort.sharedHint(), "30 + 5 % (integer) per partition");
      final NumericGroupAggTable local2 = planShort.freshLocal();
      for (long key = 1; key <= groups; key++) {
        local2.acquire(key, key);
      }
      planShort.flush(local2);
      held = 0;
      for (int p = 0; p < 32; p++) {
        held += planShort.takeOrCreate(p, () -> new NumericGroupAggTable(1, 16, true, 0L, 0)).size();
      }
      assertEquals(groups, held);
      assertTrue(planShort.sharedRehashes() >= 32L * 6L,
          "3,000 groups in a 32-bucket table rehash at least six times per partition: " + planShort.sharedRehashes());
      // An aborted pass's release sums the same witness, so a blind pass's growth is never lost.
      final GroupTableSpill planShortAborted = new GroupTableSpill(32, 59, recordingFactory(new ArrayList<>()),
          32 * 30, 0, 32, Long.MAX_VALUE);
      final NumericGroupAggTable local4 = planShortAborted.freshLocal();
      for (long key = 1; key <= groups; key++) {
        local4.acquire(key, key);
      }
      planShortAborted.flush(local4);
      assertEquals(0L, planShortAborted.sharedRehashes(), "summed only as the tables leave the spill");
      planShortAborted.releaseTables();
      assertTrue(planShortAborted.sharedRehashes() >= 32L * 6L, "release sums the rehashes: " + planShortAborted.sharedRehashes());

      // Blind: no hint, the shared tables are asked for at the worker hint and never pre-sized.
      final List<Integer> blindHints = new ArrayList<>();
      final GroupTableSpill blind = new GroupTableSpill(32, 59, recordingFactory(blindHints), 0L, 0, 32, Long.MAX_VALUE);
      assertEquals(-1, blind.sharedHint());
      final long presizedBeforeBlind = GroupTableSpill.presizedSharedCount();
      final NumericGroupAggTable local3 = blind.freshLocal();
      local3.acquire(5L, 5L);
      blindHints.clear();
      blind.flush(local3);
      assertEquals(List.of(GroupTableSpill.WORKER_TABLE_HINT), blindHints, "the one shared table takes the worker hint");
      assertEquals(presizedBeforeBlind, GroupTableSpill.presizedSharedCount(), "nothing pre-sized");
      // A released aborted pass sums the rehashes too (the witness is not lost with the tables).
      assertEquals(0L, blind.sharedRehashes());
      blind.releaseTables();
      assertEquals(0L, blind.sharedRehashes());
      assertThrows(IllegalArgumentException.class,
          () -> new GroupTableSpill(32, 59, recordingFactory(blindHints), -1L, 0, 32, Long.MAX_VALUE));
    } finally {
      GroupTableSpill.setPresizeSharedForTesting(previous);
    }
  }

  @Test
  @DisplayName("the kill switches restore the fixed walk and the worker hint for shared tables")
  void killSwitchesRestoreTheFixedWalkAndTheWorkerHint() {
    final int offsetBefore = GroupTableSpill.setFlushOffsetForTesting(0);
    final int presizeBefore = GroupTableSpill.setPresizeSharedForTesting(0);
    try {
      final List<Integer> hints = new ArrayList<>();
      final GroupTableSpill spill = new GroupTableSpill(32, 59, recordingFactory(hints), 96_000L, 0, 32, Long.MAX_VALUE);
      assertFalse(spill.flushOffset(), "the walk is fixed");
      assertEquals(-1, spill.sharedHint(), "no pre-size");
      final long presizedBefore = GroupTableSpill.presizedSharedCount();
      final NumericGroupAggTable local = spill.freshLocal();
      for (long key = 1; key <= 3_000; key++) {
        local.acquire(key, key);
      }
      hints.clear();
      spill.flush(local);
      for (final int hint : hints) {
        assertEquals(GroupTableSpill.WORKER_TABLE_HINT, hint, "a shared table takes the worker hint");
      }
      assertEquals(presizedBefore, GroupTableSpill.presizedSharedCount());
    } finally {
      GroupTableSpill.setFlushOffsetForTesting(offsetBefore);
      GroupTableSpill.setPresizeSharedForTesting(presizeBefore);
    }
    GroupTableSpill.setFlushOffsetForTesting(1);
    try {
      assertTrue(new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, true, 0L, 0)).flushOffset());
    } finally {
      GroupTableSpill.setFlushOffsetForTesting(offsetBefore);
    }
  }

  @Test
  @DisplayName("a flush that dies of memory still counts its table's groups toward the abort-time estimate")
  void failedFlushCountsItsGroupsAsAbandoned() {
    final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, true, 0L, 0),
        0, 32, Long.MAX_VALUE);
    final NumericGroupAggTable local = spill.freshLocal();
    for (long key = 1; key <= 40; key++) {
      local.acquire(key, key);
    }
    spill.noteLeavesScanned(10);
    GroupTableSpill.setSimulateOutOfMemoryOnFlushForTesting(true);
    try {
      assertThrows(OutOfMemoryError.class, () -> spill.flush(local));
    } finally {
      GroupTableSpill.setSimulateOutOfMemoryOnFlushForTesting(false);
    }
    assertEquals(0L, spill.groupsSpilled(), "nothing landed");
    assertEquals(40L, spill.groupsAbandoned(), "the forty groups the dying flush held were seen by the pass");
    assertEquals(160L, spill.estimatedTotalGroups(40), "40 over a quarter of the leaves");
    assertFalse(local.released(), "a failed flush leaves the table to its caller");
  }

  @Test
  @DisplayName("the leaf cursor deals every row group exactly once, in order, and then only the end")
  void leafCursorDealsEveryRowGroupOnce() {
    final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, true, 0L, 0));
    final int leaves = 1000;
    final int morsel = 64;
    final boolean[] claimed = new boolean[leaves];
    int previous = -1;
    int morsels = 0;
    for (int start = spill.claimLeaves(leaves, morsel); start < leaves; start = spill.claimLeaves(leaves, morsel)) {
      assertTrue(start > previous, "morsels come in leaf order");
      assertEquals(0, start % morsel, "a morsel starts on a morsel boundary — one decoded window per column");
      for (int leaf = start; leaf < Math.min(start + morsel, leaves); leaf++) {
        assertFalse(claimed[leaf], "row group " + leaf + " dealt twice");
        claimed[leaf] = true;
      }
      previous = start;
      morsels++;
    }
    assertEquals(16, morsels, "ceil(1000 / 64) morsels");
    for (int leaf = 0; leaf < leaves; leaf++) {
      assertTrue(claimed[leaf], "row group " + leaf + " never dealt");
    }
    // Exhausted: every later claim, from any worker, is the end — and never a value past it.
    assertEquals(leaves, spill.claimLeaves(leaves, morsel));
    assertEquals(leaves, spill.claimLeaves(leaves, morsel));
    assertTrue(spill.leavesClaimed() >= leaves);
    assertThrows(IllegalArgumentException.class, () -> spill.claimLeaves(leaves, 0));
    assertThrows(IllegalArgumentException.class, () -> spill.claimLeaves(-1, morsel));
    // A fresh spill (a new pass) starts over.
    assertEquals(0, new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, true, 0L, 0)).claimLeaves(leaves, morsel));
  }

  @Test
  @DisplayName("concurrent claimers partition the row groups without a gap or an overlap")
  void concurrentClaimersPartitionTheRowGroups() throws Exception {
    final GroupTableSpill spill = new GroupTableSpill(32, 59, () -> new NumericGroupAggTable(1, 16, true, 0L, 0));
    final int leaves = 100_000;
    final int morsel = 64;
    final int workers = 8;
    final AtomicIntegerArray owner = new AtomicIntegerArray(leaves);
    final int[] perWorker = new int[workers];
    final Thread[] threads = new Thread[workers];
    for (int w = 0; w < workers; w++) {
      final int me = w + 1;
      threads[w] = new Thread(() -> {
        for (int start = spill.claimLeaves(leaves, morsel); start < leaves; start = spill.claimLeaves(leaves, morsel)) {
          for (int leaf = start; leaf < Math.min(start + morsel, leaves); leaf++) {
            if (!owner.compareAndSet(leaf, 0, me)) {
              throw new AssertionError("row group " + leaf + " claimed twice");
            }
          }
          perWorker[me - 1]++;
        }
      });
      threads[w].start();
    }
    for (final Thread t : threads) {
      t.join();
    }
    for (int leaf = 0; leaf < leaves; leaf++) {
      assertTrue(owner.get(leaf) != 0, "row group " + leaf + " never claimed");
    }
    int morsels = 0;
    for (final int n : perWorker) {
      morsels += n;
    }
    assertEquals((leaves + morsel - 1) / morsel, morsels);
  }
}
