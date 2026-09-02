package io.sirix.index.projection;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Bounds the per-worker group tables of a parallel group-by by spilling them into SHARED partition
 * tables.
 *
 * <p>
 * A parallel group-by hands every worker its own {@link NumericGroupAggTable}, so a group that all
 * workers see is held once per worker: memory is {@code workers × groups}, which for a million-group
 * string key at 100M rows is several GB before the post-scan merge even starts — the heap exhaustion
 * that sent q13 ({@code SearchPhrase, COUNT(DISTINCT UserID)}) to the interpreter. Here a worker checks
 * its table every {@link #SUB_CHUNK_LEAVES} row groups and, past {@link #flushGroups()} groups, merges
 * it into the partition tables under each partition's monitor and starts a fresh one: resident state
 * is {@code groups + workers × flushGroups}. The post-scan merge takes a partition's shared table as
 * its base ({@link #takeOrCreate}) so nothing is copied twice, and the merge itself is the same
 * {@link NumericGroupAggTable#mergePartitionIndexed} the post-scan already runs — first-seen ordinals,
 * exact sums, identity lanes and the zero-key group all merge the way they always did.
 * </p>
 */
public final class GroupTableSpill {

  /** Configured flush threshold in groups per worker table. */
  public static final String FLUSH_GROUPS_PROPERTY = "sirix.projection.groupTable.flushGroups";

  /** Row groups a worker scans between two threshold checks. */
  public static final int SUB_CHUNK_LEAVES = 64;

  private static volatile int subChunkLeavesForTesting = -1;

  /** The sub-chunk in effect: {@link #SUB_CHUNK_LEAVES}, or the test override. */
  public static int subChunkLeaves() {
    final int testing = subChunkLeavesForTesting;
    return testing > 0
        ? testing
        : SUB_CHUNK_LEAVES;
  }

  /**
   * Test seam: a one-leaf sub-chunk makes a handful of row groups reach flush points.
   *
   * @param value the sub-chunk in row groups, or a non-positive value to restore the default
   * @return the previous override, for restoring in a finally block
   */
  public static int setSubChunkLeavesForTesting(final int value) {
    final int previous = subChunkLeavesForTesting;
    subChunkLeavesForTesting = value;
    return previous;
  }

  private static final int DEFAULT_FLUSH_GROUPS = 1 << 18;
  private static volatile int flushGroupsForTesting = -1;
  private static final LongAdder FLUSHES = new LongAdder();
  private static final LongAdder RELEASES = new LongAdder();

  /** The threshold in effect: the property when set, else 2^18 groups (≈ 25 MB per worker). */
  public static int flushGroups() {
    final int testing = flushGroupsForTesting;
    if (testing >= 0) {
      return testing;
    }
    return Math.max(1, Integer.getInteger(FLUSH_GROUPS_PROPERTY, DEFAULT_FLUSH_GROUPS));
  }

  /**
   * Test seam: a tiny threshold makes every group-by spill constantly.
   *
   * @param value the threshold, or a negative value to restore the default
   * @return the previous override, for restoring in a finally block
   */
  public static int setFlushGroupsForTesting(final int value) {
    final int previous = flushGroupsForTesting;
    flushGroupsForTesting = value;
    return previous;
  }

  /** Test observability: flushes performed process-wide. */
  public static long flushCount() {
    return FLUSHES.sum();
  }

  /** Spills whose shared tables were dropped by {@link #releaseTables} (test observability). */
  public static long releaseCount() {
    return RELEASES.sum();
  }

  /** Configured ceiling on resident groups per pass (default heap-derived). */
  public static final String GROUP_BUDGET_PROPERTY = "sirix.projection.groupTable.groupBudget";
  /** Approximate heap bytes per resident group across stripe, aux, identity lanes and probe slack. */
  private static final long BYTES_PER_GROUP = 128L;
  private static volatile long groupBudgetForTesting = -1L;

  /**
   * The resident-group ceiling per pass: the property when set, else the smaller of an eighth of the
   * heap and a quarter of the current {@link HeapHeadroom} at {@value #BYTES_PER_GROUP} bytes per
   * group, floored at 2^20 and capped at 2^26 groups. The headroom term is what lets a query late in
   * a leg — after earlier queries retained fills, fingerprints and windows — split into more passes
   * instead of dying in a worker.
   */
  public static long groupBudget() {
    final long testing = groupBudgetForTesting;
    if (testing >= 0L) {
      return testing;
    }
    final long configured = Long.getLong(GROUP_BUDGET_PROPERTY, -1L);
    if (configured > 0L) {
      return configured;
    }
    return groupBudgetFor(Runtime.getRuntime().maxMemory(), HeapHeadroom.headroomBytes());
  }

  /**
   * The derived budget for {@code maxMemory} and {@code headroom} bytes (pure, for tests) — the
   * shared {@link HeapHeadroom#plannedShareBytes(long, long)} at {@value #BYTES_PER_GROUP} bytes per
   * group. The share is NOT recomputed here: the residency budget and the distinct ceiling read the
   * same figure, and a second copy of the arithmetic is how they would drift apart.
   */
  public static long groupBudgetFor(final long maxMemory, final long headroom) {
    final long planned = HeapHeadroom.plannedShareBytes(maxMemory, headroom) / BYTES_PER_GROUP;
    return Math.max(1L << 20, Math.min(1L << 26, planned));
  }

  /**
   * The budget a CLEAN heap of this size yields — {@link #groupBudgetFor} at a headroom of the whole
   * heap — or the fixed figure when the property or the test override sets one. A plan compares the
   * budget it read against this to decide whether a collection could widen it: the two collector
   * records a budget derives from are upper bounds on the live set, and right after a large grouped
   * query both still count that query's dead tables (q32 at 100M planned at a sixth of the ceiling,
   * and ran thirty-two passes where eight held).
   */
  public static long groupBudgetCeiling() {
    final long testing = groupBudgetForTesting;
    if (testing >= 0L) {
      return testing;
    }
    final long configured = Long.getLong(GROUP_BUDGET_PROPERTY, -1L);
    if (configured > 0L) {
      return configured;
    }
    final long maxMemory = Runtime.getRuntime().maxMemory();
    return groupBudgetFor(maxMemory, maxMemory);
  }

  /**
   * Partitions the largest of {@code passes} BALANCED hash-range passes over {@code partitions} owns:
   * pass {@code p} owns {@code [passLo(p), passHi(p))}, consecutive ranges whose sizes differ by at
   * most one — so any pass count up to the partition count is admissible, not only the powers of two
   * that divide it. At 100M q18 (~50M groups, a 12.58M budget) that is five passes where the powers
   * of two offered four (too few, the abort) or eight.
   */
  public static int largestPassShare(final int partitions, final int passes) {
    return (partitions + passes - 1) / passes;
  }

  /** First partition (inclusive) of pass {@code pass} of {@code passes} balanced passes. */
  public static int passLo(final int partitions, final int passes, final int pass) {
    return (int) ((long) pass * partitions / passes);
  }

  /** Last partition (exclusive) of pass {@code pass} of {@code passes} balanced passes. */
  public static int passHi(final int partitions, final int passes, final int pass) {
    return (int) ((long) (pass + 1) * partitions / passes);
  }

  /**
   * The fewest balanced passes whose LARGEST share holds {@code groups} uniformly hashed groups within
   * {@code budget}: one pass while the count fits the budget, else the smallest count whose largest
   * share's expected groups fit, at most one pass per partition (where the caller's abort machinery
   * decides what a pass cannot hold).
   */
  public static int passesFor(final long groups, final long budget, final int partitions) {
    if (groups <= budget) {
      return 1;
    }
    for (int passes = 2; passes < partitions; passes++) {
      if (expectedLargestPass(groups, passes, partitions) <= budget) {
        return passes;
      }
    }
    return partitions;
  }

  /** The groups the largest of {@code passes} balanced passes over {@code groups} is expected to hold. */
  public static long expectedLargestPass(final long groups, final int passes, final int partitions) {
    final long share = largestPassShare(partitions, passes);
    return (groups * share + partitions - 1) / partitions;
  }

  /**
   * The smallest group count for which {@link #passesFor} answers at least {@code passes} at
   * {@code budget}: the count that makes {@code passes - 1} passes insufficient. A completed pass set
   * whose SEED aborted records this instead of its exact count, so the next seeding lands on the
   * pass count that completed instead of the one that aborted again.
   */
  public static long groupsForcingPasses(final int passes, final long budget, final int partitions) {
    if (passes <= 1) {
      return 0L;
    }
    final long share = largestPassShare(partitions, Math.min(passes, partitions) - 1);
    if (budget > Long.MAX_VALUE / partitions) {
      return Long.MAX_VALUE;
    }
    return budget * partitions / share + 1L;
  }

  private static volatile boolean simulateOutOfMemoryOnFlush;
  private static final LongAdder OUT_OF_MEMORY_ABORTS = new LongAdder();

  /**
   * Test seam: the next flush throws a synthetic {@link OutOfMemoryError} once, so the arms' restart
   * on a worker OOM can be exercised without exhausting a heap.
   */
  public static void setSimulateOutOfMemoryOnFlushForTesting(final boolean simulate) {
    simulateOutOfMemoryOnFlush = simulate;
  }

  /** Test observability: passes aborted because a worker ran out of memory. */
  public static long outOfMemoryAbortsCount() {
    return OUT_OF_MEMORY_ABORTS.sum();
  }

  /**
   * A worker failure that is an {@link OutOfMemoryError} (anywhere in the cause chain) aborts the
   * pass like an over-budget table does — the arm restarts with more passes, each keeping fewer
   * groups — unless the key space is already split one pass per partition, where the failure stands.
   *
   * @param failure the scan failure
   * @param currentPasses the passes the arm is running
   * @return {@code true} when the pass was aborted and the arm should restart
   */
  public boolean abortOnOutOfMemory(final Throwable failure, final int currentPasses) {
    Throwable cause = failure;
    boolean outOfMemory = false;
    while (cause != null) {
      if (cause instanceof OutOfMemoryError) {
        outOfMemory = true;
        break;
      }
      cause = cause.getCause() == cause
          ? null
          : cause.getCause();
    }
    if (!outOfMemory || currentPasses >= partitions) {
      return false;
    }
    aborted = true;
    OUT_OF_MEMORY_ABORTS.increment();
    return true;
  }

  /**
   * Test seam: a tiny per-pass budget forces the hash-range passes on a small corpus.
   *
   * @param value the budget in groups, or a negative value to restore the derived default
   * @return the previous override, for restoring in a finally block
   */
  public static long setGroupBudgetForTesting(final long value) {
    final long previous = groupBudgetForTesting;
    groupBudgetForTesting = value;
    return previous;
  }

  private final NumericGroupAggTable[] shared;
  private final Object[] locks;
  private final int partitions;
  private final int shift;
  private final Supplier<NumericGroupAggTable> factory;
  private final int threshold;
  private final int passLo;
  private final int passHi;
  private final long budget;
  private final LongAdder spilled = new LongAdder();
  private final LongAdder abandoned = new LongAdder();
  private final LongAdder leavesScanned = new LongAdder();
  private volatile boolean aborted;

  /**
   * @param partitions the post-scan partition count (a power of two)
   * @param shift the partition shift, as {@link NumericGroupAggTable#partitionOf} expects
   * @param factory builds a table with exactly the worker tables' layout (columns, aux, exactness,
   *        identity width) — shared and fresh worker tables alike come from it
   */
  public GroupTableSpill(final int partitions, final int shift, final Supplier<NumericGroupAggTable> factory) {
    this(partitions, shift, factory, 0, partitions, Long.MAX_VALUE);
  }

  /**
   * A spill for ONE hash-range pass: the worker tables it hands out keep only the groups of the
   * partitions {@code [passLo, passHi)}, and once more than {@code budget} groups have been spilled
   * the pass {@link #aborted() aborts} so the caller can restart with more passes.
   */
  public GroupTableSpill(final int partitions, final int shift, final Supplier<NumericGroupAggTable> factory,
      final int passLo, final int passHi, final long budget) {
    if (partitions <= 0 || (partitions & (partitions - 1)) != 0) {
      throw new IllegalArgumentException("partitions must be a power of two: " + partitions);
    }
    if (passLo < 0 || passHi <= passLo || passHi > partitions) {
      throw new IllegalArgumentException("pass range [" + passLo + ", " + passHi + ") over " + partitions);
    }
    if (budget <= 0L) {
      throw new IllegalArgumentException("budget must be positive: " + budget);
    }
    this.partitions = partitions;
    this.shift = shift;
    this.factory = requireNonNull(factory, "factory");
    this.passLo = passLo;
    this.passHi = passHi;
    this.budget = budget;
    this.shared = new NumericGroupAggTable[partitions];
    this.locks = new Object[partitions];
    for (int p = 0; p < partitions; p++) {
      locks[p] = new Object();
    }
    this.threshold = flushGroups();
  }

  /** A fresh worker table from the factory, restricted to this pass's partitions. */
  public NumericGroupAggTable freshLocal() {
    final NumericGroupAggTable table = factory.get();
    if (passLo != 0 || passHi != partitions) {
      table.setPassRange(shift, passLo, passHi);
    }
    return table;
  }

  /** Whether this spill covers only part of the key space. */
  public boolean isPartialPass() {
    return passLo != 0 || passHi != partitions;
  }

  /** First partition (inclusive) of this pass. */
  public int passLo() {
    return passLo;
  }

  /** Last partition (exclusive) of this pass. */
  public int passHi() {
    return passHi;
  }

  /** Whether {@code part} belongs to this pass. */
  public boolean ownsPartition(final int part) {
    return part >= passLo && part < passHi;
  }

  /** Whether the resident-group budget was exceeded: the caller restarts with more passes. */
  public boolean aborted() {
    return aborted;
  }

  /** Groups spilled into the shared tables so far (flushed batches only). */
  public long groupsSpilled() {
    return spilled.sum();
  }

  /** A worker reports {@code leaves} row groups scanned; drives the pass estimate on abort. */
  public void noteLeavesScanned(final int leaves) {
    leavesScanned.add(leaves);
  }

  /** Row groups the workers reported scanned so far. */
  public long leavesScanned() {
    return leavesScanned.sum();
  }

  /**
   * A worker that leaves an aborted pass reports the groups of the local table it drops, and the arm
   * reports the final tables of the workers that had finished before the abort: those groups were
   * never flushed, so {@link #groupsSpilled} alone undercounts what the pass had seen by up to
   * {@code workers × flushGroups} — at 100M the pass estimate was low by a fifth, and a pass count
   * seeded from it aborted again.
   */
  public void noteAbandonedLocal(final int groups) {
    if (groups > 0) {
      abandoned.add(groups);
    }
  }

  /** Groups reported through {@link #noteAbandonedLocal} (test observability). */
  public long groupsAbandoned() {
    return abandoned.sum();
  }

  /**
   * The pass count an abort recommends at {@code budget}: the fewest balanced passes that hold the
   * {@link #estimatedTotalGroups estimated total}, at most one pass per partition. No floor above the
   * aborted count here — the caller knows whether the budget it restarts with is the one that
   * aborted (then at least one more pass) or a refreshed, wider one (then the fit as it is).
   */
  public int recommendedPasses(final int totalLeaves, final long budget) {
    return Math.min(partitions, passesFor(estimatedTotalGroups(totalLeaves), budget, partitions));
  }

  /**
   * The abort-time total-group estimate: the groups this pass saw (spilled plus abandoned in worker
   * tables), extrapolated over the unscanned leaves and over the partitions this pass did not own —
   * exposed so the caller can memo it on the handle and seed the NEXT execution's pass count directly,
   * skipping the aborted scan this execution already paid for.
   */
  public long estimatedTotalGroups(final int totalLeaves) {
    final double fraction = Math.max(0.01, (double) leavesScanned.sum() / Math.max(1, totalLeaves));
    final double seen = spilled.sum() + abandoned.sum();
    return (long) (seen / fraction * partitions / (passHi - passLo));
  }

  /** Whether {@code local} has grown past the threshold and should be flushed. */
  public boolean shouldFlush(final NumericGroupAggTable local) {
    return local.size() >= threshold;
  }

  /**
   * Merge every group of {@code local} into the shared partition tables. The caller drops
   * {@code local} afterwards (it is not cleared here: a fresh table is cheaper than a reset of a
   * grown one, and the old one becomes garbage at once).
   */
  public void flush(final NumericGroupAggTable local) {
    if (simulateOutOfMemoryOnFlush) {
      simulateOutOfMemoryOnFlush = false;
      throw new OutOfMemoryError("simulated: GroupTableSpill flush (test seam)");
    }
    final int[][] index = local.buildPartitionIndex(partitions, shift);
    final NumericGroupAggTable[] sources = {local};
    final int[][][] indexes = {index};
    for (int p = 0; p < partitions; p++) {
      if (index[p].length == 0 && !(p == 0 && local.hasZeroKey())) {
        continue;
      }
      synchronized (locks[p]) {
        NumericGroupAggTable target = shared[p];
        if (target == null) {
          target = factory.get();
          shared[p] = target;
        }
        final int before = target.size();
        NumericGroupAggTable.mergePartitionIndexed(sources, indexes, p, target);
        spilled.add(target.size() - before);
      }
    }
    FLUSHES.increment();
    if (spilled.sum() > budget) {
      aborted = true;
    }
  }

  /**
   * Drop the shared partition tables of an ABORTED pass. The estimate and the pass recommendation need
   * only the counters ({@link #groupsSpilled}, {@link #groupsAbandoned}, leaves scanned), but the
   * tables stay reachable through this spill until the arm has re-planned — and a restart that refreshes
   * the budget by a forced collection measures whatever is still REFERENCED, not what the arm intends
   * to keep: at 100M (q32) the aborted pass's 16.6M spilled groups read as 3.9 GB of live heap, the
   * budget FELL 11.5M → 7.9M and the restart ran 16 passes instead of 8. Call after the parallel
   * section has joined and before re-planning; the spill is not reused.
   */
  public void releaseTables() {
    for (int p = 0; p < partitions; p++) {
      synchronized (locks[p]) {
        shared[p] = null;
      }
    }
    RELEASES.increment();
  }

  /**
   * The shared table of {@code part} as the post-scan merge base — detached, so it is merged into
   * exactly once — or a fresh table from {@code fresh} when nothing was spilled there. Post-scan only,
   * after the parallel section has joined.
   */
  public NumericGroupAggTable takeOrCreate(final int part, final Supplier<NumericGroupAggTable> fresh) {
    final NumericGroupAggTable table;
    synchronized (locks[part]) {
      table = shared[part];
      shared[part] = null;
    }
    return table != null
        ? table
        : requireNonNull(fresh, "fresh").get();
  }
}
