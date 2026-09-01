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

  /**
   * The pass count to restart with after an abort: the groups seen so far, extrapolated over the
   * unscanned leaves and the partitions this pass did not own, divided by the budget — rounded up to
   * a power of two, at least twice the current count and at most one pass per partition.
   */
  public int recommendedPasses(final int currentPasses, final int totalLeaves) {
    final double fraction = Math.max(0.01, (double) leavesScanned.sum() / Math.max(1, totalLeaves));
    final double estimated = groupsSpilled() / fraction * currentPasses;
    long passes = 1L;
    while (passes * budget < estimated) {
      passes <<= 1;
    }
    return (int) Math.min(partitions, Math.max((long) currentPasses * 2L, passes));
  }

  /**
   * The abort-time total-group estimate {@link #recommendedPasses} divides by the budget — exposed
   * so the caller can memo it on the handle and seed the NEXT execution's pass count directly,
   * skipping the aborted scan this execution already paid for.
   */
  public long estimatedTotalGroups(final int currentPasses, final int totalLeaves) {
    final double fraction = Math.max(0.01, (double) leavesScanned.sum() / Math.max(1, totalLeaves));
    return (long) (groupsSpilled() / fraction * currentPasses);
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
