package io.sirix.index.projection;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
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
 *
 * <p>
 * Every table the spill hands out or creates shares one {@link LongChunkPool}: a flushed worker table
 * {@link NumericGroupAggTable#release releases} its chunks, a growing partition table recycles the
 * ones it outgrew, and the caller releases the merged tables once a pass has emitted its candidates —
 * so a pass allocates its storage once and G1 promotes it once, instead of copying ≈ 9 GB of
 * short-lived tables through the young generation per pass (the q32 profile at 100M). Kill switch
 * {@code -Dsirix.projection.groupTable.chunkPool=false}.
 * </p>
 *
 * <p>
 * The workers' tables fill at the same rate, so their flushes arrive together — and a flush that
 * walked the partitions in one fixed order made every worker queue on the same monitor as the one
 * ahead of it: a lock convoy that parked a fifth of the workers' wall at 100M (q32's wall profile),
 * with each shared table's rehashes (six of them, 600 MB copied per partition, from a worker-sized
 * hint) taken under that lock while the queue waited. Two answers, one kill switch each: every flush
 * starts its walk at a different partition ({@link #flushStart}, {@code
 * -Dsirix.projection.groupTable.flushOffset=false}), and a shared table is created at the count the
 * caller's plan expects it to hold ({@link #sharedTableHint}, {@code
 * -Dsirix.projection.groupTable.presizeShared=false}) — witnessed per spill by {@link
 * #sharedRehashes}, which a taken pre-size leaves at zero.
 * </p>
 *
 * <p>
 * The pass's row groups are dealt to the workers from one shared cursor ({@link #claimLeaves}), a
 * morsel of {@link #SUB_CHUNK_LEAVES} at a time, not as a fixed slice of the leaf range per worker.
 * The cost of a row group is a property of the DATA: a leaf range dense in the grouped strings runs
 * about twice as slow as its neighbours (at 100M, worker 8's slice finished every pass of q16 and q18
 * 45–57 % after the median worker, with the same slice slowest in every pass and every try), and a
 * static partition ends its scan with the slowest slice. A shared cursor ends it within one morsel of
 * the mean, and the workers sweep the file together instead of from twenty starting points.
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

  /** Recycle table chunks through a per-spill {@link LongChunkPool} (default on). */
  public static final String CHUNK_POOL_PROPERTY = "sirix.projection.groupTable.chunkPool";
  private static volatile int chunkPoolForTesting = -1;

  /** Whether spills recycle their tables' chunks: the test override when set, else the property. */
  public static boolean chunkPoolEnabled() {
    final int testing = chunkPoolForTesting;
    if (testing >= 0) {
      return testing != 0;
    }
    return Boolean.parseBoolean(System.getProperty(CHUNK_POOL_PROPERTY, "true"));
  }

  /**
   * Test seam for the chunk pool.
   *
   * @param value {@code 1} on, {@code 0} off, negative restores the property
   * @return the previous override, for restoring in a finally block
   */
  public static int setChunkPoolForTesting(final int value) {
    final int previous = chunkPoolForTesting;
    chunkPoolForTesting = value;
    return previous;
  }

  /** Start every flush's partition walk at a different partition (default on). */
  public static final String FLUSH_OFFSET_PROPERTY = "sirix.projection.groupTable.flushOffset";
  /** Create a shared partition table at the count the plan expects it to hold (default on). */
  public static final String PRESIZE_SHARED_PROPERTY = "sirix.projection.groupTable.presizeShared";
  /** The sizing hint every worker table gets: it flushes long before it would outgrow this twice. */
  public static final int WORKER_TABLE_HINT = 1 << 16;

  /**
   * Configured worker-table sizing hint, in groups. The DEFAULT is
   * {@link #WORKER_TABLE_HINT}, which predates the stripe spill: back then a flush meant probing every
   * group into a shared table far larger than any cache, so flushing rarely was worth a big local
   * table. Now a flush is a sequential stripe copy with a compaction behind it, and the trade has
   * inverted — a table small enough to PROBE out of cache is worth flushing more often for.
   *
   * <p>
   * The probe is where the suite's CPU is: {@code NumericGroupAggTable.acquireExact} alone is 17 % of
   * a 43-query leg, and at 65,536 groups a stripe-15 table is ~7.9 MB, well past L2.
   * </p>
   *
   * <p>
   * <b>MEASURED AND REFUTED (2026-09-03), which is why the default is unchanged.</b> At 100M, hot
   * seconds for q32+q18+q13: <b>65,536 → 12.557 s</b>, 16,384 → 14.238, 8,192 → 14.275, 4,096 →
   * 14.453. Monotonically worse. Cache residency is real but it is not what dominates: a smaller table
   * flushes more often, and every flush drags a COMPACTION that probes the partition table anyway — so
   * shrinking the local table moves the misses rather than removing them, and adds the stripe copies
   * on top. The knob stays because it is how that was learned, not because a smaller value is wanted.
   * </p>
   */
  public static final String WORKER_HINT_PROPERTY = "sirix.projection.groupTable.workerHint";

  /** The worker sizing hint in effect. */
  public static int workerTableHint() {
    final int configured = Integer.getInteger(WORKER_HINT_PROPERTY, WORKER_TABLE_HINT);
    return Math.max(16, configured);
  }
  /** Skew allowance on a shared table's expected share, in percent (a partition's count is ± its root). */
  private static final int SHARED_HINT_SKEW_PCT = 5;
  /**
   * Standard deviations of a hash partition's count (≈ √share) the hint must still cover when the
   * percent allowance alone would double the table's capacity; eight roots (p < 1e-15 for a binomial
   * split) is never exceeded by placement, so the residual risk is the estimate's own error — one
   * rehash, not a doubled pass.
   */
  private static final int SHARED_HINT_TIGHT_ROOTS = 8;
  private static volatile int flushOffsetForTesting = -1;
  private static volatile int presizeSharedForTesting = -1;
  private static final LongAdder PRESIZED_SHARED = new LongAdder();

  /** Whether flushes start at rotating partitions: the test override when set, else the property. */
  public static boolean flushOffsetEnabled() {
    final int testing = flushOffsetForTesting;
    if (testing >= 0) {
      return testing != 0;
    }
    return Boolean.parseBoolean(System.getProperty(FLUSH_OFFSET_PROPERTY, "true"));
  }

  /** Whether shared tables are created at the plan's expected count: the override when set, else the property. */
  public static boolean presizeSharedEnabled() {
    final int testing = presizeSharedForTesting;
    if (testing >= 0) {
      return testing != 0;
    }
    return Boolean.parseBoolean(System.getProperty(PRESIZE_SHARED_PROPERTY, "true"));
  }

  /**
   * Test seam for the rotating flush start.
   *
   * @param value {@code 1} on, {@code 0} off, negative restores the property
   * @return the previous override, for restoring in a finally block
   */
  public static int setFlushOffsetForTesting(final int value) {
    final int previous = flushOffsetForTesting;
    flushOffsetForTesting = value;
    return previous;
  }

  /**
   * Test seam for the shared-table pre-size.
   *
   * @param value {@code 1} on, {@code 0} off, negative restores the property
   * @return the previous override, for restoring in a finally block
   */
  public static int setPresizeSharedForTesting(final int value) {
    final int previous = presizeSharedForTesting;
    presizeSharedForTesting = value;
    return previous;
  }

  /** Shared partition tables created at a plan-derived hint, process-wide (test observability). */
  public static long presizedSharedCount() {
    return PRESIZED_SHARED.sum();
  }

  /**
   * Spill a worker table as RAW STRIPES into per-partition append buffers and build each partition's
   * table once after the scan, instead of probing every flushed group into a shared partition table
   * (default on).
   */
  public static final String STRIPE_SPILL_PROPERTY = "sirix.projection.groupTable.stripeSpill";
  /** Flush threshold in groups per worker table under the stripe spill when the property sets none. */
  public static final int STRIPE_FLUSH_GROUPS = WORKER_TABLE_HINT;
  /** Stripes a partition buffers before a compaction, whatever the budget says (a batch worth probing). */
  private static final long MIN_COMPACT_STRIPES = 4_096L;
  private static volatile int stripeSpillForTesting = -1;
  private static final LongAdder STRIPES_SPILLED = new LongAdder();
  private static final LongAdder COMPACTIONS = new LongAdder();

  /** Whether spills copy stripes into partition buffers: the test override when set, else the property. */
  public static boolean stripeSpillEnabled() {
    final int testing = stripeSpillForTesting;
    if (testing >= 0) {
      return testing != 0;
    }
    return Boolean.parseBoolean(System.getProperty(STRIPE_SPILL_PROPERTY, "true"));
  }

  /**
   * Test seam for the stripe spill.
   *
   * @param value {@code 1} on, {@code 0} off, negative restores the property
   * @return the previous override, for restoring in a finally block
   */
  public static int setStripeSpillForTesting(final int value) {
    final int previous = stripeSpillForTesting;
    stripeSpillForTesting = value;
    return previous;
  }

  /** Stripes copied into partition buffers process-wide (test observability). */
  public static long stripesSpilledCount() {
    return STRIPES_SPILLED.sum();
  }

  /** Buffer compactions into a shared partition table process-wide (test observability). */
  public static long compactionCount() {
    return COMPACTIONS.sum();
  }

  /**
   * Append-only run of whole stripes for ONE partition: a stripe never crosses a chunk, and the
   * chunks GROW from a handful of stripes to the spill's {@link LongChunkPool} length, so a shape
   * with thousands of partitions and few groups costs stripes rather than chunks while a partition
   * that fills recycles pooled chunks like the tables. Written under the partition's monitor, read
   * by the one worker that builds the partition after the scan has joined — so it carries no
   * synchronisation of its own.
   *
   * <p>
   * Why a buffer and not the shared table it replaces: a flush that probes {@code flushGroups}
   * groups into a shared table of the pass's share pays a cache miss per group on a table far
   * larger than any cache (at 100M, q32's 62 MB partition tables: ≈ 400 ns per group, 24 % of the
   * query's CPU, plus the local table's own misses and its rehashes — and the shared table's
   * chunks then rehash under the partition's monitor). A stripe copy is one sequential
   * {@code stride}-lane write; the partition's table is built ONCE from the buffer, hinted at its
   * exact stripe count so it never rehashes, and a high-cardinality shape (a group per row) pays
   * one probe per group instead of two plus a merge.
   */
  static final class StripeBuffer {
    /** Stripes the FIRST chunk of a buffer holds; a partition that stays small costs only this. */
    private static final int FIRST_CHUNK_STRIPES = 8;
    private final int stride;
    /** The pool's chunk length: the size every chunk past the growth ramp is allocated at. */
    private final int fullLanes;
    private long[][] chunks = new long[4][];
    private int[] used = new int[4];
    private int chunkCount;
    private long stripes;

    StripeBuffer(final int stride, final int fullLanes) {
      if (stride <= 0 || fullLanes < stride) {
        throw new IllegalArgumentException("stride " + stride + " over chunk lanes " + fullLanes);
      }
      this.stride = stride;
      this.fullLanes = fullLanes - fullLanes % stride;
    }

    /**
     * Lanes the next chunk is allocated at: the previous chunk doubled, from
     * {@value #FIRST_CHUNK_STRIPES} stripes up to the pool's chunk — so a partition holding a
     * handful of groups costs a handful of stripes, and one holding millions still reaches the
     * pooled size within a few chunks. Always a whole number of stripes.
     */
    private int nextChunkLanes() {
      if (chunkCount == 0) {
        return Math.min(fullLanes, stride * FIRST_CHUNK_STRIPES);
      }
      final int doubled = chunks[chunkCount - 1].length << 1;
      return doubled <= 0 || doubled >= fullLanes
          ? fullLanes
          : doubled - doubled % stride;
    }

    /** Copy the stripe whose accumulator base is {@code accBase} of {@code src} (key lane at base − 1). */
    void append(final long[] src, final int accBase, final LongChunkPool pool) {
      final int st = stride;
      if (chunkCount == 0 || used[chunkCount - 1] + st > chunks[chunkCount - 1].length) {
        if (chunkCount == chunks.length) {
          final long[][] grown = new long[chunkCount << 1][];
          System.arraycopy(chunks, 0, grown, 0, chunkCount);
          chunks = grown;
          final int[] grownUsed = new int[chunkCount << 1];
          System.arraycopy(used, 0, grownUsed, 0, chunkCount);
          used = grownUsed;
        }
        final int lanes = nextChunkLanes();
        chunks[chunkCount] = pool != null && lanes == pool.chunkLanes()
            ? pool.take()
            : new long[lanes];
        used[chunkCount] = 0;
        chunkCount++;
      }
      final int tail = chunkCount - 1;
      System.arraycopy(src, accBase - 1, chunks[tail], used[tail], st);
      used[tail] += st;
      stripes++;
    }

    long stripes() {
      return stripes;
    }

    int chunkCount() {
      return chunkCount;
    }

    long[] chunk(final int index) {
      return chunks[index];
    }

    /** Lanes of whole stripes in chunk {@code index}. */
    int usedLanes(final int index) {
      return used[index];
    }

    /**
     * Hand every chunk back and empty the buffer. Only chunks at the pool's length are kept by it
     * ({@link LongChunkPool#give} refuses the others); the ramp's small chunks go to the collector.
     */
    void release(final LongChunkPool pool) {
      for (int i = 0; i < chunkCount; i++) {
        if (pool != null) {
          pool.give(chunks[i]);
        }
        chunks[i] = null;
        used[i] = 0;
      }
      chunkCount = 0;
      stripes = 0L;
    }
  }

  /**
   * The partition the {@code ordinal}-th flush of a spill starts its walk at: the flushes step
   * through the partitions by an odd stride near {@code 0.618 × partitions}, so any {@code
   * partitions} consecutive flushes start at {@code partitions} DISTINCT partitions (an odd stride
   * is coprime to a power of two) and consecutive ones start far apart (the golden-ratio stride is
   * the one whose multiples spread most evenly). Workers that flush together therefore meet on a
   * monitor only by coincidence, not by construction. A spill built with the rotation switched off
   * starts every flush at partition 0 instead ({@link #flushOffset}).
   */
  static int flushStart(final long ordinal, final int partitions) {
    return (int) (ordinal * flushStride(partitions)) & (partitions - 1);
  }

  /** The odd stride of {@link #flushStart} for {@code partitions} (a power of two). */
  static int flushStride(final int partitions) {
    return (int) ((long) partitions * 0x9E37L >>> 16) | 1;
  }

  /**
   * The sizing hint of a shared partition table when the caller's plan knows the shape's group
   * count, or {@code -1} when it does not (or the pre-size is switched off): the uniform share of
   * {@code expectedGroups} per partition plus {@value #SHARED_HINT_SKEW_PCT} percent skew allowance,
   * never past the share of the pass budget — beyond what the budget lets a pass hold, a hint can
   * only be an estimate's error, and the table grows on demand there as before. Storage is chunked
   * and a chunk is allocated when its first group lands — but hashed placement touches every chunk of
   * the capacity, so a hint costs the whole power of two it rounds up to
   * ({@link NumericGroupAggTable#capacityFor}).
   *
   * <p>
   * Hence the allowance is refused when IT ALONE crosses that boundary: at 100M rows a share of
   * 3,124,927 groups fits a 2^22-bucket table (grows at 3,145,728) and its 5 % allowance asked for
   * 2^23 — every shared table of every pass doubled, 2.1 GB of fresh chunks per pass past the retained
   * pool, and the hot tries ran SLOWER per pass than the cold one (q32: 1.5 → 1.8 s, gc 32 → 42). A
   * partition's count deviates from its share by about its root, so the tighter hint keeps
   * {@value #SHARED_HINT_TIGHT_ROOTS} roots of headroom and lets an estimate that is truly off pay one
   * rehash instead.
   */
  static int sharedTableHint(final long expectedGroups, final int partitions, final int passLo, final int passHi,
      final long budget) {
    if (expectedGroups <= 0L || !presizeSharedEnabled()) {
      return -1;
    }
    final long share = Math.min(expectedGroups / partitions, Integer.MAX_VALUE);
    final long bound = budget / (passHi - passLo);
    long hinted = Math.min(share + share * SHARED_HINT_SKEW_PCT / 100L, bound);
    final long tight = Math.min(share + SHARED_HINT_TIGHT_ROOTS * (long) Math.ceil(Math.sqrt((double) share)), bound);
    if (tight < hinted && capacityOf(hinted) > capacityOf(tight)) {
      hinted = tight;
    }
    return (int) Math.max(16L, Math.min(Integer.MAX_VALUE, hinted));
  }

  /** {@link NumericGroupAggTable#capacityFor} over the hint's saturated int. */
  private static int capacityOf(final long hint) {
    return NumericGroupAggTable.capacityFor((int) Math.max(16L, Math.min(Integer.MAX_VALUE, hint)));
  }

  /**
   * Chunks a spill's pool keeps at most: twice the resident state of a pass — {@code budget} groups
   * plus the workers' unflushed tables — at the table's 3/4 load factor. The pool never holds more than
   * it was given, so this is a safety net against a runaway caller, not a tuning knob.
   */
  static int poolCapacityChunks(final long budget, final int threshold, final int stride, final int chunkLanes) {
    final double groups = Math.min(budget, 1L << 32) + 64.0 * threshold;
    final double lanes = groups * stride * (4.0 / 3.0) * 2.0;
    return (int) Math.max(64, Math.min(1 << 22, Math.ceil(lanes / chunkLanes)));
  }

  /** Configured ceiling on resident groups per pass (default heap-derived). */
  public static final String GROUP_BUDGET_PROPERTY = "sirix.projection.groupTable.groupBudget";
  /** Approximate heap bytes per resident group across stripe, aux, identity lanes and probe slack. */
  private static final long BYTES_PER_GROUP = 128L;

  /**
   * Arms the STRIDE-AWARE per-group cost. Off restores the flat {@value #BYTES_PER_GROUP} bytes every
   * shape used to be charged; it gates BEHAVIOUR (how many passes a plan takes), never a decoder, and
   * a pass count can only ever cost time.
   */
  public static final String STRIDE_BUDGET_PROPERTY = "sirix.projection.groupTable.strideBudget";

  /**
   * Capacity slack over the live group count: {@link NumericGroupAggTable} grows at 3/4 load and
   * rounds its bucket count UP to a power of two, so a table holding {@code n} groups has between
   * 4/3 and 8/3 buckets per group. Two is the midpoint of that range and the figure the budget
   * charges — the ceiling would forfeit a third of the heap share on every shape, and the floor
   * would plan a pass that cannot hold what it planned for.
   */
  private static final long CAPACITY_SLACK = 2L;

  /**
   * Bytes one resident group costs a pass at {@code strideLanes} lanes per stripe — the stripe at
   * {@link #CAPACITY_SLACK}, but never MORE than the flat {@value #BYTES_PER_GROUP} this budget
   * charged before strides were consulted.
   *
   * <p>
   * The clamp is the whole safety argument, and it is deliberately one-sided. A WIDE stripe (q32's
   * two aggregate columns over a composite key occupy thirteen lanes, 208 B at the slack) costs more
   * than the flat figure, and charging it honestly would hand that query MORE passes than the eight
   * it runs today — a measured, working plan made worse by a sizing correction. A NARROW stripe is
   * the case worth fixing: {@code GROUP BY x ORDER BY count(*)} occupies three lanes, 48 B, and has
   * been charged 128 B and split into up to 2.67x the passes its memory required, each surplus pass a
   * full rescan. So the stride may only ever LOWER the charge: no shape can take more passes than it
   * takes today, and shapes that were over-charged take fewer. Wide stripes keep the flat figure and
   * the abort machinery that has always covered them.
   * </p>
   *
   * @param strideLanes lanes per group stripe ({@link NumericGroupAggTable#strideFor})
   * @return bytes charged per resident group
   * @throws IllegalArgumentException if {@code strideLanes} is not positive
   */
  public static long bytesPerGroup(final int strideLanes) {
    if (strideLanes <= 0) {
      throw new IllegalArgumentException("strideLanes must be positive: " + strideLanes);
    }
    if (!Boolean.parseBoolean(System.getProperty(STRIDE_BUDGET_PROPERTY, "true"))) {
      return BYTES_PER_GROUP;
    }
    return Math.min(BYTES_PER_GROUP, (long) strideLanes * Long.BYTES * CAPACITY_SLACK);
  }
  private static volatile long groupBudgetForTesting = -1L;

  /**
   * The resident-group ceiling per pass: the property when set, else the smaller of an eighth of the
   * heap and a quarter of the current {@link HeapHeadroom} at {@value #BYTES_PER_GROUP} bytes per
   * group, floored at 2^20 and capped at 2^26 groups. The headroom term is what lets a query late in
   * a leg — after earlier queries retained fills, fingerprints and windows — split into more passes
   * instead of dying in a worker. The chunks the shared {@link LongChunkPool}s retain count as
   * headroom here: they are the tables of the next pass, taken instead of allocated.
   */
  public static long groupBudget() {
    return groupBudget(WIDEST_CHARGED_STRIDE);
  }

  /**
   * The resident-group ceiling per pass for a shape whose stripe is {@code strideLanes} lanes wide —
   * {@link #groupBudget()} against {@link #bytesPerGroup(int)} rather than the flat figure. A pass
   * planned this way holds the groups its heap share can actually carry, instead of the groups a
   * 128-byte stripe could carry.
   *
   * @param strideLanes lanes per group stripe ({@link NumericGroupAggTable#strideFor})
   * @return resident groups admitted per pass
   */
  public static long groupBudget(final int strideLanes) {
    final long testing = groupBudgetForTesting;
    if (testing >= 0L) {
      return testing;
    }
    final long configured = Long.getLong(GROUP_BUDGET_PROPERTY, -1L);
    if (configured > 0L) {
      return configured;
    }
    // The shared chunk pools' contents are live to every collector record and are exactly the memory
    // the next pass's tables take without allocating: headroom for this budget, for no other.
    return groupBudgetFor(Runtime.getRuntime().maxMemory(),
        HeapHeadroom.headroomBytes() + LongChunkPool.retainedBytes(), strideLanes);
  }

  /**
   * The derived budget for {@code maxMemory} and {@code headroom} bytes (pure, for tests) — the
   * shared {@link HeapHeadroom#plannedShareBytes(long, long)} at {@value #BYTES_PER_GROUP} bytes per
   * group. The share is NOT recomputed here: the residency budget and the distinct ceiling read the
   * same figure, and a second copy of the arithmetic is how they would drift apart.
   */
  public static long groupBudgetFor(final long maxMemory, final long headroom) {
    return groupBudgetFor(maxMemory, headroom, WIDEST_CHARGED_STRIDE);
  }

  /**
   * The derived budget for {@code maxMemory}, {@code headroom} and a stripe of {@code strideLanes}
   * lanes (pure, for tests) — the shared {@link HeapHeadroom#plannedShareBytes(long, long)} at
   * {@link #bytesPerGroup(int)} per group, floored and capped exactly as the flat form is.
   *
   * @param strideLanes lanes per group stripe ({@link NumericGroupAggTable#strideFor})
   */
  public static long groupBudgetFor(final long maxMemory, final long headroom, final int strideLanes) {
    final long planned = HeapHeadroom.plannedShareBytes(maxMemory, headroom) / bytesPerGroup(strideLanes);
    return Math.max(1L << 20, Math.min(1L << 26, planned));
  }

  /**
   * The stripe at which {@link #bytesPerGroup} reaches the flat {@value #BYTES_PER_GROUP} figure, so
   * that the stride-free overloads keep the numbers they have always returned whatever the property
   * says.
   */
  private static final int WIDEST_CHARGED_STRIDE = (int) (BYTES_PER_GROUP / (Long.BYTES * CAPACITY_SLACK));

  /**
   * The budget a CLEAN heap of this size yields — {@link #groupBudgetFor} at a headroom of the whole
   * heap — or the fixed figure when the property or the test override sets one. A plan compares the
   * budget it read against this to decide whether a collection could widen it: the two collector
   * records a budget derives from are upper bounds on the live set, and right after a large grouped
   * query both still count that query's dead tables (q32 at 100M planned at a sixth of the ceiling,
   * and ran thirty-two passes where eight held).
   */
  public static long groupBudgetCeiling() {
    return groupBudgetCeiling(WIDEST_CHARGED_STRIDE);
  }

  /**
   * The clean-heap ceiling for a stripe of {@code strideLanes} lanes. A refresh compares the budget it
   * planned against this, so the two must be charged the SAME bytes per group: a narrow shape whose
   * budget was widened by its stride would otherwise be measured against a ceiling computed for a
   * 128-byte stripe and look as though a collection could not help it.
   *
   * @param strideLanes lanes per group stripe ({@link NumericGroupAggTable#strideFor})
   */
  public static long groupBudgetCeiling(final int strideLanes) {
    final long testing = groupBudgetForTesting;
    if (testing >= 0L) {
      return testing;
    }
    final long configured = Long.getLong(GROUP_BUDGET_PROPERTY, -1L);
    if (configured > 0L) {
      return configured;
    }
    final long maxMemory = Runtime.getRuntime().maxMemory();
    return groupBudgetFor(maxMemory, maxMemory, strideLanes);
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
  /** Per-partition stripe buffers under the stripe spill, or {@code null} under the table spill. */
  private final StripeBuffer[] stripeBuffers;
  /** Layout lanes per stripe, fixed by the factory. */
  private final int stripeStride;
  /** Stripes one partition buffers before they are compacted into its shared table. */
  private final long compactAt;
  /**
   * Under the stripe spill: the zero group's side slots of every flushed worker table, folded under
   * partition 0's monitor. A zero key lane would read as an empty bucket in a buffer, so the zero
   * group travels outside the stripes and lands on the built partition-0 table.
   */
  private NumericGroupAggTable zeroGroups;
  private final int partitions;
  private final int shift;
  private final IntFunction<NumericGroupAggTable> factory;
  /** {@link #sharedTableHint}: the hint a shared table is created at, or {@code -1} for the worker hint. */
  private final int sharedHint;
  private final boolean flushOffset;
  private final AtomicLong flushOrdinal = new AtomicLong();
  /** The next row group no worker has claimed yet; see {@link #claimLeaves}. */
  private final AtomicInteger leafCursor = new AtomicInteger();
  private final LongAdder sharedRehashes = new LongAdder();
  private final int threshold;
  private final int passLo;
  private final int passHi;
  private final long budget;
  /** Shared by every table of this spill, or {@code null} when the pool is switched off. */
  private final LongChunkPool pool;
  private final LongAdder spilled = new LongAdder();
  /** Stripes sitting in the partition buffers: resident state the abort must price, not yet deduplicated. */
  private final LongAdder buffered = new LongAdder();
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
    this(partitions, shift, hint -> requireNonNull(factory, "factory").get(), 0L, passLo, passHi, budget);
  }

  /**
   * A pass spill whose SHARED tables are created at the count the caller's plan expects them to hold.
   *
   * @param factory builds a table of the worker tables' layout at a sizing hint: worker tables are
   *        asked for at {@link #WORKER_TABLE_HINT}, shared partition tables at {@link
   *        #sharedTableHint} when {@code expectedGroups} is known
   * @param expectedGroups the shape's total group count over ALL partitions as the plan expects it
   *        (a memoised or abort-estimated count), or {@code 0} when the plan is blind — shared
   *        tables then start at the worker hint and grow
   */
  public GroupTableSpill(final int partitions, final int shift, final IntFunction<NumericGroupAggTable> factory,
      final long expectedGroups, final int passLo, final int passHi, final long budget) {
    if (partitions <= 0 || (partitions & (partitions - 1)) != 0) {
      throw new IllegalArgumentException("partitions must be a power of two: " + partitions);
    }
    if (passLo < 0 || passHi <= passLo || passHi > partitions) {
      throw new IllegalArgumentException("pass range [" + passLo + ", " + passHi + ") over " + partitions);
    }
    if (budget <= 0L) {
      throw new IllegalArgumentException("budget must be positive: " + budget);
    }
    if (expectedGroups < 0L) {
      throw new IllegalArgumentException("expectedGroups must be >= 0: " + expectedGroups);
    }
    this.partitions = partitions;
    this.shift = shift;
    this.factory = requireNonNull(factory, "factory");
    this.passLo = passLo;
    this.passHi = passHi;
    this.budget = budget;
    this.sharedHint = sharedTableHint(expectedGroups, partitions, passLo, passHi, budget);
    this.flushOffset = flushOffsetEnabled();
    this.shared = new NumericGroupAggTable[partitions];
    this.locks = new Object[partitions];
    for (int p = 0; p < partitions; p++) {
      locks[p] = new Object();
    }
    final boolean stripeSpill = stripeSpillEnabled();
    // Under the stripe spill a flush is a sequential copy, so the worker table stays at the hint it
    // was created at (no rehash, a cache-sized probe) unless the property asks for more.
    this.threshold = stripeSpill && flushGroupsForTesting < 0 && Integer.getInteger(FLUSH_GROUPS_PROPERTY) == null
        ? STRIPE_FLUSH_GROUPS
        : flushGroups();
    // One probe table fixes the layout every table of this spill shares; it never holds a group.
    final int stride = factory.apply(workerTableHint()).stride();
    this.stripeStride = stride;
    if (stripeSpill) {
      this.stripeBuffers = new StripeBuffer[partitions];
      final int lanes = NumericGroupAggTable.fullChunkLanes(stride);
      for (int p = passLo; p < passHi; p++) {
        stripeBuffers[p] = new StripeBuffer(stride, lanes);
      }
      // One partition's share of the pass budget, so the buffers together never hold more than the
      // pass is allowed to — floored so a thousand-partition split still compacts batches worth
      // probing rather than a handful of stripes at a time.
      this.compactAt =
          Math.max(1L, Math.min(Math.max(MIN_COMPACT_STRIPES, budget / Math.max(1, passHi - passLo)), budget));
    } else {
      this.stripeBuffers = null;
      this.compactAt = Long.MAX_VALUE;
    }
    if (chunkPoolEnabled()) {
      final int chunkLanes = NumericGroupAggTable.fullChunkLanes(stride);
      final int capacity = poolCapacityChunks(budget, threshold, stride, chunkLanes);
      // The shared pool outlives this pass: the chunks the previous pass (or the previous query)
      // handed back are this pass's tables, and nothing is allocated for them.
      this.pool = LongChunkPool.retainAcrossScans()
          ? LongChunkPool.shared(chunkLanes, capacity)
          : new LongChunkPool(chunkLanes, capacity);
    } else {
      this.pool = null;
    }
  }

  /** A fresh worker table from the factory, restricted to this pass's partitions. */
  public NumericGroupAggTable freshLocal() {
    final NumericGroupAggTable table = adopt(factory.apply(workerTableHint()));
    if (passLo != 0 || passHi != partitions) {
      table.setPassRange(shift, passLo, passHi);
    }
    return table;
  }

  /** The chunk pool every table of this spill draws from, or {@code null} when switched off. */
  public LongChunkPool chunkPool() {
    return pool;
  }

  /** Attach this spill's pool to a table that holds no group yet. */
  private NumericGroupAggTable adopt(final NumericGroupAggTable table) {
    return pool == null
        ? table
        : table.attachChunkPool(pool);
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

  /**
   * Resident groups this spill holds outside the workers' live tables: the distinct groups compacted
   * into the shared partition tables plus the stripes still buffered (an upper bound on their groups,
   * and exactly the memory they occupy). The abort prices this against the pass budget.
   */
  public long groupsSpilled() {
    return spilled.sum() + buffered.sum();
  }

  /** A worker reports {@code leaves} row groups scanned; drives the pass estimate on abort. */
  public void noteLeavesScanned(final int leaves) {
    leavesScanned.add(leaves);
  }

  /**
   * Claim the next morsel of the pass's row groups for the calling worker: the first of up to
   * {@code morsel} consecutive row groups no other worker has taken, or {@code leafCount} once every
   * row group is claimed. Wait-free — one atomic add per morsel — and the morsels are dealt in leaf
   * order, so at any moment the workers read neighbouring leaves. Morsel starts are multiples of
   * {@code morsel}, which with {@link #SUB_CHUNK_LEAVES} equal to the column store's leaf-access
   * window means a morsel is exactly one decoded window per column.
   *
   * @param leafCount row groups in the scan
   * @param morsel row groups per claim, > 0
   * @return the morsel's first row group, or {@code leafCount} when none is left
   */
  public int claimLeaves(final int leafCount, final int morsel) {
    if (leafCount < 0 || morsel <= 0) {
      throw new IllegalArgumentException("leafCount " + leafCount + ", morsel " + morsel);
    }
    final int start = leafCursor.getAndAdd(morsel);
    return start < leafCount
        ? start
        : leafCount;
  }

  /** Row groups claimed so far through {@link #claimLeaves}, bounded by nothing: a witness for tests. */
  public int leavesClaimed() {
    return leafCursor.get();
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
    // groupsSpilled(), not the compacted total: a stripe still sitting in a buffer was SEEN by this
    // pass, and an estimate that forgets it plans the restart from a fraction of the truth.
    final double seen = groupsSpilled() + abandoned.sum();
    return (long) (seen / fraction * partitions / (passHi - passLo));
  }

  /** Whether {@code local} has grown past the threshold and should be flushed. */
  public boolean shouldFlush(final NumericGroupAggTable local) {
    return local.size() >= threshold;
  }

  /**
   * Merge every group of {@code local} into the shared partition tables, then {@link
   * NumericGroupAggTable#release release} it: its chunks return to the pool for the caller's next
   * {@link #freshLocal} (a fresh table is cheaper than a reset of a grown one). The caller must not
   * touch {@code local} afterwards.
   */
  public void flush(final NumericGroupAggTable local) {
    try {
      merge(local);
    } catch (final OutOfMemoryError failure) {
      // The pass SAW this table's groups whether or not the merge landed them: the abort-time
      // estimate that plans the restart must count them, or a worker that dies on its first flush
      // leaves the restart planned blind (nothing spilled, nothing abandoned).
      abandoned.add(local.size());
      throw failure;
    }
  }

  private void merge(final NumericGroupAggTable local) {
    if (simulateOutOfMemoryOnFlush) {
      simulateOutOfMemoryOnFlush = false;
      throw new OutOfMemoryError("simulated: GroupTableSpill flush (test seam)");
    }
    final int[][] index = local.buildPartitionIndex(partitions, shift);
    if (stripeBuffers != null) {
      spillStripes(local, index);
      return;
    }
    final NumericGroupAggTable[] sources = {local};
    final int[][][] indexes = {index};
    // Each flush walks the partitions from its own start, so the workers that flush together (their
    // tables fill at one rate) queue on a monitor by coincidence, not one behind the other.
    final int start = flushOffset
        ? flushStart(flushOrdinal.getAndIncrement(), partitions)
        : 0;
    for (int i = 0; i < partitions; i++) {
      final int p = (start + i) & (partitions - 1);
      if (index[p].length == 0 && !(p == 0 && local.hasZeroKey())) {
        continue;
      }
      synchronized (locks[p]) {
        NumericGroupAggTable target = shared[p];
        if (target == null) {
          target = createShared();
          shared[p] = target;
        }
        final int before = target.size();
        NumericGroupAggTable.mergePartitionIndexed(sources, indexes, p, target);
        spilled.add(target.size() - before);
      }
    }
    local.release();
    FLUSHES.increment();
    if (spilled.sum() > budget) {
      aborted = true;
    }
  }

  /**
   * The stripe spill's flush: copy every live stripe of {@code local} into its partition's buffer
   * under that partition's monitor — a batch of sequential copies per partition instead of a probe
   * per group into a table far larger than any cache — fold the zero group aside, release the table.
   *
   * <p>
   * A buffer past {@link #compactAt} stripes is COMPACTED into the partition's shared table before
   * the flush returns. Without that the spill would hold a group once per worker flush, which for a
   * shape whose groups every worker sees is {@code workers × groups} — the memory the table spill
   * exists to bound. With it the resident state is the shared tables plus at most
   * {@code partitions × compactAt} stripes, and the probes that deduplicate run in cache-sized
   * batches against a partition table small enough to hold.
   */
  private void spillStripes(final NumericGroupAggTable local, final int[][] index) {
    final int start = flushOffset
        ? flushStart(flushOrdinal.getAndIncrement(), partitions)
        : 0;
    for (int i = 0; i < partitions; i++) {
      final int p = (start + i) & (partitions - 1);
      final int[] handles = index[p];
      if (handles.length == 0) {
        continue;
      }
      final StripeBuffer buffer = stripeBuffers[p];
      if (buffer == null) {
        throw new IllegalStateException("partition " + p + " is outside the pass [" + passLo + ", " + passHi + ")");
      }
      synchronized (locks[p]) {
        for (final int handle : handles) {
          buffer.append(local.storageAtAccBase(handle), local.offsetAtAccBase(handle), pool);
        }
        buffered.add(handles.length);
        STRIPES_SPILLED.add(handles.length);
        if (buffer.stripes() >= compactAt) {
          compact(p, buffer);
        }
      }
    }
    if (local.hasZeroKey()) {
      synchronized (locks[0]) {
        if (zeroGroups == null) {
          zeroGroups = adopt(factory.apply(16));
        }
        zeroGroups.mergeZeroGroupFrom(local);
      }
    }
    local.release();
    FLUSHES.increment();
    if (spilled.sum() + buffered.sum() > budget) {
      // Never abort on stripes that have not been deduplicated: a shape whose groups every worker
      // sees buffers the same group once per worker, and a pass that restarted on THAT would split
      // by a multiple of its real state. Compact first, then judge the distinct total.
      compactAll();
      if (spilled.sum() > budget) {
        aborted = true;
      }
    }
  }

  /** Compact every partition's buffer, each under its own monitor. */
  private void compactAll() {
    for (int p = passLo; p < passHi; p++) {
      final StripeBuffer buffer = stripeBuffers[p];
      if (buffer == null) {
        continue;
      }
      synchronized (locks[p]) {
        compact(p, buffer);
      }
    }
  }

  /**
   * Fold {@code buffer}'s stripes into partition {@code part}'s shared table and empty it. Under the
   * partition's monitor; the shared table is created on first need, sized for what will land in it.
   */
  private void compact(final int part, final StripeBuffer buffer) {
    final long stripes = buffer.stripes();
    if (stripes == 0L) {
      return;
    }
    NumericGroupAggTable target = shared[part];
    if (target == null) {
      target = createSharedForStripes(stripes);
      shared[part] = target;
    }
    final int before = target.size();
    for (int c = 0; c < buffer.chunkCount(); c++) {
      target.mergeStripes(buffer.chunk(c), 0, buffer.usedLanes(c));
    }
    buffer.release(pool);
    buffered.add(-stripes);
    spilled.add(target.size() - before);
    COMPACTIONS.increment();
  }

  /**
   * The stripe spill's post-scan finish of one partition: compact whatever the scan left buffered
   * and land the zero group on partition 0's table, so {@link #takeOrCreate} hands over one table
   * holding every group spilled there. Under the partition's monitor.
   */
  private void finishStripes(final int part) {
    final StripeBuffer buffer = stripeBuffers[part];
    if (buffer != null) {
      compact(part, buffer);
    }
    if (part != 0 || zeroGroups == null) {
      return;
    }
    NumericGroupAggTable target = shared[0];
    if (target == null) {
      target = createSharedForStripes(16L);
      shared[0] = target;
    }
    target.mergeZeroGroupFrom(zeroGroups);
    zeroGroups.release();
    zeroGroups = null;
  }

  /**
   * Whether this spill holds ANY state for {@code part} — a shared table, buffered stripes, or (for
   * partition 0) a zero group. A merge split far wider than the group count leaves most partitions
   * empty, and a caller that creates a table and a selector for each of those pays the whole split
   * per query: at a thousand partitions that is the fixed cost a small group-by measures.
   */
  public boolean holdsPartition(final int part) {
    if (part < passLo || part >= passHi) {
      return false;
    }
    synchronized (locks[part]) {
      if (shared[part] != null) {
        return true;
      }
      if (part == 0 && zeroGroups != null) {
        return true;
      }
      final StripeBuffer buffer = stripeBuffers == null
          ? null
          : stripeBuffers[part];
      return buffer != null && buffer.stripes() > 0L;
    }
  }

  /** Whether this spill copies stripes into partition buffers (test observability). */
  public boolean stripeSpill() {
    return stripeBuffers != null;
  }

  /** Lanes per stripe of this spill's layout (test observability). */
  public int stripeStride() {
    return stripeStride;
  }

  /** Stripes buffered for {@code part} so far — post-scan, or under test (test observability). */
  public long stripesBuffered(final int part) {
    final StripeBuffer buffer = stripeBuffers == null
        ? null
        : stripeBuffers[part];
    return buffer == null
        ? 0L
        : buffer.stripes();
  }

  /** A shared partition table: at the plan's expected count when known, else at the worker hint. */
  private NumericGroupAggTable createShared() {
    if (sharedHint < 0) {
      return adopt(factory.apply(workerTableHint()));
    }
    PRESIZED_SHARED.increment();
    return adopt(factory.apply(sharedHint));
  }

  /**
   * A shared partition table sized for a compaction of {@code stripes}: never more than the stripes
   * that will land in it (they bound its groups from above) and never more than one partition's
   * share of the pass budget — a blind plan's worker hint would otherwise size EVERY partition of a
   * thousand-partition split for a whole worker table.
   */
  private NumericGroupAggTable createSharedForStripes(final long stripes) {
    final long perPartition = Math.max(16L, budget / Math.max(1, passHi - passLo));
    final long bounded = Math.min(stripes, Math.min(perPartition, sharedHint < 0
        ? workerTableHint()
        : sharedHint));
    if (sharedHint >= 0) {
      PRESIZED_SHARED.increment();
    }
    return adopt(factory.apply((int) Math.max(16L, Math.min(Integer.MAX_VALUE, bounded))));
  }

  /**
   * Rehashes the shared partition tables took under their locks, summed as the tables leave this
   * spill ({@link #takeOrCreate}, {@link #releaseTables}): zero when the pre-size held, the count a
   * blind pass paid otherwise. Diag and test observability.
   */
  public long sharedRehashes() {
    return sharedRehashes.sum();
  }

  /** The hint shared tables are created at, or {@code -1} for the worker hint (test observability). */
  public int sharedHint() {
    return sharedHint;
  }

  /** Whether this spill's flushes start at rotating partitions (test observability). */
  public boolean flushOffset() {
    return flushOffset;
  }

  /**
   * Drop the shared partition tables of an ABORTED pass. The estimate and the pass recommendation need
   * only the counters ({@link #groupsSpilled}, {@link #groupsAbandoned}, leaves scanned), but the
   * tables stay reachable through this spill until the arm has re-planned — and a restart that refreshes
   * the budget by a forced collection measures whatever is still REFERENCED, not what the arm intends
   * to keep: at 100M (q32) the aborted pass's 16.6M spilled groups read as 3.9 GB of live heap, the
   * budget FELL 11.5M → 7.9M and the restart ran 16 passes instead of 8. Call after the parallel
   * section has joined and before re-planning; the spill is not reused. The pool is drained for the
   * same reason: what it holds is retained by intent only, and the measurement must not count it.
   */
  public void releaseTables() {
    for (int p = 0; p < partitions; p++) {
      synchronized (locks[p]) {
        final NumericGroupAggTable table = shared[p];
        if (table != null) {
          sharedRehashes.add(table.rehashes());
          shared[p] = null;
          // Its storage is the restart's tables: hand it back instead of dropping it to the collector.
          table.release();
        }
        if (stripeBuffers != null && stripeBuffers[p] != null) {
          // The counters the estimate reads survive a release (they are what the pass SAW), so the
          // buffered stripes move to the spilled total rather than vanishing with their chunks.
          final long held = stripeBuffers[p].stripes();
          if (held > 0L) {
            buffered.add(-held);
            spilled.add(held);
          }
          stripeBuffers[p].release(pool);
        }
        if (p == 0 && zeroGroups != null) {
          zeroGroups.release();
          zeroGroups = null;
        }
      }
    }
    if (pool != null && !pool.isShared()) {
      // A per-scan pool is invisible to the budget; a shared one is added back to the headroom.
      pool.drain();
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
      if (stripeBuffers != null) {
        finishStripes(part);
      }
      table = shared[part];
      shared[part] = null;
    }
    if (table == null) {
      return adopt(requireNonNull(fresh, "fresh").get());
    }
    sharedRehashes.add(table.rehashes());
    return table;
  }
}
