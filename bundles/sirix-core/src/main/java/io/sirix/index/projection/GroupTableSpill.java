package io.sirix.index.projection;

import static io.sirix.cache.ShardedPageCache.retainCleanupFailure;
import static io.sirix.cache.ShardedPageCache.rethrowCleanupFailure;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Bounds the per-worker group tables of a parallel group-by by spilling them into SHARED partition
 * tables.
 *
 * <p>
 * A parallel group-by hands every worker its own {@link NumericGroupAggTable}, so a group that all
 * workers see is held once per worker: memory is {@code workers × groups}, which for a
 * million-group string key at 100M rows is several GB before the post-scan merge even starts — the
 * heap exhaustion that sent q13 ({@code SearchPhrase, COUNT(DISTINCT UserID)}) to the interpreter.
 * Here a worker checks its table every {@link #SUB_CHUNK_LEAVES} row groups and, past
 * {@link #flushGroups()} groups, merges it into the partition tables under each partition's monitor
 * and starts a fresh one: resident state is {@code groups + workers × flushGroups}. The post-scan
 * merge takes a partition's shared table as its base ({@link #takeOrCreate}) so nothing is copied
 * twice, and the merge itself is the same {@link NumericGroupAggTable#mergePartitionIndexed} the
 * post-scan already runs — first-seen ordinals, exact sums, identity lanes and the zero-key group
 * all merge the way they always did.
 * </p>
 *
 * <p>
 * Every table the spill hands out or creates, and every stripe buffer, takes its chunks from one
 * {@link GroupWorkspace}, the working memory of the grouped scan the pass belongs to
 * ({@link #inWorkspace}): a flushed worker table {@link NumericGroupAggTable#release releases} its
 * chunks to it, a growing partition table hands back the ones it outgrew, and the caller releases
 * the merged tables once a pass has emitted its candidates. A pass therefore builds its tables from
 * the chunks the previous one handed back, and the scan returns all of it to the allocator at once
 * when it closes its workspace. The chunks used to be heap arrays in free lists that outlived the
 * scan.
 * </p>
 *
 * <p>
 * The workers' tables fill at the same rate, so their flushes arrive together — and a flush that
 * walked the partitions in one fixed order made every worker queue on the same monitor as the one
 * ahead of it: a lock convoy that parked a fifth of the workers' wall at 100M (q32's wall profile),
 * with each shared table's rehashes (six of them, 600 MB copied per partition, from a worker-sized
 * hint) taken under that lock while the queue waited. Two answers, one kill switch each: every
 * flush starts its walk at a different partition ({@link #flushStart}, {@code
 * -Dsirix.projection.groupTable.flushOffset=false}), and a shared table is created at the count the
 * caller's plan expects it to hold ({@link #sharedTableHint}, {@code
 * -Dsirix.projection.groupTable.presizeShared=false}) — witnessed per spill by
 * {@link #sharedRehashes}, which a taken pre-size leaves at zero.
 * </p>
 *
 * <p>
 * The pass's row groups are dealt to the workers from one shared cursor ({@link #claimLeaves}), a
 * morsel of {@link #SUB_CHUNK_LEAVES} at a time, not as a fixed slice of the leaf range per worker.
 * The cost of a row group is a property of the DATA: a leaf range dense in the grouped strings runs
 * about twice as slow as its neighbours (at 100M, worker 8's slice finished every pass of q16 and
 * q18 45–57 % after the median worker, with the same slice slowest in every pass and every try),
 * and a static partition ends its scan with the slowest slice. A shared cursor ends it within one
 * morsel of the mean, and the workers sweep the file together instead of from twenty starting
 * points.
 * </p>
 */
public final class GroupTableSpill {

  /** Use compact hash indexes and dense accumulator records (default on above the crossover). */
  public static final String DENSE_INDEX_PROPERTY = "sirix.projection.groupTable.denseIndex";

  /**
   * The narrowest stripe the dense layout is worth, from the lanes each layout holds per LIVE group
   * at the 3/4 growth threshold. The interleaved layout reserves a whole stripe in every bucket, so
   * it holds {@code stride / (3/4) = (4/3) * stride} lanes per live group. The dense layout packs the
   * stripes with no holes and adds one index lane per bucket, so it holds {@code stride + 4/3}. Those
   * are equal at {@code stride == 4} and the dense form is larger below it: at the three lanes of
   * {@code GROUP BY x ORDER BY count(*)} it is 4.33 lanes against 4.0, and it charges a second
   * dependent load per probe to prove the key behind the index hit. So dense saves lanes only for
   * {@code stride > 4}, and the gate admits exactly those shapes.
   */
  public static final int DENSE_INDEX_MIN_STRIDE = 5;

  private static boolean denseIndexEnabled(final int stride) {
    return stride >= DENSE_INDEX_MIN_STRIDE && Boolean.parseBoolean(System.getProperty(DENSE_INDEX_PROPERTY, "true"));
  }

  /**
   * Let dense worker tables of a spill that asked for it keep partial records instead of
   * deduplicating every group locally (default on; requires the dense index).
   */
  public static final String PARTIAL_GROUPS_PROPERTY = "sirix.projection.groupTable.partialGroups";

  private static boolean partialGroupsEnabled() {
    return Boolean.parseBoolean(System.getProperty(PARTIAL_GROUPS_PROPERTY, "true"));
  }

  /** Test seam: receives one increment per partial worker table handed out; {@code null} = off. */
  private static volatile @Nullable LongAdder partialWorkersForTesting;

  /**
   * Test seam for the partial-grouping gate: while {@code counter} is installed, every worker table a
   * spill hands out with partial grouping enabled increments it once. Production installs nothing and
   * pays one volatile read per worker table.
   *
   * @param counter the counter to install, or {@code null} to stop observing
   * @return the previous counter, for restoring in a finally block
   */
  public static @Nullable LongAdder setPartialWorkersForTesting(final @Nullable LongAdder counter) {
    final @Nullable LongAdder previous = partialWorkersForTesting;
    partialWorkersForTesting = counter;
    return previous;
  }

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

  /** Start every flush's partition walk at a different partition (default on). */
  public static final String FLUSH_OFFSET_PROPERTY = "sirix.projection.groupTable.flushOffset";
  /** Create a shared partition table at the count the plan expects it to hold (default on). */
  public static final String PRESIZE_SHARED_PROPERTY = "sirix.projection.groupTable.presizeShared";
  /** The sizing hint every worker table gets: it flushes long before it would outgrow this twice. */
  public static final int WORKER_TABLE_HINT = 1 << 16;

  /**
   * Configured worker-table sizing hint, in groups. The DEFAULT is {@link #WORKER_TABLE_HINT}, which
   * predates the stripe spill: back then a flush meant probing every group into a shared table far
   * larger than any cache, so flushing rarely was worth a big local table. Now a flush is a
   * sequential stripe copy with a compaction behind it, and the trade has inverted — a table small
   * enough to PROBE out of cache is worth flushing more often for.
   *
   * <p>
   * The probe is where the suite's CPU is: {@code NumericGroupAggTable.acquireExact} alone is 17 % of
   * a 43-query leg, and at 65,536 groups a stripe-15 table is ~7.9 MB, well past L2.
   * </p>
   *
   * <p>
   * <b>MEASURED AND REFUTED (2026-09-03), which is why the default is unchanged.</b> At 100M, hot
   * seconds for q32+q18+q13: <b>65,536 → 12.557 s</b>, 16,384 → 14.238, 8,192 → 14.275, 4,096 →
   * 14.453. Monotonically worse. Cache residency is real but it is not what dominates: a smaller
   * table flushes more often, and every flush drags a COMPACTION that probes the partition table
   * anyway — so shrinking the local table moves the misses rather than removing them, and adds the
   * stripe copies on top. The knob stays because it is how that was learned, not because a smaller
   * value is wanted.
   * </p>
   */
  public static final String WORKER_HINT_PROPERTY = "sirix.projection.groupTable.workerHint";

  /** The worker sizing hint in effect. */
  public static int workerTableHint() {
    final int configured = Integer.getInteger(WORKER_HINT_PROPERTY, WORKER_TABLE_HINT);
    return Math.max(16, configured);
  }

  /**
   * Skew allowance on a shared table's expected share, in percent (a partition's count is ± its
   * root).
   */
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

  /**
   * Whether shared tables are created at the plan's expected count: the override when set, else the
   * property.
   */
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
  /**
   * Flush threshold in groups per worker table under the stripe spill when the property sets none.
   */
  public static final int STRIPE_FLUSH_GROUPS = WORKER_TABLE_HINT;
  /**
   * Stripes a partition buffers before a compaction, whatever the budget says (a batch worth
   * probing).
   */
  private static final long MIN_COMPACT_STRIPES = 4_096L;
  private static volatile int stripeSpillForTesting = -1;
  private static final LongAdder STRIPES_SPILLED = new LongAdder();
  private static final LongAdder COMPACTIONS = new LongAdder();

  /**
   * Whether spills copy stripes into partition buffers: the test override when set, else the
   * property.
   */
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
   * chunks GROW from a handful of stripes to the tables' full chunk length, so a shape with thousands
   * of partitions and few groups costs stripes rather than chunks while a partition that fills reuses
   * the chunks the scan's {@link GroupWorkspace} took back, like the tables. Written under the
   * partition's monitor, read by the one worker that builds the partition after the scan has joined —
   * so it carries no synchronisation of its own.
   *
   * <p>
   * Why a buffer and not the shared table it replaces: a flush that probes {@code flushGroups} groups
   * into a shared table of the pass's share pays a cache miss per group on a table far larger than
   * any cache (at 100M, q32's 62 MB partition tables: ≈ 400 ns per group, 24 % of the query's CPU,
   * plus the local table's own misses and its rehashes — and the shared table's chunks then rehash
   * under the partition's monitor). A stripe copy is one sequential {@code stride}-lane write; the
   * partition's table is built ONCE from the buffer, hinted at its exact stripe count so it never
   * rehashes, and a high-cardinality shape (a group per row) pays one probe per group instead of two
   * plus a merge.
   */
  static final class StripeBuffer {
    /** Stripes the FIRST chunk of a buffer holds; a partition that stays small costs only this. */
    private static final int FIRST_CHUNK_STRIPES = 8;
    private final int stride;
    /** The tables' full chunk length: the size every chunk past the growth ramp is taken at. */
    private final int fullLanes;
    private GroupBuffer[] chunks = new GroupBuffer[4];
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
     * {@value #FIRST_CHUNK_STRIPES} stripes up to the full chunk — so a partition holding a handful of
     * groups costs a handful of stripes, and one holding millions still reaches the full size within a
     * few chunks. Always a whole number of stripes.
     */
    private int nextChunkLanes() {
      if (chunkCount == 0) {
        return Math.min(fullLanes, stride * FIRST_CHUNK_STRIPES);
      }
      final int doubled = GroupLanes.count(chunks[chunkCount - 1]) << 1;
      return doubled <= 0 || doubled >= fullLanes
          ? fullLanes
          : doubled - doubled % stride;
    }

    /**
     * Copy the stripe whose accumulator base is {@code accBase} of {@code src} (key lane at base − 1).
     */
    void append(final GroupBuffer src, final int accBase, final GroupWorkspace workspace) {
      final int st = stride;
      if (chunkCount == 0 || used[chunkCount - 1] + st > GroupLanes.count(chunks[chunkCount - 1])) {
        if (chunkCount == chunks.length) {
          final GroupBuffer[] grown = new GroupBuffer[chunkCount << 1];
          System.arraycopy(chunks, 0, grown, 0, chunkCount);
          chunks = grown;
          final int[] grownUsed = new int[chunkCount << 1];
          System.arraycopy(used, 0, grownUsed, 0, chunkCount);
          used = grownUsed;
        }
        chunks[chunkCount] = workspace.takeChunk(nextChunkLanes());
        used[chunkCount] = 0;
        chunkCount++;
      }
      final int tail = chunkCount - 1;
      GroupLanes.copy(src, accBase - 1, chunks[tail], used[tail], st);
      used[tail] += st;
      stripes++;
    }

    long stripes() {
      return stripes;
    }

    int chunkCount() {
      return chunkCount;
    }

    GroupBuffer chunk(final int index) {
      return chunks[index];
    }

    /** Lanes of whole stripes in chunk {@code index}. */
    int usedLanes(final int index) {
      return used[index];
    }

    /** Hand every chunk back to the scan's workspace and empty the buffer. */
    // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
    // failure is rethrown with later ones attached.
    @SuppressWarnings("java:S1181")
    void release(final GroupWorkspace workspace) {
      Throwable failure = null;
      for (int i = 0; i < chunkCount; i++) {
        final GroupBuffer chunk = chunks[i];
        chunks[i] = null;
        used[i] = 0;
        try {
          workspace.giveChunk(chunk);
        } catch (final Throwable cleanup) {
          failure = retainCleanupFailure(failure, cleanup);
        }
      }
      chunkCount = 0;
      stripes = 0L;
      rethrowCleanupFailure(failure);
    }
  }

  /**
   * The partition the {@code ordinal}-th flush of a spill starts its walk at: the flushes step
   * through the partitions by an odd stride near {@code 0.618 × partitions}, so any {@code
   * partitions} consecutive flushes start at {@code partitions} DISTINCT partitions (an odd stride is
   * coprime to a power of two) and consecutive ones start far apart (the golden-ratio stride is the
   * one whose multiples spread most evenly). Workers that flush together therefore meet on a monitor
   * only by coincidence, not by construction. A spill built with the rotation switched off starts
   * every flush at partition 0 instead ({@link #flushOffset}).
   */
  static int flushStart(final long ordinal, final int partitions) {
    return (int) (ordinal * flushStride(partitions)) & (partitions - 1);
  }

  /** The odd stride of {@link #flushStart} for {@code partitions} (a power of two). */
  static int flushStride(final int partitions) {
    return (int) ((long) partitions * 0x9E37L >>> 16) | 1;
  }

  /**
   * The sizing hint of a shared partition table when the caller's plan knows the shape's group count,
   * or {@code -1} when it does not (or the pre-size is switched off): the uniform share of
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
   * 2^23 — every shared table of every pass doubled, 2.1 GB of fresh chunks per pass, and the hot
   * tries ran SLOWER per pass than the cold one (q32: 1.5 → 1.8 s). A partition's count deviates from
   * its share by about its root, so the tighter hint keeps {@value #SHARED_HINT_TIGHT_ROOTS} roots of
   * headroom and lets an estimate that is truly off pay one rehash instead.
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

  /** Configured ceiling on resident groups per pass, capped by the exclusive native grant. */
  public static final String GROUP_BUDGET_PROPERTY = "sirix.projection.groupTable.groupBudget";
  /** Charge used by the legacy stride-free planning overloads. */
  private static final long BYTES_PER_GROUP = 128L;

  /** Legacy property name; disabling layout accounting cannot bypass the native byte grant. */
  public static final String STRIDE_BUDGET_PROPERTY = "sirix.projection.groupTable.strideBudget";

  /** Dense source/destination stripe overlap while a partition is compacted. */
  private static final long CAPACITY_SLACK = 2L;

  /** Maximum worker footprint through its next flush check, including a rehash in that batch. */
  static long workerPeakBytes(final int strideLanes, final int hint, final int threshold, final int batchLeaves) {
    final long batchRows = Math.multiplyExact((long) batchLeaves, ProjectionIndexRowGroupPage.MAX_ROWS);
    final long groups = Math.addExact(threshold - 1L, batchRows);
    if (groups > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("worker flush batch exceeds supported group capacity");
    }
    final long capacity =
        Math.max(NumericGroupAggTable.capacityFor(hint), NumericGroupAggTable.capacityFor((int) groups));
    final long growingCapacity = capacity + capacity / 2;
    return denseIndexEnabled(strideLanes)
        ? Math.addExact(Math.multiplyExact(groups, (long) strideLanes * Long.BYTES), growingCapacity * Long.BYTES)
        : Math.multiplyExact(growingCapacity, (long) strideLanes * Long.BYTES);
  }

  static int effectiveFlushThreshold() {
    return stripeSpillEnabled() && flushGroupsForTesting < 0 && Integer.getInteger(FLUSH_GROUPS_PROPERTY) == null
        ? STRIPE_FLUSH_GROUPS
        : flushGroups();
  }

  /**
   * Peak resident charge for a group: its complete stripe plus three index lanes, doubled for
   * rehash/merge overlap. Wide records are never capped at a fictitious 128 bytes. Fixed worker
   * tables and chunk/region rounding are subtracted from the grant separately.
   */
  public static long bytesPerGroup(final int strideLanes) {
    if (strideLanes <= 0) {
      throw new IllegalArgumentException("strideLanes must be positive: " + strideLanes);
    }
    final long densePeak = ((long) strideLanes + 3L) * CAPACITY_SLACK;
    // At a 3/4-load rehash the old and doubled interleaved tables hold four stripes per
    // live group; a still-live source stripe buffer adds one more during compaction.
    final long interleavedPeak = (long) strideLanes * (stripeSpillEnabled()
        ? 5L
        : 4L);
    return (denseIndexEnabled(strideLanes)
        ? densePeak
        : Math.max(densePeak, interleavedPeak)) * Long.BYTES;
  }

  private static volatile long groupBudgetForTesting = -1L;

  /** Resident-group ceiling from granted execution capacity; no minimum can create capacity. */
  public static long groupBudget() {
    return groupBudget(WIDEST_CHARGED_STRIDE);
  }

  /**
   * The resident-group ceiling per pass for a shape whose stripe is {@code strideLanes} lanes wide —
   * {@link #groupBudget()} against {@link #bytesPerGroup(int)} rather than the flat figure. A pass
   * planned this way holds the groups its share can actually carry, instead of the groups a 128-byte
   * stripe could carry.
   *
   * @param strideLanes lanes per group stripe ({@link NumericGroupAggTable#strideFor})
   * @return resident groups admitted per pass
   */
  public static long groupBudget(final int strideLanes) {
    return groupBudget(strideLanes, null);
  }

  /**
   * {@link #groupBudget(int)} for a scan that already holds {@code workspace}: its capacity grant is
   * already exclusive. The fixed planning overhead has been subtracted once by the workspace; only
   * full per-group peak charges are applied here.
   *
   * @param strideLanes lanes per group stripe ({@link NumericGroupAggTable#strideFor})
   * @param workspace the planning scan's workspace, or {@code null} before it has one
   * @return resident groups admitted per pass
   */
  public static long groupBudget(final int strideLanes, final @Nullable GroupWorkspace workspace) {
    final long capacity = workspace == null
        ? AggregationShare.plannedBytes(AggregationShare.budgetBytes(), AggregationShare.headroomBytes(null))
        : workspace.usableGroupBytes();
    final long planned = Math.min(1L << 26, capacity / bytesPerGroup(strideLanes));
    final long testing = groupBudgetForTesting;
    if (testing >= 0L) {
      return workspace == null
          ? testing
          : Math.min(testing, planned);
    }
    final long configured = Long.getLong(GROUP_BUDGET_PROPERTY, -1L);
    return configured > 0L
        ? Math.min(configured, planned)
        : planned;
  }

  /**
   * The derived budget for a memory budget of {@code maxMemory} and {@code headroom} bytes (pure, for
   * tests) — {@link AggregationShare#plannedBytes(long, long)} at {@value #BYTES_PER_GROUP} bytes per
   * group. The share is NOT recomputed here: a second copy of the arithmetic is how two planners
   * would drift apart.
   */
  public static long groupBudgetFor(final long maxMemory, final long headroom) {
    return groupBudgetFor(maxMemory, headroom, WIDEST_CHARGED_STRIDE);
  }

  /**
   * The derived budget for {@code maxMemory}, {@code headroom} and a stripe of {@code strideLanes}
   * lanes (pure, for tests) — {@link AggregationShare#plannedBytes(long, long)} at
   * {@link #bytesPerGroup(int)} per group, capped exactly as the flat form is.
   *
   * @param maxMemory the budget of the memory the tables live in: the allocator's
   * @param headroom what of it is still available to the scan
   * @param strideLanes lanes per group stripe ({@link NumericGroupAggTable#strideFor})
   */
  public static long groupBudgetFor(final long maxMemory, final long headroom, final int strideLanes) {
    final long planned = AggregationShare.plannedBytes(maxMemory, headroom) / bytesPerGroup(strideLanes);
    return Math.max(0L, Math.min(1L << 26, planned));
  }

  /**
   * Default shape for the legacy stride-free overloads; five lanes cost 128 bytes with a dense index.
   */
  public static final int WIDEST_CHARGED_STRIDE = 5;

  /**
   * The budget an UNCOMMITTED allocator of this size yields — {@link #groupBudgetFor} at a headroom
   * of the whole budget — or the fixed figure when the property or the test override sets one. A plan
   * compares the budget it read against this to decide whether a re-read could widen it: only a
   * budget below the ceiling can.
   */
  public static long groupBudgetCeiling() {
    return groupBudgetCeiling(WIDEST_CHARGED_STRIDE);
  }

  /**
   * The uncommitted-allocator ceiling for a stripe of {@code strideLanes} lanes. A refresh compares
   * the budget it planned against this, so the two must be charged the SAME bytes per group: a narrow
   * shape whose budget was widened by its stride would otherwise be measured against a ceiling
   * computed for a 128-byte stripe and look as though a re-read could not help it.
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
    final long maxMemory = AggregationShare.budgetBytes();
    return groupBudgetFor(maxMemory, maxMemory, strideLanes);
  }

  /** Restore the shared-consumer budget for bounded aggregates when disabled. */
  public static final String BOUNDED_BUDGET_PROPERTY = "sirix.projection.groupTable.boundedBudget";

  private static boolean boundedBudgetEnabled(final int strideLanes) {
    return denseIndexEnabled(strideLanes) && stripeSpillEnabled()
        && Boolean.parseBoolean(System.getProperty(BOUNDED_BUDGET_PROPERTY, "true"));
  }

  /** Bounded output does not reduce the peak live footprint of a group while constructing it. */
  public static long boundedBytesPerGroup(final int strideLanes) {
    return bytesPerGroup(strideLanes);
  }

  /**
   * Budget for a bounded aggregate whose completed partitions can be selected and released
   * independently. Callers must exclude grouped distinct state, key ordering and an unbounded
   * selection (a limit of none or of every group, under which each partition's winners are its whole
   * table and nothing is released). This allowance is the aggregation's own, in the allocator; the
   * column-fill and distinct-set consumers plan against the heap. The fixed operator budget and test
   * override retain precedence.
   */
  public static long boundedGroupBudget(final int strideLanes) {
    return boundedGroupBudget(strideLanes, null);
  }

  /**
   * {@link #boundedGroupBudget(int)} for a scan that already holds {@code workspace}; see
   * {@link #groupBudget(int, GroupWorkspace)}.
   */
  public static long boundedGroupBudget(final int strideLanes, final @Nullable GroupWorkspace workspace) {
    return groupBudget(strideLanes, workspace);
  }

  /**
   * Uncommitted-allocator counterpart of {@link #boundedGroupBudget}, for the same refresh decision.
   */
  public static long boundedGroupBudgetCeiling(final int strideLanes) {
    if (!boundedBudgetEnabled(strideLanes)) {
      return groupBudgetCeiling(strideLanes);
    }
    final long testing = groupBudgetForTesting;
    if (testing >= 0L) {
      return testing;
    }
    final long configured = Long.getLong(GROUP_BUDGET_PROPERTY, -1L);
    if (configured > 0L) {
      return configured;
    }
    final long maxMemory = AggregationShare.budgetBytes();
    return boundedGroupBudgetFor(maxMemory, maxMemory, strideLanes);
  }

  /**
   * Pure bounded-aggregation budget: {@link AggregationShare#plannedBoundedBytes(long, long)} (at
   * most the exclusive execution capacity), charged for the actual layout and growth peaks.
   * Production acquires a grant before invoking the planner; this overload is the pure arithmetic.
   *
   * @param maxMemory non-negative budget of the memory the tables live in: the allocator's
   * @param headroom non-negative available bytes, including reusable table chunks
   * @param strideLanes positive lanes per record
   * @return group allowance; no minimum may manufacture memory
   */
  public static long boundedGroupBudgetFor(final long maxMemory, final long headroom, final int strideLanes) {
    if (maxMemory < 0L || headroom < 0L || strideLanes <= 0) {
      throw new IllegalArgumentException(
          "invalid budget, headroom or stride: " + maxMemory + ", " + headroom + ", " + strideLanes);
    }
    if (!boundedBudgetEnabled(strideLanes)) {
      return groupBudgetFor(maxMemory, headroom, strideLanes);
    }
    final long planned = AggregationShare.plannedBoundedBytes(maxMemory, headroom) / boundedBytesPerGroup(strideLanes);
    return Math.max(0L, Math.min(1L << 26, planned));
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
   * The fewest balanced passes whose LARGEST share holds {@code groups} uniformly hashed groups
   * within {@code budget}: one pass while the count fits the budget, else the smallest count whose
   * largest share's expected groups fit, at most one pass per partition (where the caller's abort
   * machinery decides what a pass cannot hold).
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

  /**
   * The groups the largest of {@code passes} balanced passes over {@code groups} is expected to hold.
   */
  public static long expectedLargestPass(final long groups, final int passes, final int partitions) {
    final long share = largestPassShare(partitions, passes);
    return (groups * share + partitions - 1) / partitions;
  }

  /**
   * The smallest group count for which {@link #passesFor} answers at least {@code passes} at
   * {@code budget}: the count that makes {@code passes - 1} passes insufficient. A completed pass set
   * whose SEED aborted records this instead of its exact count, so the next seeding lands on the pass
   * count that completed instead of the one that aborted again.
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
  /**
   * {@link #sharedTableHint}: the hint a shared table is created at, or {@code -1} for the worker
   * hint.
   */
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

  private final boolean partialGroups;
  /**
   * The grouped scan's working memory: every table this spill hands out or creates and every stripe
   * buffer takes its chunks here. The scan attaches its own ({@link #inWorkspace}); a spill nobody
   * attached one to opens a private one on first need.
   */
  private @Nullable GroupWorkspace workspace;
  private boolean privateWorkspace;
  private final boolean denseIndex;
  private final LongAdder spilled = new LongAdder();
  /**
   * Stripes sitting in the partition buffers: resident state the abort must price, not yet
   * deduplicated.
   */
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
   *        asked for at {@link #WORKER_TABLE_HINT}, shared partition tables at
   *        {@link #sharedTableHint} when {@code expectedGroups} is known
   * @param expectedGroups the shape's total group count over ALL partitions as the plan expects it (a
   *        memoised or abort-estimated count), or {@code 0} when the plan is blind — shared tables
   *        then start at the worker hint and grow
   */
  public GroupTableSpill(final int partitions, final int shift, final IntFunction<NumericGroupAggTable> factory,
      final long expectedGroups, final int passLo, final int passHi, final long budget) {
    this(partitions, shift, factory, expectedGroups, passLo, passHi, budget, false);
  }

  /**
   * A spill that may relax local deduplication for dense, high-cardinality worker tables. Every
   * partial record still reaches an exact partition table before any group is selected. Callers must
   * exclude distinct sinks and any other state requiring unique local group handles. Kill switch
   * {@code -Dsirix.projection.groupTable.partialGroups=false} keeps the dense layout and restores
   * exact worker tables, so the two can be compared in place.
   */
  public GroupTableSpill(final int partitions, final int shift, final IntFunction<NumericGroupAggTable> factory,
      final long expectedGroups, final int passLo, final int passHi, final long budget, final boolean partialGroups) {
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
    this.threshold = effectiveFlushThreshold();
    // One probe table fixes the layout every table of this spill shares; it never holds a group.
    final int stride = factory.apply(workerTableHint()).stride();
    this.denseIndex = denseIndexEnabled(stride);
    this.partialGroups = partialGroups && denseIndex && partialGroupsEnabled();
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
  }

  /**
   * Take every chunk of this spill's tables and buffers from {@code workspace}, the working memory of
   * the grouped scan this pass belongs to. A scan runs several passes and each pass has a spill of
   * its own; sharing the scan's workspace is what lets a pass build its tables from the chunks the
   * previous pass handed back. Only before the spill has handed out a table.
   *
   * @return this spill
   * @throws IllegalStateException if the spill already has a workspace
   */
  public GroupTableSpill inWorkspace(final GroupWorkspace workspace) {
    requireNonNull(workspace, "workspace");
    if (this.workspace != null && this.workspace != workspace) {
      throw new IllegalStateException("the spill already takes its memory from another workspace");
    }
    this.workspace = workspace;
    return this;
  }

  /**
   * The workspace this spill's tables and buffers take their memory from. A spill without one opens a
   * PRIVATE workspace here (tests, tools), a {@linkplain GroupWorkspace#selfReturning()
   * self-returning} one: {@link #close()} returns it, and so does dropping the spill together with
   * every table it handed out. A query attaches its scan's workspace instead.
   */
  public synchronized GroupWorkspace workspace() {
    GroupWorkspace own = workspace;
    if (own == null) {
      own = GroupWorkspace.selfReturning();
      workspace = own;
      privateWorkspace = true;
    }
    return own;
  }

  /**
   * Releases this spill's tables and returns a private workspace to the allocator. Nothing built from
   * this spill may be read afterwards.
   */
  // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
  // failure is rethrown with later ones attached.
  @SuppressWarnings("java:S1181")
  public void close() {
    Throwable failure = null;
    try {
      releaseTables();
    } catch (final Throwable cleanup) {
      failure = cleanup;
    }
    final GroupWorkspace own;
    synchronized (this) {
      own = privateWorkspace
          ? workspace
          : null;
    }
    if (own != null) {
      try {
        own.close();
      } catch (final Throwable cleanup) {
        failure = retainCleanupFailure(failure, cleanup);
      }
    }
    rethrowCleanupFailure(failure);
  }

  private int localTableHint() {
    final GroupWorkspace own = workspace;
    return own == null
        ? workerTableHint()
        : own.workerTableHint();
  }

  public int batchLeaves() {
    final GroupWorkspace own = workspace;
    return own == null
        ? subChunkLeaves()
        : own.batchLeaves();
  }

  /** A fresh worker table from the factory, restricted to this pass's partitions. */
  public NumericGroupAggTable freshLocal() {
    final NumericGroupAggTable table = adopt(factory.apply(localTableHint()));
    if (partialGroups) {
      table.allowPartialGroups();
      final @Nullable LongAdder observer = partialWorkersForTesting;
      if (observer != null) {
        observer.increment();
      }
    }
    if (passLo != 0 || passHi != partitions) {
      table.setPassRange(shift, passLo, passHi);
    }
    return table;
  }

  /** Put a table that holds no group yet into this spill's workspace and layout. */
  private NumericGroupAggTable adopt(final NumericGroupAggTable table) {
    if (denseIndex) {
      table.useDenseIndex();
    }
    return table.attachWorkspace(workspace());
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

  /**
   * Row groups claimed so far through {@link #claimLeaves}, bounded by nothing: a witness for tests.
   */
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
   * aborted count here — the caller knows whether the budget it restarts with is the one that aborted
   * (then at least one more pass) or a refreshed, wider one (then the fit as it is).
   */
  public int recommendedPasses(final int totalLeaves, final long budget) {
    return Math.min(partitions, passesFor(estimatedTotalGroups(totalLeaves), budget, partitions));
  }

  /**
   * The abort-time total-group estimate: the groups this pass saw (spilled plus abandoned in worker
   * tables), extrapolated over the unscanned leaves and over the partitions this pass did not own —
   * exposed so the caller can memo it on the handle and seed the NEXT execution's pass count
   * directly, skipping the aborted scan this execution already paid for.
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
    final GroupWorkspace own = workspace;
    return local.size() >= (own == null
        ? threshold
        : own.flushThreshold(threshold));
  }

  /**
   * Merge every group of {@code local} into the shared partition tables, then
   * {@link NumericGroupAggTable#release release} it: its chunks return to the workspace for the
   * caller's next {@link #freshLocal} (a fresh table is cheaper than a reset of a grown one). The
   * caller must not touch {@code local} afterwards.
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
    } finally {
      // A flush consumes the worker table even when a merge or allocation fails part way through.
      local.retire();
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
    local.retire();
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
    final GroupWorkspace chunks = workspace();
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
          buffer.append(local.storageAtAccBase(handle), local.offsetAtAccBase(handle), chunks);
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
    local.retire();
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
    buffer.release(workspace());
    buffered.add(-stripes);
    spilled.add(target.size() - before);
    COMPACTIONS.increment();
  }

  /**
   * The stripe spill's post-scan finish of one partition: compact whatever the scan left buffered and
   * land the zero group on partition 0's table, so {@link #takeOrCreate} hands over one table holding
   * every group spilled there. Under the partition's monitor.
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
    zeroGroups.retire();
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
      return adopt(factory.apply(localTableHint()));
    }
    PRESIZED_SHARED.increment();
    return adopt(factory.apply(sharedHint));
  }

  /**
   * A shared partition table sized for a compaction of {@code stripes}: never more than the stripes
   * that will land in it (they bound its groups from above) and never more than one partition's share
   * of the pass budget — a blind plan's worker hint would otherwise size EVERY partition of a
   * thousand-partition split for a whole worker table.
   */
  private NumericGroupAggTable createSharedForStripes(final long stripes) {
    final long perPartition = Math.max(16L, budget / Math.max(1, passHi - passLo));
    final long bounded = Math.min(stripes, Math.min(perPartition, sharedHint < 0
        ? localTableHint()
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

  /**
   * The hint shared tables are created at, or {@code -1} for the worker hint (test observability).
   */
  public int sharedHint() {
    return sharedHint;
  }

  /** Whether this spill's flushes start at rotating partitions (test observability). */
  public boolean flushOffset() {
    return flushOffset;
  }

  /**
   * Drop the shared partition tables of an ABORTED pass. The estimate and the pass recommendation
   * need only the counters ({@link #groupsSpilled}, {@link #groupsAbandoned}, leaves scanned), which
   * survive; the tables' chunks go back to the scan's workspace, where the restarted pass takes them
   * from instead of borrowing fresh regions next to them. Call after the parallel section has joined
   * and before re-planning; the spill is not reused.
   */
  // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
  // failure is rethrown with later ones attached.
  @SuppressWarnings("java:S1181")
  public void releaseTables() {
    Throwable failure = null;
    for (int p = 0; p < partitions; p++) {
      synchronized (locks[p]) {
        final NumericGroupAggTable table = shared[p];
        if (table != null) {
          sharedRehashes.add(table.rehashes());
          shared[p] = null;
          // Its storage is the restart's tables: hand it back to the workspace.
          try {
            table.retire();
          } catch (final Throwable cleanup) {
            failure = retainCleanupFailure(failure, cleanup);
          }
        }
        if (stripeBuffers != null && stripeBuffers[p] != null) {
          // The counters the estimate reads survive a release (they are what the pass SAW), so the
          // buffered stripes move to the spilled total rather than vanishing with their chunks.
          final long held = stripeBuffers[p].stripes();
          if (held > 0L) {
            buffered.add(-held);
            spilled.add(held);
          }
          try {
            stripeBuffers[p].release(workspace());
          } catch (final Throwable cleanup) {
            failure = retainCleanupFailure(failure, cleanup);
          }
        }
        if (p == 0 && zeroGroups != null) {
          final NumericGroupAggTable zero = zeroGroups;
          zeroGroups = null;
          try {
            zero.retire();
          } catch (final Throwable cleanup) {
            failure = retainCleanupFailure(failure, cleanup);
          }
        }
      }
    }
    RELEASES.increment();
    rethrowCleanupFailure(failure);
  }

  /**
   * The shared table of {@code part} as the post-scan merge base — detached, so it is merged into
   * exactly once — or a fresh table from {@code fresh} when nothing was spilled there. Post-scan
   * only, after the parallel section has joined.
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
