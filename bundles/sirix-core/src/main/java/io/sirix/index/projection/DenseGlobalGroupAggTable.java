/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SegmentAccess;
import org.jspecify.annotations.Nullable;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Reference;
import java.util.Arrays;

/**
 * Per-group aggregates for a group key that is a GLOBAL value-dictionary id, held in one native
 * block indexed by the id itself instead of one open-addressed hash table per worker.
 *
 * <p>
 * {@link GlobalValueDictionary} mints ids as dense positive integers, so id {@code i}'s accumulator
 * can live at {@code (i - 1) * slotWidth} and be reached with a multiply and an add: no mix, no
 * probe, no key compare, no rehash — and, because every worker addresses the SAME slot for the same
 * id, no per-worker table and therefore no merge at all. On JSONBench Q4/Q5 at 100M rows the merge
 * machinery it deletes ({@code buildPartitionIndex} + {@code mergePartitionIndexed}) was 12.7 % of
 * the hot path and the probe another 18.8 %.
 *
 * <p>
 * The accumulator block layout is byte-for-byte {@link ProjectionIndexByteScan#newGroupAggAcc}'s —
 * {@code [count, firstSeen, then per aggregate column: presentCount, sum, min, max]} — which is
 * what lets the selection, order plans and emission read a dense slot exactly as they read a
 * {@link NumericGroupAggTable} stripe or a standalone accumulator.
 *
 * <h2>Sizing</h2> The table costs {@code entryCount × slotWidth × 8} bytes whatever the group
 * count, so it is the LIVE DENSITY of the id space that decides whether it is a good trade. It is
 * affordable exactly when the dictionary is small relative to the aggregate width; the caller gates
 * on {@link #bytesFor} against a byte budget and falls back to the hash tables when it does not
 * fit. There is no growth path: a table that fits at construction fits for the whole scan.
 *
 * <h2>Concurrency</h2> Slots are SHARED across the scan's workers, so every lane update is atomic.
 * {@code count}, {@code presentCount} and {@code sum} accumulate with {@code getAndAdd}; the
 * monotone lanes ({@code firstSeen}, {@code min}, {@code max}) read plainly and only pay a
 * compare-and-exchange when the row actually improves the lane — the common case is one load and
 * one branch. A monotone lane's plain read can only be STALE IN THE LOSING DIRECTION (a min lane
 * never grows), so the read either skips correctly or loses the exchange and retries with the
 * witness.
 */
public final class DenseGlobalGroupAggTable {

  /** Largest lane count addressable by a group buffer. */
  private static final int MAX_LANES = Integer.MAX_VALUE - 8;

  private static final VarHandle INITIALIZED;

  static {
    try {
      INITIALIZED = MethodHandles.lookup()
                                 .findVarHandle(DenseGlobalGroupAggTable.class, "initializedSlots", long.class)
                                 .withInvokeExactBehavior();
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * A worker's ids-I-created list. Selection walks these instead of the whole id space: the table is
   * sized by the DICTIONARY (3.7M ids at 100M JSONBench rows) while the live groups are a third of
   * that, and scanning every slot to find them cost more than the fold it followed.
   *
   * <p>
   * Exactly one worker creates a given id — the one whose exchange moves the first-seen lane off
   * {@link Long#MAX_VALUE} — so the rosters PARTITION the live ids with no coordination and no
   * duplicates, which is what lets each roster be selected from in parallel and its winners be final.
   */
  public static final class IdRoster {
    private int[] ids;
    private int size;

    public IdRoster(final int initialCapacity) {
      if (initialCapacity < 1) {
        throw new IllegalArgumentException("initialCapacity must be >= 1, got " + initialCapacity);
      }
      this.ids = new int[initialCapacity];
    }

    void add(final int id) {
      if (size == ids.length) {
        ids = Arrays.copyOf(ids, ids.length << 1);
      }
      ids[size++] = id;
    }

    /** Ids this worker created, in first-touch order; only {@code 0..size()} is meaningful. */
    public int[] ids() {
      return ids;
    }

    public int size() {
      return size;
    }
  }

  private final int slotWidth;
  private final int aggColumns;
  private final long sumExactMask;
  /**
   * Whether the {@code count} lane is folded at all. It is one atomic add per matching row — the
   * single most expensive instruction on this path — and a top-K by {@code min} never reads it. The
   * lane a group's EXISTENCE is read from is {@code firstSeen}, not {@code count}, precisely so that
   * skipping this one cannot make a live group invisible; the caller proves nothing reads it (see
   * {@code SirixVectorizedExecutor#denseCountLaneRead}).
   */
  private final boolean foldCountLane;
  private final int maxId;
  private final GroupBuffer lanes;
  /**
   * The accumulator for group id {@code 0}. A global column's cells are minted ids, which start at
   * {@code 1}, so this slot exists for the same reason {@link NumericGroupAggTable}'s zero slot does:
   * a value id of {@code 0} must not silently fold into id {@code 1}'s block.
   */
  private final GroupBuffer zeroSlot;

  @SuppressWarnings("unused") // read and written through INITIALIZED
  private volatile long initializedSlots;

  /**
   * Bytes a table for {@code maxId} ids and {@code aggColumns} aggregate columns occupies, as a
   * {@code long} so an id space that cannot be addressed at all is still comparable against a budget
   * rather than overflowing into a small number.
   *
   * @param aggColumns aggregate columns per group
   * @param maxId highest dictionary id, i.e. the dictionary's entry count
   * @return the table's byte cost
   */
  public static long bytesFor(final int aggColumns, final long maxId) {
    if (maxId < 0L || maxId > (Long.MAX_VALUE / Long.BYTES) / slotWidthOf(aggColumns) - 1L) {
      throw new IllegalArgumentException("invalid dictionary id count: " + maxId);
    }
    return slotWidthOf(aggColumns) * (maxId + 1L) * Long.BYTES;
  }

  /** Longs per accumulator block for {@code aggColumns} aggregate columns. */
  private static int slotWidthOf(final int aggColumns) {
    if (aggColumns < 0 || aggColumns > (Integer.MAX_VALUE - 2) / 4) {
      throw new IllegalArgumentException("invalid aggregate column count: " + aggColumns);
    }
    return 2 + 4 * aggColumns;
  }

  /**
   * @param aggColumns aggregate columns per group ({@code slotWidth = 2 + 4 * aggColumns})
   * @param maxId highest id the dictionary can issue; ids run {@code 1..maxId}
   * @param sumExactMask which columns' SUM lanes the query reads (see
   *        {@link NumericGroupAggTable#sumsExact})
   * @param foldCountLane whether anything reads the {@code count} lane (see {@link #foldCountLane})
   * @throws IllegalArgumentException if the arguments are out of range or the table cannot be
   *         addressed as one buffer — the caller must have gated on {@link #bytesFor} first
   */
  public DenseGlobalGroupAggTable(final GroupWorkspace workspace, final int aggColumns, final int maxId,
      final long sumExactMask, final boolean foldCountLane) {
    this.foldCountLane = foldCountLane;
    if (maxId <= 0) {
      throw new IllegalArgumentException("maxId must be > 0, got " + maxId);
    }
    this.aggColumns = aggColumns;
    this.slotWidth = slotWidthOf(aggColumns);
    this.sumExactMask = sumExactMask;
    this.maxId = maxId;
    final long length = (long) slotWidth * maxId;
    if (length > MAX_LANES) {
      throw new IllegalArgumentException(
          "dense group table of " + maxId + " ids needs " + length + " lanes, which exceeds one buffer");
    }
    this.lanes = workspace.takeAccumulatorChunk((int) length, slotWidth);
    this.zeroSlot = ProjectionIndexByteScan.newGroupAggAcc(workspace, aggColumns, Long.MAX_VALUE);
  }

  // The workspace monitor makes capacity admission and table allocation one atomic operation.
  @SuppressWarnings("java:S2445")
  public static @Nullable DenseGlobalGroupAggTable tryCreate(final GroupWorkspace workspace, final int aggColumns,
      final int maxId, final long sumExactMask, final boolean foldCountLane, final int workers) {
    if (maxId <= 0 || workers <= 0) {
      throw new IllegalArgumentException("dense table needs positive dictionary and worker counts");
    }
    final long slotBytes = (long) slotWidthOf(aggColumns) * Long.BYTES;
    final long tableBytes = bytesFor(aggColumns, maxId) - slotBytes;
    synchronized (workspace) {
      if (!workspace.canAllocate(tableBytes, slotBytes, Math.addExact(workers, 1))) {
        return null;
      }
      return new DenseGlobalGroupAggTable(workspace, aggColumns, maxId, sumExactMask, foldCountLane);
    }
  }

  /** Aggregate columns this table's accumulator blocks carry. */
  public int aggColumns() {
    return aggColumns;
  }

  /** Longs per accumulator block. */
  public int slotWidth() {
    return slotWidth;
  }

  /** Highest id addressable; ids run {@code 1..maxId()}. */
  public int maxId() {
    return maxId;
  }

  /**
   * This table's {@link NumericGroupAggTable#sumsExact} mask — the kernel folds under the same rule.
   */
  public long sumExactMask() {
    return sumExactMask;
  }

  /** The interleaved storage; id {@code i}'s block starts at {@code (i - 1) * slotWidth()}. */
  public GroupBuffer lanes() {
    return lanes;
  }

  /** The accumulator block of the id-{@code 0} group; {@code count} lane {@code 0} means unseen. */
  public GroupBuffer zeroSlot() {
    return zeroSlot;
  }

  /** Block base of {@code id} within {@link #lanes()}. */
  public int baseOf(final int id) {
    return (id - 1) * slotWidth;
  }

  /**
   * Whether the block at {@code base} was ever folded into. The FIRST-SEEN lane answers it, not
   * {@code count}: every matching row lowers first-seen off {@link Long#MAX_VALUE} whatever the query
   * reads, so existence stays observable even when the count lane is skipped as unread.
   */
  public static boolean isLive(final GroupBuffer lanes, final int base) {
    return GroupLanes.get(lanes, base + 1) != Long.MAX_VALUE;
  }

  /**
   * Stamp the empty-accumulator pattern over ids {@code [fromId, toId)} — {@code min} lanes to
   * {@code Long.MAX_VALUE}, {@code max} lanes to {@code Long.MIN_VALUE}, everything else to zero.
   *
   * <p>
   * Split across the scan's workers rather than done in the constructor: at 100M rows the table is
   * hundreds of megabytes, and one thread walking it is time the fan-out is idle for. Every id must
   * be covered exactly once before the first fold — {@link #requireFullyInitialized} is the check.
   *
   * @param fromId first id to initialize (inclusive, {@code >= 1})
   * @param toId last id to initialize (exclusive, {@code <= maxId() + 1})
   */
  public void initIds(final int fromId, final int toId) {
    if (fromId < 1 || toId > maxId + 1 || fromId > toId) {
      throw new IllegalArgumentException("id range [" + fromId + ", " + toId + ") outside 1.." + maxId);
    }
    final GroupBuffer a = lanes;
    final int w = slotWidth;
    final int end = (toId - 1) * w;
    for (int base = (fromId - 1) * w; base < end; base += w) {
      GroupLanes.set(a, base + 1, Long.MAX_VALUE);
      for (int off = base + 2; off < base + w; off += 4) {
        GroupLanes.set(a, off + 2, Long.MAX_VALUE);
        GroupLanes.set(a, off + 3, Long.MIN_VALUE);
      }
    }
    @SuppressWarnings("unused")
    final long previouslyCovered = (long) INITIALIZED.getAndAdd(this, (long) toId - fromId);
  }

  /**
   * Fail loudly if any id was left uninitialized: an unstamped block would read {@code min == 0},
   * which is not "no rows yet" but a smaller minimum than any positive value can beat — a wrong
   * answer that no exception would mark.
   *
   * @throws IllegalStateException if {@link #initIds} did not cover every id exactly once
   */
  public void requireFullyInitialized() {
    lanes.requireLive();
    final long covered = (long) INITIALIZED.getVolatile(this);
    if (covered != maxId) {
      throw new IllegalStateException("dense group table initialized " + covered + " of " + maxId + " ids");
    }
  }

  /**
   * Fold one matching row into {@code id}'s block.
   *
   * <p>
   * {@code aggValues}/{@code aggPresence} are the row's leaf-local column slices, addressed by
   * {@code rowIdx} and by the {@code (w, bit)} the caller is already iterating — the same shape the
   * hash kernels fold from, so the two arms cannot disagree about which lane a value lands in.
   *
   * @param id the row's group id ({@code 0} routes to the zero slot)
   * @param firstSeenOrdinal the row's {@code (leaf, row)} ordinal, folded as a MINIMUM
   * @param roster this worker's created-id list; the id is appended iff this row created the group
   * @param decline set to {@code 1} when {@code id} is outside the sized range — the caller declines,
   *        because folding it elsewhere or dropping it are both wrong answers
   * @throws ArithmeticException when a READ sum lane overflows, exactly as the hash kernels throw
   */
  // Sonar S107: the operands of a hot path are passed one by one so that they stay in registers. An
  // operand object would cost a load at every use.
  @SuppressWarnings("java:S107")
  public void foldLane(final long id, final long firstSeenOrdinal, final MemorySegment[] aggSeg, final long[] laneBases,
      final long[][] aggPresence, final int aggCount, final int w, final int bit, final int rowIdx,
      final IdRoster roster, final long[] decline) {
    final GroupBuffer buffer;
    final int base;
    if (id == 0L) {
      buffer = zeroSlot;
      base = 0;
    } else if (id < 0L || id > maxId) {
      decline[0] = 1L;
      return;
    } else {
      buffer = lanes;
      base = (int) (id - 1L) * slotWidth;
    }
    final long block = buffer.accumulator(base, aggCount, 4);
    try {
      if (foldCountLane) {
        addLane(block, 0, 1L);
      }
      if (minLaneCreated(block, 1, firstSeenOrdinal) && id != 0L) {
        roster.add((int) id);
      }
      for (int a = 0; a < aggCount; a++) {
        if ((aggPresence[a][w] & 1L << bit) == 0L) {
          continue;
        }
        final long v = SegmentAccess.getLong(aggSeg[a], laneBases[a] + ((long) rowIdx << 3));
        final int aggBase = 2 + 4 * a;
        // The PRESENT count is never skippable: min/max/avg over a group whose every row lacked the
        // operand must answer the empty sequence, and this lane is the only thing that says so.
        addLane(block, aggBase, 1L);
        // Exact sum or DECLINE, and a lane no sum/avg reads is not folded at all — the same rule the
        // hash kernels obey, so a min-only query never declines on a sum it does not read.
        if (NumericGroupAggTable.sumsExact(sumExactMask, a)) {
          addExactLane(block, aggBase + 1, v);
        }
        minLane(block, aggBase + 2, v);
        maxLane(block, aggBase + 3, v);
      }
    } finally {
      Reference.reachabilityFence(buffer);
    }
  }

  /** {@code lane += delta}, for the accumulating lanes whose exchange loop would be pure overhead. */
  private static void addLane(final long block, final int index, final long delta) {
    // The access mode returns the previous value and exact invoke behaviour requires it be consumed;
    // the store is dead and eliminated.
    @SuppressWarnings("unused")
    final long previous = GroupKernelLanes.getAndAdd(block, index, delta);
  }

  /** {@code lane = min(lane, v)}; one plain load and a branch unless the row actually improves it. */
  private static void minLane(final long block, final int index, final long v) {
    long cur = GroupKernelLanes.getOpaque(block, index);
    while (v < cur) {
      final long witness = GroupKernelLanes.compareAndExchange(block, index, cur, v);
      if (witness == cur) {
        return;
      }
      cur = witness;
    }
  }

  /**
   * {@link #minLane} over the first-seen lane, reporting whether THIS call took the lane off
   * {@link Long#MAX_VALUE} — i.e. whether it created the group. Only one caller can: an exchange that
   * succeeds against any other witness found the lane already lowered.
   */
  private static boolean minLaneCreated(final long block, final int index, final long v) {
    long cur = GroupKernelLanes.getOpaque(block, index);
    while (v < cur) {
      final long witness = GroupKernelLanes.compareAndExchange(block, index, cur, v);
      if (witness == cur) {
        return cur == Long.MAX_VALUE;
      }
      cur = witness;
    }
    return false;
  }

  /** {@code lane = max(lane, v)}; the mirror of {@link #minLane}. */
  private static void maxLane(final long block, final int index, final long v) {
    long cur = GroupKernelLanes.getOpaque(block, index);
    while (v > cur) {
      final long witness = GroupKernelLanes.compareAndExchange(block, index, cur, v);
      if (witness == cur) {
        return;
      }
      cur = witness;
    }
  }

  /**
   * {@code lane += v} under {@link Math#addExact}. The check has to live INSIDE the exchange loop:
   * testing before the update would let a concurrent add push the lane past the range between the
   * test and the store, and an interpreter that promotes an overflowing sum must never see a wrapped
   * one here.
   */
  private static void addExactLane(final long block, final int index, final long v) {
    long cur = GroupKernelLanes.getOpaque(block, index);
    for (;;) {
      final long next = Math.addExact(cur, v);
      final long witness = GroupKernelLanes.compareAndExchange(block, index, cur, next);
      if (witness == cur) {
        return;
      }
      cur = witness;
    }
  }
}
