/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import io.sirix.cache.AggregationGrant;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.ScratchLease;
import io.sirix.cache.ScratchUse;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;

/**
 * The working memory of ONE grouped scan: every group table's stripes and index chunks, their
 * zero-key slots and the scan's standalone accumulators, borrowed from the allocator and returned
 * together when the scan ends.
 *
 * <p>
 * Group-by working memory used to be {@code long[]} chunks on the heap, recycled through free lists
 * that outlived the scan and kept up to a quarter of the heap for the next one. It is allocator
 * memory now ({@link ScratchUse#AGGREGATION}), counted by the allocator's one budget, and nothing
 * of it outlives the scan that borrowed it: {@link #close()} returns every region, and what the
 * allocator keeps resident for the next scan is its own decision, under its own ceiling.
 *
 * <p>
 * <b>Regions and chunks.</b> The workspace borrows regions (the first small, each next one twice
 * the last up to {@link #MAX_REGION_BYTES}) and carves them front to back. A table asks for a chunk
 * of a lane count ({@link #takeChunk}) and hands it back when it rehashes or is released
 * ({@link #giveChunk}); a chunk handed back serves the next request of the same lane count, so a
 * scan whose worker tables flush and whose partition tables grow many times per pass touches each
 * page once instead of once per table. Small blocks ({@link #newBlock}: a zero-key slot, a
 * standalone accumulator) are carved and not taken back.
 *
 * <p>
 * <b>Everything handed out is zero.</b> Allocator memory holds its previous borrower's bytes, and
 * an all-zero chunk is what a group table reads as "every bucket empty". A carved chunk is cleared
 * when it is carved and a returned chunk when it is returned. Returned buffer generations are
 * permanently invalid, even when a later table reuses the same bytes.
 *
 * <p>
 * <b>Lifetime.</b> Each buffer retains this workspace. Checked public access rejects closed
 * workspaces and returned chunks; audited internal kernels resolve a complete record before their
 * lane loop. Closing is an owner operation, after every accepted worker has completed, including on
 * cancellation and exceptional submission; it must never race any table or buffer access. Whoever
 * opens a workspace closes it after the last result has been copied out of its tables. A query does
 * that around each grouped scan. Only a table or a spill built outside any scan (a test, a tool)
 * uses a {@linkplain #selfReturning() self-returning} workspace, which nobody has to close.
 *
 * <p>
 * Thread-safe: the tables of a scan's workers take and give chunks concurrently. Chunks are
 * exchanged at most once per 128 KiB of table, so one monitor serves the bookkeeping. The clearing
 * is done outside it, by the thread that takes or gives the chunk: workers clear (and, on memory
 * the allocator has just mapped, first touch) their own chunks side by side instead of queueing
 * behind one another. That is safe for the same reason writing a table is: the workspace is closed
 * only once every worker of the scan has finished.
 */
public final class GroupWorkspace implements AutoCloseable {

  /** The first region a workspace borrows: a small group-by never needs more. */
  static final long FIRST_REGION_BYTES = 64L << 10;

  /** The size regions grow to; allocator regions of this size are unmapped when nobody needs them. */
  static final long MAX_REGION_BYTES = 2L << 20;

  private static final LongAdder OPENED = new LongAdder();
  private static final LongAdder CLOSED = new LongAdder();
  private static final LongAdder REUSED = new LongAdder();

  /**
   * Returns the regions of self-returning workspaces; created with the first one, never in a query.
   */
  private static final class SelfReturning {
    private static final Cleaner CLEANER = Cleaner.create();

    private SelfReturning() {}
  }

  /**
   * The regions a workspace has borrowed. Kept apart from the workspace so that a cleaner can return
   * them without holding the workspace reachable; guarded by its own monitor because the cleaner's
   * thread is not one that ever held the workspace's.
   */
  private static final class Regions implements Runnable {
    private ScratchLease[] leases = new ScratchLease[4];
    private int count;
    private boolean returned;
    private AggregationGrant grant;

    synchronized void add(final ScratchLease lease) {
      if (returned) {
        lease.close();
        throw new IllegalStateException("the grouped scan's workspace is closed");
      }
      if (count == leases.length) {
        leases = Arrays.copyOf(leases, count << 1);
      }
      leases[count++] = lease;
    }

    synchronized int count() {
      return count;
    }

    /** Return every region to the allocator, once. */
    @Override
    public synchronized void run() {
      if (returned) {
        return;
      }
      returned = true;
      for (int i = 0; i < count; i++) {
        leases[i].close();
        leases[i] = null;
      }
      count = 0;
      if (grant != null) {
        grant.close();
        grant = null;
      }
      CLOSED.increment();
    }
  }

  /** One free list per lane count in use. */
  private static final class Spare {
    private MemorySegment[] chunks = new MemorySegment[8];
    private int count;

    void push(final MemorySegment chunk) {
      if (count == chunks.length) {
        chunks = Arrays.copyOf(chunks, count << 1);
      }
      chunks[count++] = chunk;
    }

    MemorySegment pop() {
      final MemorySegment chunk = chunks[--count];
      chunks[count] = null;
      return chunk;
    }
  }

  private final MemorySegmentAllocator allocator;
  private final boolean scan;
  private final Regions regions = new Regions();
  /**
   * The registration that returns the regions of a self-returning workspace; {@code null} otherwise.
   */
  private final Cleaner.@Nullable Cleanable selfReturn;
  /** The region being carved, {@code null} before the first request and after {@link #close()}. */
  private MemorySegment carving;
  private long carved;
  private long nextRegionBytes = FIRST_REGION_BYTES;
  private final Int2ObjectOpenHashMap<Spare> spareByLanes = new Int2ObjectOpenHashMap<>();
  /** Written under the monitor; read without it by {@link #giveChunk} before it clears a chunk. */
  private volatile boolean closed;
  /**
   * {@link #closed} for the lane accesses of this workspace's buffers and tables, as a plain field: a
   * volatile read per access would stop the compiler from keeping anything in a register across it.
   * Sound because closing never races an access (see the class comment), so every access that is
   * allowed to happen at all is ordered after the write it must see.
   */
  boolean ended;

  private long borrowedBytes;
  private long nextGeneration;
  private long chunksCarved;
  private long chunksReused;
  private long chunksGiven;

  /** A workspace on the process allocator. */
  public GroupWorkspace() {
    this(Allocators.getInstance());
  }

  /** A workspace on {@code allocator} (tests that bound the budget). */
  public GroupWorkspace(final MemorySegmentAllocator allocator) {
    this(allocator, false, false);
  }

  private GroupWorkspace(final MemorySegmentAllocator allocator, final boolean selfReturning, final boolean scan) {
    this.allocator = requireNonNull(allocator, "allocator");
    this.scan = scan;
    OPENED.increment();
    // Last: the registration must not see a half-built workspace, and the action holds the regions
    // only.
    this.selfReturn = selfReturning
        ? SelfReturning.CLEANER.register(this, regions)
        : null;
  }

  /**
   * A workspace nobody has to close: its regions go back to the allocator once the workspace itself
   * is unreachable (or at {@link #close()}, whichever comes first). It is what a table or a spill
   * built outside a grouped scan takes its memory from. The table, the spill and every table the
   * spill hands out hold the workspace, so it lives as long as any of them does.
   *
   * <p>
   * Every returned buffer holds this workspace too, so retaining a record keeps its memory alive. A
   * query never uses one; its scans close their workspaces themselves.
   */
  public static GroupWorkspace selfReturning() {
    return new GroupWorkspace(Allocators.getInstance(), true, false);
  }

  /**
   * A production scan: acquire exclusive fixed capacity at planning, or before its first region when
   * it has no pass planner. Incremental grants are reserved for standalone tables and tools.
   */
  public static GroupWorkspace forScan() {
    return forScan(Allocators.getInstance());
  }

  /** A scan using an explicit allocator, for capacity and admission tests. */
  public static GroupWorkspace forScan(final MemorySegmentAllocator allocator) {
    return new GroupWorkspace(allocator, false, true);
  }

  /**
   * An all-zero chunk of {@code lanes} lanes: one a table handed back when there is one of that lane
   * count, else a freshly carved one.
   *
   * @throws IllegalArgumentException if {@code lanes} is not positive
   * @throws IllegalStateException if the workspace is closed
   * @throws OutOfMemoryError when the allocator's budget cannot serve another region
   */
  public GroupBuffer takeChunk(final int lanes) {
    return takeChunk(lanes, lanes, 0, lanes);
  }

  GroupBuffer takeRecordChunk(final int lanes, final int stride, final int accumulatorLanes) {
    return takeChunk(lanes, stride, 1, accumulatorLanes);
  }

  private GroupBuffer takeChunk(final int lanes, final int stride, final int offset, final int accumulatorLanes) {
    if (lanes <= 0) {
      throw new IllegalArgumentException("lanes must be positive: " + lanes);
    }
    final MemorySegment chunk;
    synchronized (this) {
      requireOpen();
      final Spare spare = spareByLanes.get(lanes);
      if (spare != null && spare.count > 0) {
        chunksReused++;
        REUSED.increment();
        return new GroupBuffer(spare.pop(), this, ++nextGeneration, stride, offset, accumulatorLanes);
      }
      chunk = carve((long) lanes << 3);
      chunksCarved++;
    }
    // Cleared by the taking thread, outside the monitor: see the class comment.
    chunk.fill((byte) 0);
    return new GroupBuffer(chunk, this, nextGeneration(), stride, offset, accumulatorLanes);
  }

  /**
   * Hand a chunk back for the next {@link #takeChunk} of its lane count. It is cleared here and must
   * not be touched by the caller afterwards. A chunk given to a closed workspace is ignored: its
   * region went back to the allocator with the rest.
   *
   * @param chunk a chunk this workspace handed out
   */
  public void giveChunk(final GroupBuffer chunk) {
    requireNonNull(chunk, "chunk");
    if (closed) {
      return; // its memory is another borrower's by now: not ours to clear
    }
    final int lanes = GroupLanes.count(chunk);
    chunk.retire(this);
    // Cleared by the giving thread, outside the monitor: see the class comment.
    chunk.segment.fill((byte) 0);
    synchronized (this) {
      if (closed) {
        return;
      }
      Spare spare = spareByLanes.get(lanes);
      if (spare == null) {
        spare = new Spare();
        spareByLanes.put(lanes, spare);
      }
      spare.push(chunk.segment);
      chunksGiven++;
    }
  }

  /**
   * An all-zero block of {@code lanes} lanes that is never taken back: a table's zero-key slot, a
   * standalone accumulator, the scratch a pass folds its out-of-range keys into.
   *
   * @throws IllegalArgumentException if {@code lanes} is not positive
   * @throws IllegalStateException if the workspace is closed
   */
  public GroupBuffer newBlock(final int lanes) {
    if (lanes <= 0) {
      throw new IllegalArgumentException("lanes must be positive: " + lanes);
    }
    final MemorySegment block;
    synchronized (this) {
      requireOpen();
      block = carve((long) lanes << 3);
    }
    block.fill((byte) 0);
    return new GroupBuffer(block, this, nextGeneration());
  }

  /** Carve {@code bytes} from the current region, borrowing the next one when it does not fit. */
  private MemorySegment carve(final long bytes) {
    MemorySegment region = carving;
    if (region == null || carved + bytes > region.byteSize()) {
      final AggregationGrant grant = grant();
      final long remaining = grant.capacity() - grant.usedBytes();
      // Incremental tools-only workspaces grow on demand. A planned scan spends only its grant,
      // including capacity rounding, and uses a smaller last region when the growth ramp won't fit.
      final long target = !scan && plannedBytes == 0L
          ? nextRegionBytes
          : Math.min(nextRegionBytes, Long.highestOneBit(remaining));
      final long regionBytes = Math.max(bytes, target);
      final ScratchLease lease = grant.borrow(regionBytes);
      try {
        regions.add(lease);
      } catch (final RuntimeException | Error failure) {
        lease.close();
        throw failure;
      }
      borrowedBytes += lease.capacity();
      nextRegionBytes = Math.min(MAX_REGION_BYTES, nextRegionBytes << 1);
      region = lease.segment();
      carving = region;
      carved = 0L;
    }
    final MemorySegment slice = region.asSlice(carved, bytes);
    carved += bytes;
    return slice;
  }

  private synchronized long nextGeneration() {
    return ++nextGeneration;
  }

  private long plannedBytes;
  private long planningOverhead;
  private int plannedWorkers;
  private int plannedPartitions;
  private int plannedStride;

  // A current tail and retired sub-chunk growth per storage/index/spill stream. Full-chunk
  // growth and merge overlap are already in bytesPerGroup; these are their partial chunks.
  private static final long PARTITION_TAIL_BYTES = 6L * (128L << 10);

  private AggregationGrant grant() {
    return grant(FIRST_REGION_BYTES);
  }

  private AggregationGrant grant(final long minimum) {
    AggregationGrant grant = regions.grant;
    if (grant == null) {
      grant = scan
          ? allocator.aggregationBudget().acquire(Math.max(allocator.aggregationBudget().capacity(), minimum), minimum)
          : allocator.aggregationBudget().incremental();
      regions.grant = grant;
    }
    return grant;
  }

  /**
   * Reserve capacity before the first pass. Fixed worker tables and chunk/region tails are removed
   * once here; per-group growth and merge overlap are charged by the planner's bytes-per-group.
   */
  public synchronized long preparePlan(final int workers, final int partitions, final int strideLanes) {
    requireOpen();
    if (workers <= 0 || partitions <= 0 || strideLanes <= 0) {
      throw new IllegalArgumentException("invalid aggregation planning geometry");
    }
    if (plannedBytes == 0L) {
      final long workerBytes = Math.multiplyExact(GroupTableSpill.workerPeakBytes(strideLanes), workers);
      final long workerTails = Math.multiplyExact((long) workers, PARTITION_TAIL_BYTES);
      // The initial region ramp and one final region; full 2 MiB regions waste less than
      // 128 KiB each because every group chunk is at most that large. Reserve that 1/16 too.
      final long fixed = Math.addExact(Math.addExact(workerBytes, workerTails), 2 * MAX_REGION_BYTES);
      final long needed =
          Math.addExact(Math.addExact(fixed, PARTITION_TAIL_BYTES), GroupTableSpill.bytesPerGroup(strideLanes));
      final long minimum = Math.addExact(needed, Math.addExact(needed, 14L) / 15L);
      plannedBytes = grant(minimum).preparePlan(minimum);
      planningOverhead = Math.addExact(fixed, (plannedBytes + 15L) / 16L);
      plannedWorkers = workers;
      plannedPartitions = partitions;
      plannedStride = strideLanes;
    } else if (plannedWorkers != workers || plannedPartitions != partitions || plannedStride != strideLanes) {
      throw new IllegalStateException("a scan cannot change its planned aggregation layout");
    }
    return usableGroupBytes();
  }

  /** Maximum group capacity with one active partition; the pass planner charges its actual fanout. */
  public synchronized long usableGroupBytes() {
    return usableGroupBytes(1);
  }

  /** Group capacity after the fixed overhead and the live partition streams of this pass. */
  public synchronized long usableGroupBytes(final int activePartitions) {
    if (activePartitions <= 0 || (plannedBytes != 0L && activePartitions > plannedPartitions)) {
      throw new IllegalArgumentException("invalid live aggregation partition count");
    }
    return Math.max(0L, plannedBytes - planningOverhead - activePartitions * PARTITION_TAIL_BYTES);
  }

  public synchronized long grantedBytes() {
    return regions.grant == null
        ? 0L
        : regions.grant.capacity();
  }

  public synchronized long peakUsedBytes() {
    return regions.grant == null
        ? borrowedBytes
        : regions.grant.peakUsedBytes();
  }

  /** Actual rounded region capacity currently charged to this scan. */
  public synchronized long usedBytes() {
    return regions.grant == null
        ? 0L
        : regions.grant.usedBytes();
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("the grouped scan's workspace is closed");
    }
  }

  /**
   * Return every region to the allocator. Idempotent. Nothing this workspace handed out may be read
   * or written afterwards.
   */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    ended = true;
    carving = null;
    spareByLanes.clear();
    final Cleaner.Cleanable registration = selfReturn;
    if (registration != null) {
      registration.clean(); // runs the regions' return once and drops the registration
    } else {
      regions.run();
    }
  }

  /** Whether {@link #close()} has run. */
  public boolean isClosed() {
    return closed;
  }

  /** Bytes this workspace has borrowed from the allocator so far (regions, at their capacity). */
  public synchronized long borrowedBytes() {
    return borrowedBytes;
  }

  /** Regions borrowed and not yet returned. */
  public int regions() {
    return regions.count();
  }

  /** Chunks carved fresh, over the workspace's life (tests). */
  public synchronized long chunksCarved() {
    return chunksCarved;
  }

  /** Chunk requests served by a chunk a table had handed back (tests). */
  public synchronized long chunksReused() {
    return chunksReused;
  }

  /** Chunks handed back (tests). */
  public synchronized long chunksGiven() {
    return chunksGiven;
  }

  /** Chunks of {@code lanes} lanes waiting for reuse (tests). */
  public synchronized int spareChunks(final int lanes) {
    final Spare spare = spareByLanes.get(lanes);
    return spare == null
        ? 0
        : spare.count;
  }

  /** Workspaces opened, process-wide (test observability). */
  public static long openedCount() {
    return OPENED.sum();
  }

  /**
   * Workspaces whose regions went back to the allocator, process-wide: equal to
   * {@link #openedCount()} when no scan is running and no self-returning workspace is alive.
   */
  public static long closedCount() {
    return CLOSED.sum();
  }

  /**
   * Chunk requests served by a chunk a table had handed back, over every workspace of the process
   * (test observability: a query's workspace is not reachable from outside it).
   */
  public static long reusedCount() {
    return REUSED.sum();
  }
}
