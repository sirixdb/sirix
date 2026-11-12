/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import static io.sirix.cache.ShardedPageCache.retainCleanupFailure;
import static io.sirix.cache.ShardedPageCache.rethrowCleanupFailure;

import io.sirix.cache.Allocators;
import io.sirix.cache.AggregationGrant;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.ScratchLease;
import io.sirix.cache.ScratchUse;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
 * page once instead of once per table. Blocks from {@link #newBlock} are carved and not taken back.
 *
 * <p>
 * <b>Everything handed out is zero.</b> Allocator memory holds its previous borrower's bytes, a
 * returned chunk its last table's, and an all-zero chunk is what a group table reads as "every
 * bucket empty". A chunk is therefore cleared when it is taken, carved or returned alike, and not
 * when it is given back: what is cleared is then in the cache for the table that asked for it, and
 * a chunk nobody takes again is never cleared at all. Returned buffer generations are permanently
 * invalid, even when a later table reuses the same bytes.
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
 * is done outside it, by the thread that takes the chunk: workers clear (and, on memory the
 * allocator has just mapped, first touch) their own chunks side by side instead of queueing behind
 * one another. That is safe for the same reason writing a table is: the workspace is closed only
 * once every worker of the scan has finished.
 */
public final class GroupWorkspace implements AutoCloseable {

  /** A page of zero lanes, the source {@link #clear} copies from. Never written. */
  private static final long[] ZERO_PAGE = new long[512];
  private static final long ZERO_PAGE_BYTES = (long) ZERO_PAGE.length << 3;

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

    synchronized void prepare() {
      if (returned) {
        throw new IllegalStateException("the grouped scan's workspace is closed");
      }
      if (count == leases.length) {
        leases = Arrays.copyOf(leases, count << 1);
      }
    }

    synchronized void add(final ScratchLease lease) {
      if (returned) {
        throw new IllegalStateException("the grouped scan's workspace is closed");
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
      Throwable failure = null;
      for (int i = 0; i < count; i++) {
        final ScratchLease lease = leases[i];
        leases[i] = null;
        try {
          lease.close();
        } catch (final Throwable cleanup) {
          failure = retainCleanupFailure(failure, cleanup);
        }
      }
      count = 0;
      if (grant != null) {
        try {
          grant.close();
        } catch (final Throwable cleanup) {
          failure = retainCleanupFailure(failure, cleanup);
        }
        grant = null;
      }
      CLOSED.increment();
      rethrowCleanupFailure(failure);
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

  GroupBuffer takeAccumulatorChunk(final int lanes, final int stride) {
    return takeChunk(lanes, stride, 0, stride);
  }

  private GroupBuffer takeChunk(final int lanes, final int stride, final int offset, final int accumulatorLanes) {
    if (lanes <= 0) {
      throw new IllegalArgumentException("lanes must be positive: " + lanes);
    }
    final MemorySegment chunk;
    final long generation;
    synchronized (this) {
      requireOpen();
      final Spare spare = spareByLanes.get(lanes);
      if (spare != null && spare.count > 0) {
        chunksReused++;
        REUSED.increment();
        chunk = spare.pop();
      } else {
        chunk = carve((long) lanes << 3);
        chunksCarved++;
      }
      generation = ++nextGeneration;
    }
    // Cleared by the taking thread, outside the monitor: see the class comment.
    clear(chunk);
    return new GroupBuffer(chunk, this, generation, stride, offset, accumulatorLanes);
  }

  /**
   * Hand a chunk back for the next {@link #takeChunk} of its lane count. The caller must not touch it
   * afterwards; its bytes stay as they are until it is taken again. A chunk given to a closed
   * workspace is ignored: its region went back to the allocator with the rest.
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
   * An all-zero block of {@code lanes} lanes that is never taken back.
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
    clear(block);
    return new GroupBuffer(block, this, nextGeneration());
  }

  /**
   * Zero {@code memory}, a whole number of lanes.
   *
   * <p>
   * By copying from a page of zeros, not by {@link MemorySegment#fill}: a fill reaches
   * {@code Unsafe.setMemory}, whose x86-64 stub stores eight bytes at a time, while the copy stubs
   * move whole vectors. A table that grows to a million groups takes about twice its final size in
   * chunks, each of them cleared here; building one ran 324 cycles a row with the fill and 320 with
   * the copy, which is what the heap table it replaced takes.
   */
  private static void clear(final MemorySegment memory) {
    final long bytes = memory.byteSize();
    long done = 0L;
    while (bytes - done >= ZERO_PAGE_BYTES) {
      MemorySegment.copy(ZERO_PAGE, 0, memory, ValueLayout.JAVA_LONG_UNALIGNED, done, ZERO_PAGE.length);
      done += ZERO_PAGE_BYTES;
    }
    if (done < bytes) {
      MemorySegment.copy(ZERO_PAGE, 0, memory, ValueLayout.JAVA_LONG_UNALIGNED, done, (int) (bytes - done >>> 3));
    }
  }

  /** Carve {@code bytes} from the current region, borrowing the next one when it does not fit. */
  private MemorySegment carve(final long bytes) {
    MemorySegment region = carving;
    if (region == null || carved + bytes > region.byteSize()) {
      final AggregationGrant grant = grant();
      final long remaining = grant.capacity() - grant.usedBytes();
      // Incremental tools-only workspaces grow on demand. A planned scan spends only its grant,
      // including capacity rounding, and uses a smaller last region when the growth ramp won't fit.
      final long regionBytes = regionBytes(bytes, nextRegionBytes, remaining);
      regions.prepare();
      final ScratchLease lease = grant.borrow(regionBytes);
      try {
        regions.add(lease);
      } catch (final Throwable failure) {
        try {
          lease.close();
        } catch (final Throwable cleanup) {
          retainCleanupFailure(failure, cleanup);
        }
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

  private long regionBytes(final long bytes, final long nextBytes, final long remaining) {
    final long target = !scan && plannedBytes == 0L
        ? nextBytes
        : Math.min(nextBytes, Long.highestOneBit(remaining));
    return Math.max(bytes, target);
  }

  synchronized boolean canAllocate(final long firstBytes, final long additionalBytes, final int additionalBlocks) {
    requireOpen();
    if (firstBytes <= 0L || additionalBytes <= 0L || additionalBlocks < 0) {
      throw new IllegalArgumentException("invalid aggregation allocation geometry");
    }
    final boolean openingGrant = regions.grant == null;
    final long capacity = openingGrant
        ? allocator.aggregationBudget().capacity()
        : regions.grant.capacity();
    if (capacity == 0L) {
      return false;
    }
    final AggregationGrant grant = grant(Math.min(FIRST_REGION_BYTES, capacity));
    long remaining = grant.capacity() - grant.usedBytes();
    long tail = carving == null
        ? 0L
        : carving.byteSize() - carved;
    long nextBytes = nextRegionBytes;
    for (long block = 0; block <= additionalBlocks; block++) {
      final long bytes = block == 0
          ? firstBytes
          : additionalBytes;
      if (bytes > tail) {
        final long regionCapacity = grant.allocationCapacity(regionBytes(bytes, nextBytes, remaining));
        if (regionCapacity > remaining) {
          if (openingGrant) {
            grant.close();
            regions.grant = null;
          }
          return false;
        }
        remaining -= regionCapacity;
        tail = regionCapacity;
        nextBytes = Math.min(MAX_REGION_BYTES, nextBytes << 1);
      }
      tail -= bytes;
    }
    return true;
  }

  private synchronized long nextGeneration() {
    return ++nextGeneration;
  }

  private long plannedBytes;
  private long planningOverhead;
  private int requestedWorkers;
  private int plannedWorkers;
  private int plannedWorkerHint;
  private int plannedFlushThreshold;
  private int plannedBatchLeaves;
  private int plannedPartitions;
  private int plannedStride;

  // A current tail and retired sub-chunk growth per storage/index/spill stream. Full-chunk
  // growth and merge overlap are already in bytesPerGroup; these are their partial chunks.
  private static final long PARTITION_TAIL_BYTES = 6L * (128L << 10);

  /**
   * The partial chunks of a partition in multiples of its groups' charge, while they are smaller than
   * {@link #PARTITION_TAIL_BYTES}. Under the stripe spill every stream of a partition is sized by
   * what lands in it: a stripe buffer's chunks double from eight stripes, and a partition table is
   * created at the stripes of its first compaction and doubles from there. The chunk a stream is
   * filling and the smaller ones it has outgrown are therefore each smaller than the table the stream
   * has reached, which has at most 8/3 buckets a group: 16/3 stripes and 16/3 index lanes a group at
   * the worst, under 8/3 of the charge at any stripe width.
   */
  private static final long PARTITION_TAIL_CHARGES = 3L;

  /**
   * Stripes the first chunks of a partition hold before its groups account for them: a table of
   * sixteen buckets, its sixteen index lanes and the buffer's first eight stripes are at most 32
   * stripes at any width, and twice that covers the chunks a first growth retires.
   */
  private static final long PARTITION_FIRST_CHUNK_STRIPES = 64L;

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
      int hint = GroupTableSpill.workerTableHint();
      int threshold = GroupTableSpill.effectiveFlushThreshold();
      int batchLeaves = GroupTableSpill.subChunkLeaves();
      final long capacity = regions.grant == null
          ? allocator.aggregationBudget().capacity()
          : regions.grant.capacity();
      long fixed;
      long minimum;
      int fittedWorkers;
      while (true) {
        final long perWorker = Math.addExact(GroupTableSpill.workerPeakBytes(strideLanes, hint, threshold, batchLeaves),
            PARTITION_TAIL_BYTES);
        final long shared =
            Math.addExact(2 * MAX_REGION_BYTES + PARTITION_TAIL_BYTES, GroupTableSpill.bytesPerGroup(strideLanes));
        final long available = capacity - (capacity + 15L) / 16L - shared;
        fittedWorkers = (int) Math.min(workers, Math.max(0L, available / perWorker));
        if (fittedWorkers > 0) {
          fixed = Math.addExact(Math.multiplyExact(perWorker, fittedWorkers), 2 * MAX_REGION_BYTES);
          final long needed =
              Math.addExact(fixed, Math.addExact(PARTITION_TAIL_BYTES, GroupTableSpill.bytesPerGroup(strideLanes)));
          minimum = Math.addExact(needed, Math.addExact(needed, 14L) / 15L);
          break;
        }
        if (hint == 16 && threshold == 1 && batchLeaves == 1) {
          return 0L;
        }
        hint = Math.max(16, hint / 2);
        threshold = Math.max(1, threshold / 2);
        batchLeaves = Math.max(1, batchLeaves / 2);
      }
      plannedBytes = grant(minimum).preparePlan(minimum);
      planningOverhead = Math.addExact(fixed, (plannedBytes + 15L) / 16L);
      requestedWorkers = workers;
      plannedWorkers = fittedWorkers;
      plannedWorkerHint = hint;
      plannedFlushThreshold = threshold;
      plannedBatchLeaves = batchLeaves;
      plannedPartitions = partitions;
      plannedStride = strideLanes;
    } else if (requestedWorkers != workers || plannedPartitions != partitions || plannedStride != strideLanes) {
      throw new IllegalStateException("a scan cannot change its planned aggregation layout");
    }
    return usableGroupBytes();
  }

  public int plannedWorkers() {
    return plannedWorkers;
  }

  int workerTableHint() {
    return plannedBytes == 0L
        ? GroupTableSpill.workerTableHint()
        : plannedWorkerHint;
  }

  int flushThreshold(final int configured) {
    return plannedBytes == 0L
        ? configured
        : plannedFlushThreshold;
  }

  int batchLeaves() {
    return plannedBytes == 0L
        ? GroupTableSpill.subChunkLeaves()
        : plannedBatchLeaves;
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

  /**
   * The groups a pass over {@code activePartitions} partitions can hold in this scan's grant: every
   * group at its charge, and the partial chunks of the partitions that hold one.
   *
   * <p>
   * A partition owes partial chunks only once a group has landed in it, and never more than
   * {@link #PARTITION_TAIL_BYTES}. A pass limited to few groups therefore owes few tails, however
   * wide the merge split is: reserving the full tail of every partition before the first group made a
   * plan over a thousand partitions start with several passes, each a scan of the whole input, on any
   * grant below their 768 MiB. Three bounds hold at once and the pass may use the widest:
   * <ul>
   * <li>every partition at its full tail, the bound that binds once the partitions are large;
   * <li>one full tail for each group, since a pass of fewer groups than partitions fills no more
   * partitions than it has groups;
   * <li>under the stripe spill, tails in proportion to what the partitions hold
   * ({@link #PARTITION_TAIL_CHARGES}) plus their first chunks
   * ({@link #PARTITION_FIRST_CHUNK_STRIPES}).
   * </ul>
   * A plan that proves too small for the grant after all fails its allocation, and the pass restarts
   * with more passes as it does past its group budget.
   *
   * @param activePartitions partitions the largest pass owns
   * @return groups that pass can hold; zero before {@link #preparePlan}
   * @throws IllegalArgumentException if the count is not positive or exceeds the planned partitions
   */
  public synchronized long usableGroups(final int activePartitions) {
    if (activePartitions <= 0 || (plannedBytes != 0L && activePartitions > plannedPartitions)) {
      throw new IllegalArgumentException("invalid live aggregation partition count");
    }
    final long usable = plannedBytes - planningOverhead;
    if (usable <= 0L) {
      return 0L;
    }
    final long charge = GroupTableSpill.bytesPerGroup(plannedStride);
    final long everyTail = Math.max(0L, usable - activePartitions * PARTITION_TAIL_BYTES) / charge;
    final long tailPerGroup = heldGroups(usable, activePartitions, charge, PARTITION_TAIL_BYTES);
    if (!GroupTableSpill.stripeSpillEnabled()) {
      // Flushes probe into partition tables created at the worker hint: a chunk a stream, whatever lands.
      return Math.max(everyTail, tailPerGroup);
    }
    final long firstChunks = PARTITION_FIRST_CHUNK_STRIPES * plannedStride * Long.BYTES;
    return Math.max(Math.max(everyTail, tailPerGroup),
        heldGroups(usable, activePartitions, (1L + PARTITION_TAIL_CHARGES) * charge, firstChunks));
  }

  /**
   * The most groups {@code g} with {@code g * perGroup + min(g, partitions) * perPartition} within
   * {@code usable}.
   */
  private static long heldGroups(final long usable, final int partitions, final long perGroup,
      final long perPartition) {
    final long everyPartition = (usable - partitions * perPartition) / perGroup;
    return everyPartition >= partitions
        ? everyPartition
        : usable / (perGroup + perPartition);
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
