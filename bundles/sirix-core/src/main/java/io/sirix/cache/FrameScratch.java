/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.HftBoundaryTelemetry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Allocator-owned scratch regions, exclusively borrowed through {@link ScratchLease} under the same
 * physical budget as page frames and cache slabs.
 *
 * <h2>Capacities</h2>
 *
 * Every buffer has its own mapped region. Requests round up to power-of-two capacities from 4 KiB
 * through 16 MiB; larger requests use exactly sized, page-rounded regions. The 16 MiB capacity
 * covers coalesced reads spanning 8 MiB plus the final page's length header.
 *
 * <h2>Reuse</h2>
 *
 * Returned buffers retain their views and contents in thread-striped idle arrays. A warm borrow
 * takes one array entry by compare-and-set; a return publishes it the same way. Overflow goes to
 * intrusive spare lists per capacity, bounded in bytes together with recycled cache slabs. Exactly
 * sized buffers are unmapped on return. Idle backing of every capacity can be reclaimed at
 * commitment admission; lent backing is never reclaimed or moved.
 *
 * <h2>Budget</h2>
 *
 * Ordinary scratch may use the non-execution allowance, including the progress reserve left by page
 * frames and cache slabs. Aggregation regions charge an exclusive execution grant. On return, those
 * regions must transfer their commitment outside the execution floor before being retained. A
 * request larger than its allowance is refused immediately. A mandatory request within its
 * allowance reclaims idle backing, asks the page caches for relief, and waits; an optional request
 * can decline.
 *
 * <h2>Cost</h2>
 *
 * Ordinary buffers up to 256 KiB retain their active charge from creation to destruction and do not
 * touch shared counters on an idle-array borrow or return. Their idle bytes are derived on demand.
 * Idle arrays bound that population; excess spares count against the recycle bound. Larger buffers
 * and grant-backed borrows update active and lent-region counters per borrow, and every spare-list
 * transfer updates the recycle charge.
 */
final class FrameScratch extends ScratchSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(FrameScratch.class);

  /** Reusable capacities above 256 KiB. */
  static final long[] REGION_CAPACITIES = {512L << 10, 1L << 20, 2L << 20, 4L << 20, 8L << 20, 16L << 20};

  /** Capacities up to 256 KiB, with creation-to-destruction accounting for ordinary scratch. */
  static final int SMALL_CLASSES = FrameSlotAllocator.SIZE_CLASSES.length;

  /** Reusable capacities: small classes, then {@link #REGION_CAPACITIES}. */
  static final int CLASS_COUNT = SMALL_CLASSES + REGION_CAPACITIES.length;

  /** Pseudo class of exactly sized regions beyond the largest capacity; never kept idle. */
  static final int EXACT_CLASS = CLASS_COUNT;

  private static final long LARGEST_CAPACITY = REGION_CAPACITIES[REGION_CAPACITIES.length - 1];

  /** log2 of the smallest capacity (4 KiB). */
  private static final int SMALLEST_SHIFT = 12;

  private static final long PAGE_BYTES = 4096L;

  /**
   * Idle slots are this many references apart, so that two threads returning to neighbouring slots do
   * not write the same cache line (16 compressed or 8 full references per 64 bytes).
   */
  private static final int IDLE_STRIDE_SHIFT = 4;

  /**
   * Idle buffers kept per capacity: {@code -Dsirix.allocator.scratchIdleBuffers}, rounded up to a
   * power of two within [1, 256]. The default is twice the processors within [8, 64], the population
   * of the reader pool this replaces.
   */
  static final String IDLE_BUFFERS_PROPERTY = "sirix.allocator.scratchIdleBuffers";

  /**
   * Test seam: fill a buffer with {@code 0x5A} when it is returned, so a reader of a returned buffer
   * and a borrower that assumes zeroed memory both fail instead of passing on a lucky layout.
   */
  private static volatile boolean poisonOnReturn = Boolean.getBoolean("sirix.allocator.poisonScratch");

  private final FrameSlotAllocator allocator;
  private final VirtualMemory memory;

  /**
   * Idle buffers per reusable capacity; slot {@code i} lives at index {@code i << IDLE_STRIDE_SHIFT}.
   */
  private final ScratchCarrier[][] idle;
  private final int idleMask;

  /**
   * Idle region-backed buffers that found the idle array of their capacity full, newest first; see
   * the class comment. Each counts against the allocator's recycle bound while it is here.
   */
  private final SpareList[] spareRegions;
  // Volatile publishes the test callback reference; it does not protect mutable callback state.
  @SuppressWarnings("java:S3077")
  private volatile @Nullable Runnable sparePublicationHook;

  private static final class SpareList {
    private @Nullable ScratchCarrier first;
    private @Nullable ScratchCarrier last;
    private volatile int size;

    synchronized void addFirst(final ScratchCarrier carrier) {
      carrier.spareNext = first;
      carrier.sparePrevious = null;
      if (first == null) {
        last = carrier;
      } else {
        first.sparePrevious = carrier;
      }
      first = carrier;
      size++;
    }

    @Nullable
    ScratchCarrier poll(final boolean newest) {
      if (size == 0) {
        return null;
      }
      synchronized (this) {
        final ScratchCarrier carrier = newest
            ? first
            : last;
        if (carrier == null) {
          return null;
        }
        final ScratchCarrier previous = carrier.sparePrevious;
        final ScratchCarrier next = carrier.spareNext;
        if (previous == null) {
          first = next;
        } else {
          previous.spareNext = next;
        }
        if (next == null) {
          last = previous;
        } else {
          next.sparePrevious = previous;
        }
        carrier.sparePrevious = null;
        carrier.spareNext = null;
        size--;
        return carrier;
      }
    }
  }

  private final Object registryLock = new Object();

  // Guarded by registryLock: the list of live buffers (lent or idle), the counters of buffers that no
  // longer exist, folded in when a buffer is destroyed so that a snapshot never loses or
  // double-counts a buffer's history, and the numbers of buffers created and destroyed per capacity.
  private @Nullable ScratchCarrier liveHead;
  private final long[] retiredAcquisitions = new long[CLASS_COUNT + 1];
  private final long[] retiredAcquisitionsByUse = new long[ScratchUse.COUNT];
  private final long[] retiredRequestedBytesByUse = new long[ScratchUse.COUNT];
  private long retiredRequestedBytes;
  private long retiredRoundedBytes;
  private final long[] created = new long[CLASS_COUNT + 1];
  private final long[] reclaimed = new long[CLASS_COUNT + 1];

  private final AtomicLong refusals = new AtomicLong();
  private final AtomicLong refusedBytes = new AtomicLong();
  private final AtomicLong staleReleases = new AtomicLong();

  /**
   * Bytes of mapped buffers, including mappings in flight; shutdown requires none after idle
   * reclamation.
   */
  private final AtomicLong regionBytes = new AtomicLong();

  /** Lent bytes accounted per borrow: large buffers and grant-backed buffers. */
  private final AtomicLong lentRegionBytes = new AtomicLong();

  /**
   * @param allocator the allocator these buffers belong to
   * @param idleBuffers idle buffers to keep per capacity; rounded up to a power of two
   */
  FrameScratch(final FrameSlotAllocator allocator, final int idleBuffers) {
    if (idleBuffers < 1 || idleBuffers > 256) {
      throw new IllegalArgumentException("idle scratch buffers per capacity must be within [1, 256]: " + idleBuffers);
    }
    this.allocator = allocator;
    this.memory = FrameSlotAllocator.virtualMemory();
    final int slots = Integer.highestOneBit(idleBuffers) == idleBuffers
        ? idleBuffers
        : Integer.highestOneBit(idleBuffers) << 1;
    this.idleMask = slots - 1;
    this.idle = new ScratchCarrier[CLASS_COUNT][slots << IDLE_STRIDE_SHIFT];
    final SpareList[] spares = new SpareList[CLASS_COUNT];
    for (int capacityClass = 0; capacityClass < CLASS_COUNT; capacityClass++) {
      spares[capacityClass] = new SpareList();
    }
    this.spareRegions = spares;
  }

  /** The configured number of idle buffers per capacity. */
  static int defaultIdleBuffers() {
    final int processors = Runtime.getRuntime().availableProcessors();
    final int byDefault = Math.max(8, Math.min(64, 2 * processors));
    return Math.max(1, Math.min(256, Integer.getInteger(IDLE_BUFFERS_PROPERTY, byDefault)));
  }

  /** Test seam for {@link #poisonOnReturn}; returns the previous setting. */
  static boolean setPoisonOnReturnForTesting(final boolean poison) {
    final boolean previous = poisonOnReturn;
    poisonOnReturn = poison;
    return previous;
  }

  /**
   * The capacity class serving a borrow of {@code bytes}: the smallest capacity that fits, or
   * {@link #EXACT_CLASS}.
   */
  static int classForSize(final long bytes) {
    if (bytes > LARGEST_CAPACITY) {
      return EXACT_CLASS;
    }
    // ceil(log2(bytes)), at least the smallest capacity's.
    final int shift = Long.SIZE - Long.numberOfLeadingZeros(bytes - 1L);
    return Math.max(shift, SMALLEST_SHIFT) - SMALLEST_SHIFT;
  }

  /** Bytes of capacity class {@code capacityClass}; zero for {@link #EXACT_CLASS}. */
  static long capacityOf(final int capacityClass) {
    if (capacityClass < SMALL_CLASSES) {
      return FrameSlotAllocator.SIZE_CLASSES[capacityClass];
    }
    return capacityClass < CLASS_COUNT
        ? REGION_CAPACITIES[capacityClass - SMALL_CLASSES]
        : 0L;
  }

  private int home() {
    // Fibonacci hashing of the thread id: neighbouring ids land on distant slots.
    return (int) ((Thread.currentThread().threadId() * 0x9E3779B97F4A7C15L) >>> 32) & idleMask;
  }

  @Override
  @Nullable
  ScratchCarrier acquire(final long minBytes, final ScratchUse use, final boolean mandatory) {
    return acquireInternal(minBytes, use, mandatory, null);
  }

  @Override
  long aggregationCapacity(final long minBytes) {
    if (minBytes <= 0) {
      throw new IllegalArgumentException("invalid aggregation region size");
    }
    final int capacityClass = classForSize(minBytes);
    return capacityClass == EXACT_CLASS
        ? pageRounded(minBytes)
        : capacityOf(capacityClass);
  }

  @Override
  ScratchCarrier acquireAggregation(final long minBytes, final AggregationGrant grant) {
    final long capacity = aggregationCapacity(minBytes);
    grant.claim(capacity);
    try {
      return acquireInternal(minBytes, ScratchUse.AGGREGATION, true, grant);
    } catch (final RuntimeException | Error failure) {
      grant.refund(capacity);
      throw failure;
    }
  }

  private @Nullable ScratchCarrier acquireInternal(final long minBytes, final ScratchUse use, final boolean mandatory,
      final @Nullable AggregationGrant grant) {
    final int capacityClass = classForSize(minBytes);
    if (capacityClass == EXACT_CLASS) {
      return lend(mapRegion(EXACT_CLASS, pageRounded(minBytes), minBytes, mandatory, grant), minBytes, use);
    }
    final ScratchCarrier carrier = takeIdle(capacityClass, grant);
    if (carrier != null) {
      return lend(carrier, minBytes, use);
    }
    return lend(mapRegion(capacityClass, capacityOf(capacityClass), minBytes, mandatory, grant), minBytes, use);
  }

  private @Nullable ScratchCarrier takeIdle(final int capacityClass, final @Nullable AggregationGrant grant) {
    final ScratchCarrier[] slots = idle[capacityClass];
    final int home = home();
    for (int probe = 0; probe <= idleMask; probe++) {
      final int index = ((home + probe) & idleMask) << IDLE_STRIDE_SHIFT;
      final ScratchCarrier carrier = (ScratchCarrier) ScratchHandles.IDLE_SLOT.getAcquire(slots, index);
      if (carrier != null && ScratchHandles.IDLE_SLOT.compareAndSet(slots, index, carrier, null)) {
        if (capacityClass >= SMALL_CLASSES) {
          allocator.unreserveRecycledBytes(carrier.capacity);
        }
        prepareBorrow(carrier, grant);
        return carrier;
      }
    }
    final ScratchCarrier spare = spareRegions[capacityClass].poll(true);
    if (spare != null) {
      allocator.unreserveRecycledBytes(spare.capacity);
      prepareBorrow(spare, grant);
    }
    return spare;
  }

  /**
   * Hand {@code carrier}, held exclusively by this thread, to a borrow; {@code null} passes through.
   */
  private @Nullable ScratchCarrier lend(final @Nullable ScratchCarrier carrier, final long minBytes,
      final ScratchUse use) {
    if (carrier == null) {
      return null;
    }
    if (!small(carrier)) {
      lentRegionBytes.addAndGet(carrier.capacity);
      allocator.adjustActiveBytes(carrier.capacity);
    }
    carrier.lend(minBytes, use);
    return carrier;
  }

  private static long pageRounded(final long bytes) {
    return bytes > Long.MAX_VALUE - PAGE_BYTES
        ? Long.MAX_VALUE
        : (bytes + PAGE_BYTES - 1) & -PAGE_BYTES;
  }

  private static boolean small(final ScratchCarrier carrier) {
    return carrier.capacityClass < SMALL_CLASSES && carrier.aggregationGrant == null;
  }

  private void prepareBorrow(final ScratchCarrier carrier, final @Nullable AggregationGrant grant) {
    if (grant != null) {
      if (carrier.capacityClass < SMALL_CLASSES) {
        allocator.adjustActiveBytes(-carrier.capacity);
      }
      allocator.transferToAggregation(carrier.capacity);
      carrier.aggregationGrant = grant;
    }
  }

  /** A fresh buffer on a mapped region of {@code bytes}, or {@code null} when refused. */
  private @Nullable ScratchCarrier mapRegion(final int capacityClass, final long bytes, final long minBytes,
      final boolean mandatory, final @Nullable AggregationGrant grant) {
    regionBytes.addAndGet(bytes);
    boolean created = false;
    try {
      if (allocator.isTerminated()) {
        throw new IllegalStateException("FrameSlotAllocator has been shut down");
      }
      final boolean reserved;
      try {
        if (grant != null) {
          allocator.commitAggregationBytes(bytes);
          reserved = true;
        } else {
          reserved = allocator.reserveScratchCommittedBytes(bytes, mandatory);
        }
      } catch (final OutOfMemoryError exhausted) {
        refused(minBytes);
        throw exhausted;
      }
      if (!reserved) {
        refused(minBytes);
        return null;
      }
      MemorySegment region = null;
      try {
        region = memory.reserve(bytes);
        memory.commitFresh(region);
        final ScratchCarrier carrier = new ScratchCarrier(this, capacityClass, region);
        carrier.aggregationGrant = grant;
        HftBoundaryTelemetry.nativeAllocation();
        register(carrier);
        if (small(carrier)) {
          allocator.adjustActiveBytes(bytes);
        }
        created = true;
        return carrier;
      } catch (final RuntimeException | Error failure) {
        if (region != null) {
          memory.release(region);
        }
        if (grant == null) {
          allocator.refundCommittedBytes(bytes);
        } else {
          allocator.refundAggregationBytes(bytes);
        }
        throw failure;
      }
    } finally {
      if (!created) {
        regionBytes.addAndGet(-bytes);
      }
    }
  }

  private void refused(final long minBytes) {
    refusals.incrementAndGet();
    refusedBytes.addAndGet(minBytes);
  }

  @Override
  boolean release(final ScratchCarrier carrier, final long era) {
    if (!carrier.takeBack(era)) {
      staleReleases.incrementAndGet();
      LOGGER.warn("Rejected stale or repeated return of a {}-byte scratch buffer (era {})", carrier.capacity, era);
      return false;
    }
    final int capacityClass = carrier.capacityClass;
    if (!small(carrier)) {
      lentRegionBytes.addAndGet(-carrier.capacity);
      allocator.adjustActiveBytes(-carrier.capacity);
    }
    final AggregationGrant grant = carrier.aggregationGrant;
    if (grant != null) {
      carrier.aggregationGrant = null;
      final boolean retained = allocator.transferFromAggregation(carrier.capacity);
      try {
        if (!retained) {
          destroy(carrier, true);
          return true;
        }
      } finally {
        grant.refund(carrier.capacity);
      }
      if (capacityClass < SMALL_CLASSES) {
        allocator.adjustActiveBytes(carrier.capacity);
      }
    }
    try {
      if (poisonOnReturn) {
        carrier.segment.fill((byte) 0x5A);
      }
      if (retain(carrier)) {
        return true;
      }
    } catch (final RuntimeException | Error failure) {
      destroy(carrier);
      throw failure;
    }
    destroy(carrier);
    return true;
  }

  private boolean retain(final ScratchCarrier carrier) {
    final int capacityClass = carrier.capacityClass;
    if (capacityClass == EXACT_CLASS || allocator.isTerminated()) {
      return false;
    }
    final boolean small = capacityClass < SMALL_CLASSES;
    boolean recycled = false;
    try {
      if (!small && !allocator.tryReserveRecycledBytes(carrier.capacity)) {
        return false;
      }
      recycled = !small;
      final ScratchCarrier[] slots = idle[capacityClass];
      final int home = home();
      for (int probe = 0; probe <= idleMask; probe++) {
        final int index = ((home + probe) & idleMask) << IDLE_STRIDE_SHIFT;
        if (ScratchHandles.IDLE_SLOT.getAcquire(slots, index) == null
            && ScratchHandles.IDLE_SLOT.compareAndSet(slots, index, null, carrier)) {
          return true;
        }
      }
      if (small && !allocator.tryReserveRecycledBytes(carrier.capacity)) {
        return false;
      }
      recycled = true;
      final Runnable hook = sparePublicationHook;
      if (hook != null) {
        hook.run();
      }
      spareRegions[capacityClass].addFirst(carrier);
      return true;
    } catch (final RuntimeException | Error failure) {
      if (recycled) {
        allocator.unreserveRecycledBytes(carrier.capacity);
      }
      throw failure;
    }
  }

  void setSparePublicationHookForTesting(final @Nullable Runnable hook) {
    sparePublicationHook = hook;
  }

  /**
   * Return a buffer's backing to the allocator. The caller holds the buffer exclusively and idle.
   */
  private void destroy(final ScratchCarrier carrier) {
    destroy(carrier, false);
  }

  private void destroy(final ScratchCarrier carrier, final boolean aggregation) {
    unregister(carrier);
    memory.release(carrier.segment);
    if (aggregation) {
      allocator.refundAggregationBytes(carrier.capacity);
    } else {
      if (small(carrier)) {
        allocator.adjustActiveBytes(-carrier.capacity);
      }
      allocator.refundCommittedBytes(carrier.capacity);
    }
    regionBytes.addAndGet(-carrier.capacity);
    HftBoundaryTelemetry.nativeRelease();
  }

  private void register(final ScratchCarrier carrier) {
    synchronized (registryLock) {
      final ScratchCarrier head = liveHead;
      carrier.next = head;
      if (head != null) {
        head.previous = carrier;
      }
      liveHead = carrier;
      created[carrier.capacityClass]++;
    }
  }

  private void unregister(final ScratchCarrier carrier) {
    synchronized (registryLock) {
      final ScratchCarrier before = carrier.previous;
      final ScratchCarrier after = carrier.next;
      if (before == null) {
        liveHead = after;
      } else {
        before.next = after;
      }
      if (after != null) {
        after.previous = before;
      }
      carrier.previous = null;
      carrier.next = null;
      final long served = carrier.acquisitionCount();
      retiredAcquisitions[carrier.capacityClass] += served;
      retiredRequestedBytes += carrier.requestedByteCount();
      retiredRoundedBytes += served * carrier.capacity;
      carrier.addUseCountersTo(retiredAcquisitionsByUse, retiredRequestedBytesByUse);
      reclaimed[carrier.capacityClass]++;
    }
  }

  /**
   * Unmap idle region buffers, largest capacity first, until at least {@code wanted} bytes of
   * commitment are returned or none are left: called when a commitment does not fit the budget, when
   * the recycle bound is lowered, and at shutdown. A lent buffer is never touched.
   *
   * @param includeSmallIdle whether to include uncharged small idle-array entries
   * @return the bytes returned to the budget
   */
  long reclaimIdleRegions(final long wanted, final boolean includeSmallIdle) {
    long freed = 0L;
    for (int capacityClass = CLASS_COUNT - 1; capacityClass >= 0 && freed < wanted; capacityClass--) {
      final SpareList spares = spareRegions[capacityClass];
      // The spare stack first, oldest first: the buffers least likely to be borrowed next.
      for (ScratchCarrier spare; freed < wanted && (spare = spares.poll(false)) != null;) {
        allocator.unreserveRecycledBytes(spare.capacity);
        freed += spare.capacity;
        destroy(spare);
      }
      if (!includeSmallIdle && capacityClass < SMALL_CLASSES) {
        continue;
      }
      final ScratchCarrier[] slots = idle[capacityClass];
      for (int slot = 0; slot <= idleMask && freed < wanted; slot++) {
        final int index = slot << IDLE_STRIDE_SHIFT;
        final ScratchCarrier carrier = (ScratchCarrier) ScratchHandles.IDLE_SLOT.getAcquire(slots, index);
        if (carrier != null && ScratchHandles.IDLE_SLOT.compareAndSet(slots, index, carrier, null)) {
          if (capacityClass >= SMALL_CLASSES) {
            allocator.unreserveRecycledBytes(carrier.capacity);
          }
          freed += carrier.capacity;
          destroy(carrier);
        }
      }
    }
    return freed;
  }

  /** Give back every idle buffer; shutdown. */
  void reclaimAllIdle() {
    reclaimIdleRegions(Long.MAX_VALUE, true);
  }

  long idleSmallBytes() {
    long bytes = 0L;
    synchronized (registryLock) {
      for (ScratchCarrier carrier = liveHead; carrier != null; carrier = carrier.next) {
        if (small(carrier) && !carrier.isLent()) {
          bytes += carrier.capacity;
        }
      }
    }
    return bytes;
  }

  /** Bytes of all mapped buffers and mappings in flight. */
  long regionBytes() {
    return regionBytes.get();
  }

  long unchargedIdleSmallBytes() {
    long bytes = 0L;
    for (int capacityClass = 0; capacityClass < SMALL_CLASSES; capacityClass++) {
      final ScratchCarrier[] slots = idle[capacityClass];
      for (int slot = 0; slot <= idleMask; slot++) {
        final int index = slot << IDLE_STRIDE_SHIFT;
        final ScratchCarrier carrier = (ScratchCarrier) ScratchHandles.IDLE_SLOT.getAcquire(slots, index);
        if (carrier != null) {
          bytes += carrier.capacity;
        }
      }
    }
    return bytes;
  }

  /** Lent bytes accounted per borrow. */
  long lentRegionBytes() {
    return lentRegionBytes.get();
  }

  /** Idle buffers kept per capacity. */
  int idleBuffersPerCapacity() {
    return idleMask + 1;
  }

  ScratchStats stats() {
    final int rows = CLASS_COUNT + 1;
    final long[] capacities = new long[rows];
    final long[] acquisitions = new long[rows];
    final long[] createdNow = new long[rows];
    final long[] reclaimedNow = new long[rows];
    final long[] lentBuffers = new long[rows];
    final long[] idleBuffers = new long[rows];
    final long[] acquisitionsByUse = new long[ScratchUse.COUNT];
    final long[] requestedBytesByUse = new long[ScratchUse.COUNT];
    long requestedBytes;
    long roundedBytes;
    long lentBytes = 0L;
    long idleBytes = 0L;
    for (int row = 0; row < rows; row++) {
      capacities[row] = capacityOf(row);
    }
    synchronized (registryLock) {
      System.arraycopy(retiredAcquisitions, 0, acquisitions, 0, rows);
      System.arraycopy(retiredAcquisitionsByUse, 0, acquisitionsByUse, 0, ScratchUse.COUNT);
      System.arraycopy(retiredRequestedBytesByUse, 0, requestedBytesByUse, 0, ScratchUse.COUNT);
      System.arraycopy(created, 0, createdNow, 0, rows);
      System.arraycopy(reclaimed, 0, reclaimedNow, 0, rows);
      requestedBytes = retiredRequestedBytes;
      roundedBytes = retiredRoundedBytes;
      for (ScratchCarrier carrier = liveHead; carrier != null; carrier = carrier.next) {
        final long served = carrier.acquisitionCount();
        acquisitions[carrier.capacityClass] += served;
        requestedBytes += carrier.requestedByteCount();
        roundedBytes += served * carrier.capacity;
        carrier.addUseCountersTo(acquisitionsByUse, requestedBytesByUse);
        if (carrier.isLent()) {
          lentBuffers[carrier.capacityClass]++;
          lentBytes += carrier.capacity;
        } else {
          idleBuffers[carrier.capacityClass]++;
          idleBytes += carrier.capacity;
        }
      }
    }
    return new ScratchStats(capacities, acquisitions, createdNow, reclaimedNow, lentBuffers, idleBuffers,
        requestedBytes, roundedBytes, lentBytes, idleBytes, refusals.get(), refusedBytes.get(), staleReleases.get(),
        acquisitionsByUse, requestedBytesByUse);
  }

  /** One line for the allocator's out-of-memory report. */
  String describe() {
    final ScratchStats stats = stats();
    return "scratch: lent=" + stats.lentBytes() + " bytes in " + stats.totalLentBuffers() + " buffers, idle="
        + stats.idleBytes() + " bytes in " + stats.totalIdleBuffers() + " buffers, refusals=" + stats.refusals() + " ("
        + stats.refusedBytes() + " bytes)";
  }
}
