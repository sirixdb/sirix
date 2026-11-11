/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.HftBoundaryTelemetry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The scratch buffers of a {@link FrameSlotAllocator}: every transient buffer of the engine (read
 * staging, serialization, codec and page staging, import and aggregation chunks) is borrowed here
 * through a {@link ScratchLease}, so the allocator is the only owner of reusable native memory and
 * its one physical budget covers all of it.
 *
 * <h2>Capacities</h2>
 *
 * A borrow is rounded up to the next capacity, selected in constant time with no search:
 *
 * <ul>
 * <li>the allocator's seven frame-slot classes, 4 KiB to 256 KiB: the buffer is a frame slot, taken
 * and returned through the same primitive slot path as a page frame;</li>
 * <li>{@link #REGION_CAPACITIES}, 512 KiB to 16 MiB: the buffer is a mapped region of its own. The
 * largest matters: a coalesced read spans up to 8 MiB between page offsets <em>plus</em> the four
 * length bytes of its last page, which no 8 MiB buffer holds;</li>
 * <li>anything larger is an exactly sized, page-rounded region that is unmapped when it is
 * returned.</li>
 * </ul>
 *
 * <h2>Reuse</h2>
 *
 * A returned buffer stays resident in the idle array of its capacity and is lent again as it is:
 * its pages stay committed, its views are kept, and a warm borrow costs one compare-and-set out of
 * the array and one back in. The arrays are small and striped by thread, so a thread normally finds
 * the buffer it returned last. Nothing is zeroed.
 *
 * <p>
 * Idle buffers are the allocator's, bounded and given back before anything else is refused: an idle
 * slot buffer returns its slot when a page frame of its class finds none, an idle region is
 * unmapped when a commitment does not fit the budget and counts against the allocator's recycle
 * bound together with the recycled cache slabs. A <em>lent</em> buffer is never reclaimed, evicted
 * or moved; only its lease returns it.
 *
 * <p>
 * The idle arrays hold as many buffers per capacity as threads are likely to hold at once, which is
 * the right population for a reader's or a serializer's buffer and far too few for a grouped scan:
 * its workspace borrows regions by the hundred and returns them together. A region that finds its
 * idle array full therefore stays resident on a spare stack of its capacity, bounded like every
 * idle region by the recycle bound in bytes and by nothing else, and is lent again before a new
 * region is mapped. Without it the next scan would map, and fault in page by page, nearly
 * everything the last one returned. Slot buffers have no spare stack: an idle one holds a frame
 * slot of its class.
 *
 * <h2>Budget</h2>
 *
 * Scratch is charged to the allocator's physical budget like everything else, and it alone may
 * commit the last {@link FrameSlotAllocator#scratchReserveBytes()} of it, so a reader or a
 * serializer makes progress when page frames and cache slabs have taken the rest. A mandatory
 * borrow beyond that asks the page caches for relief and waits like a page frame; an optional one
 * is refused at once and the caller takes its cheaper route.
 *
 * <h2>Cost</h2>
 *
 * The borrow path of a slot buffer shares no counter: the statistics live on the buffer itself,
 * written by its one holder, and are summed on demand. A region buffer (a read of half a megabyte
 * or more) also moves the recycle bound and the active-byte gauge.
 */
final class FrameScratch extends ScratchSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(FrameScratch.class);

  /** Capacities above the largest frame-slot class, each backed by a mapped region of its own. */
  static final long[] REGION_CAPACITIES = {512L << 10, 1L << 20, 2L << 20, 4L << 20, 8L << 20, 16L << 20};

  /** Capacities backed by a frame slot: the allocator's size classes. */
  static final int SLOT_CLASSES = FrameSlotAllocator.SIZE_CLASSES.length;

  /** Reusable capacities: slot classes, then {@link #REGION_CAPACITIES}. */
  static final int CLASS_COUNT = SLOT_CLASSES + REGION_CAPACITIES.length;

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
  private final ConcurrentLinkedDeque<ScratchCarrier>[] spareRegions;

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

  /** Bytes of live region-backed buffers, lent or idle: commitment this source holds. */
  private final AtomicLong regionBytes = new AtomicLong();

  /** Bytes of region-backed buffers a lease holds now; a shutdown must find none. */
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    final ConcurrentLinkedDeque<ScratchCarrier>[] spares = new ConcurrentLinkedDeque[CLASS_COUNT];
    for (int capacityClass = 0; capacityClass < CLASS_COUNT; capacityClass++) {
      spares[capacityClass] = new ConcurrentLinkedDeque<>();
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
    if (capacityClass < SLOT_CLASSES) {
      return FrameSlotAllocator.SIZE_CLASSES[capacityClass];
    }
    return capacityClass < CLASS_COUNT
        ? REGION_CAPACITIES[capacityClass - SLOT_CLASSES]
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
    final ScratchCarrier[] slots = idle[capacityClass];
    final int home = home();
    for (int probe = 0; probe <= idleMask; probe++) {
      final int index = ((home + probe) & idleMask) << IDLE_STRIDE_SHIFT;
      final ScratchCarrier carrier = (ScratchCarrier) ScratchHandles.IDLE_SLOT.getAcquire(slots, index);
      if (carrier != null && (grant == null || carrier.slotIndex < 0)
          && ScratchHandles.IDLE_SLOT.compareAndSet(slots, index, carrier, null)) {
        if (carrier.slotIndex < 0) {
          // An idle region counts against the recycle bound; a lent one does not.
          allocator.unreserveRecycledBytes(carrier.capacity);
        }
        if (grant != null) {
          allocator.transferToAggregation(carrier.capacity);
          carrier.aggregationGrant = grant;
        }
        return lend(carrier, minBytes, use);
      }
    }
    final ScratchCarrier spare = spareRegions[capacityClass].pollFirst();
    if (spare != null) {
      allocator.unreserveRecycledBytes(spare.capacity);
      if (grant != null) {
        allocator.transferToAggregation(spare.capacity);
        spare.aggregationGrant = grant;
      }
      return lend(spare, minBytes, use);
    }
    return lend(capacityClass < SLOT_CLASSES && grant == null
        ? takeSlot(capacityClass, minBytes, mandatory)
        : mapRegion(capacityClass, capacityOf(capacityClass), minBytes, mandatory, grant), minBytes, use);
  }

  /**
   * Hand {@code carrier}, held exclusively by this thread, to a borrow; {@code null} passes through.
   */
  private @Nullable ScratchCarrier lend(final @Nullable ScratchCarrier carrier, final long minBytes,
      final ScratchUse use) {
    if (carrier == null) {
      return null;
    }
    if (carrier.slotIndex < 0) {
      // A slot buffer's bytes are active for as long as it owns its slot; a region's only while lent.
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

  /** A fresh buffer on a frame slot of {@code capacityClass}, or {@code null} when refused. */
  private @Nullable ScratchCarrier takeSlot(final int capacityClass, final long minBytes, final boolean mandatory) {
    final int slotIndex;
    try {
      slotIndex = allocator.acquireScratchSlot(capacityClass, minBytes, mandatory);
    } catch (final OutOfMemoryError exhausted) {
      refused(minBytes);
      throw exhausted;
    }
    if (slotIndex < 0) {
      refused(minBytes);
      return null;
    }
    final long slotVersion = allocator.acquireVersion(capacityClass, slotIndex);
    try {
      final ScratchCarrier carrier = new ScratchCarrier(this, capacityClass,
          allocator.slotSegment(capacityClass, slotIndex), slotIndex, slotVersion);
      register(carrier);
      return carrier;
    } catch (final RuntimeException | Error failure) {
      allocator.releaseScratchSlot(capacityClass, slotIndex, slotVersion);
      throw failure;
    }
  }

  /** A fresh buffer on a mapped region of {@code bytes}, or {@code null} when refused. */
  private @Nullable ScratchCarrier mapRegion(final int capacityClass, final long bytes, final long minBytes,
      final boolean mandatory, final @Nullable AggregationGrant grant) {
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
      // Not populated: unlike a slab of decoded lanes, a read buffer is rarely written to its end, so
      // its pages are left to the first touch and only the touched ones become resident.
      memory.commitFresh(region);
      final ScratchCarrier carrier = new ScratchCarrier(this, capacityClass, region, -1, 0L);
      carrier.aggregationGrant = grant;
      HftBoundaryTelemetry.nativeAllocation();
      register(carrier);
      regionBytes.addAndGet(bytes);
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
    if (poisonOnReturn) {
      carrier.segment.fill((byte) 0x5A);
    }
    final int capacityClass = carrier.capacityClass;
    final boolean region = carrier.slotIndex < 0;
    if (region) {
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
    }
    if (capacityClass == EXACT_CLASS || allocator.isTerminated()) {
      destroy(carrier);
      return true;
    }
    if (region && !allocator.tryReserveRecycledBytes(carrier.capacity)) {
      destroy(carrier);
      return true;
    }
    final ScratchCarrier[] slots = idle[capacityClass];
    final int home = home();
    for (int probe = 0; probe <= idleMask; probe++) {
      final int index = ((home + probe) & idleMask) << IDLE_STRIDE_SHIFT;
      if (ScratchHandles.IDLE_SLOT.getAcquire(slots, index) == null
          && ScratchHandles.IDLE_SLOT.compareAndSet(slots, index, null, carrier)) {
        // The carrier is the next borrower's from here on: nothing below may touch it.
        return true;
      }
    }
    if (region) {
      // The idle array is full: stay resident on the spare stack, inside the recycle bound reserved
      // above. The carrier is the next borrower's from here on.
      spareRegions[capacityClass].addFirst(carrier);
      return true;
    }
    destroy(carrier);
    return true;
  }

  /**
   * Return a buffer's backing to the allocator. The caller holds the buffer exclusively and idle.
   */
  private void destroy(final ScratchCarrier carrier) {
    destroy(carrier, false);
  }

  private void destroy(final ScratchCarrier carrier, final boolean aggregation) {
    unregister(carrier);
    if (carrier.slotIndex >= 0) {
      allocator.releaseScratchSlot(carrier.capacityClass, carrier.slotIndex, carrier.slotVersion);
    } else {
      regionBytes.addAndGet(-carrier.capacity);
      memory.release(carrier.segment);
      if (aggregation) {
        allocator.refundAggregationBytes(carrier.capacity);
      } else {
        allocator.refundCommittedBytes(carrier.capacity);
      }
      HftBoundaryTelemetry.nativeRelease();
    }
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
   * Give back the slots of the idle buffers of {@code capacityClass}: called when a page frame of
   * that class finds no slot, and at shutdown. A lent buffer is never touched.
   *
   * @return the number of slots returned
   */
  int reclaimIdleSlots(final int capacityClass) {
    final ScratchCarrier[] slots = idle[capacityClass];
    int returned = 0;
    for (int slot = 0; slot <= idleMask; slot++) {
      final int index = slot << IDLE_STRIDE_SHIFT;
      final ScratchCarrier carrier = (ScratchCarrier) ScratchHandles.IDLE_SLOT.getAcquire(slots, index);
      if (carrier != null && ScratchHandles.IDLE_SLOT.compareAndSet(slots, index, carrier, null)) {
        if (carrier.slotIndex < 0) {
          allocator.unreserveRecycledBytes(carrier.capacity);
        }
        destroy(carrier);
        returned++;
      }
    }
    return returned;
  }

  /**
   * Unmap idle region buffers, largest capacity first, until at least {@code wanted} bytes of
   * commitment are returned or none are left: called when a commitment does not fit the budget, when
   * the recycle bound is lowered, and at shutdown. A lent buffer is never touched.
   *
   * @return the bytes returned to the budget
   */
  long reclaimIdleRegions(final long wanted) {
    long freed = 0L;
    for (int capacityClass = CLASS_COUNT - 1; capacityClass >= 0 && freed < wanted; capacityClass--) {
      final ConcurrentLinkedDeque<ScratchCarrier> spares = spareRegions[capacityClass];
      // The spare stack first, oldest first: the buffers least likely to be borrowed next.
      for (ScratchCarrier spare; freed < wanted && (spare = spares.pollLast()) != null;) {
        allocator.unreserveRecycledBytes(spare.capacity);
        freed += spare.capacity;
        destroy(spare);
      }
      final ScratchCarrier[] slots = idle[capacityClass];
      for (int slot = 0; slot <= idleMask && freed < wanted; slot++) {
        final int index = slot << IDLE_STRIDE_SHIFT;
        final ScratchCarrier carrier = (ScratchCarrier) ScratchHandles.IDLE_SLOT.getAcquire(slots, index);
        if (carrier != null && carrier.slotIndex < 0
            && ScratchHandles.IDLE_SLOT.compareAndSet(slots, index, carrier, null)) {
          allocator.unreserveRecycledBytes(carrier.capacity);
          freed += carrier.capacity;
          destroy(carrier);
        }
      }
    }
    return freed;
  }

  /** Give back every idle buffer; shutdown. */
  void reclaimAllIdle() {
    reclaimIdleRegions(Long.MAX_VALUE);
    for (int capacityClass = 0; capacityClass < SLOT_CLASSES; capacityClass++) {
      reclaimIdleSlots(capacityClass);
    }
  }

  /** Bytes of frame slots held by idle buffers: owned at slot level, lent to nobody. */
  long idleSlotBytes() {
    long bytes = 0L;
    for (int capacityClass = 0; capacityClass < SLOT_CLASSES; capacityClass++) {
      final ScratchCarrier[] slots = idle[capacityClass];
      final long capacity = FrameSlotAllocator.SIZE_CLASSES[capacityClass];
      for (int slot = 0; slot <= idleMask; slot++) {
        final ScratchCarrier carrier =
            (ScratchCarrier) ScratchHandles.IDLE_SLOT.getAcquire(slots, slot << IDLE_STRIDE_SHIFT);
        if (carrier != null && carrier.slotIndex >= 0) {
          bytes += capacity;
        }
      }
    }
    return bytes;
  }

  /** Bytes of live region-backed buffers, lent or idle. */
  long regionBytes() {
    return regionBytes.get();
  }

  /** Bytes of region-backed buffers a lease holds now. */
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
