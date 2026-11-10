/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import com.sun.management.ThreadMXBean;
import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Scratch buffers of the frame-slot allocator: one owner for every transient buffer, rounded to a
 * capacity, charged to the one budget, resident between borrows, never reclaimed while lent, and
 * returned exactly once.
 *
 * <p>
 * Every assertion is on a counter or a gauge the allocator maintains; nothing here measures time.
 * Each test owns its allocator and fixes the number of idle buffers kept per capacity, so no figure
 * depends on the machine's processor count.
 */
final class ScratchLeaseTest {

  private static final long KIB = 1024L;
  private static final long MIB = 1024L * 1024L;

  /** Above the 64 MiB floor below which the allocator reserves no headroom. */
  private static final long BUDGET = 256L * MIB;

  /** Idle buffers kept per capacity in these tests. */
  private static final int IDLE = 4;

  private static final String POISON_LOCK = "sirix.allocator.poisonScratch";

  private static final int SLOT_4K = 0;
  private static final int SLOT_256K = 6;
  private static final int REGION_512K = FrameScratch.SLOT_CLASSES;
  private static final int REGION_1M = FrameScratch.SLOT_CLASSES + 1;
  private static final int REGION_16M = FrameScratch.CLASS_COUNT - 1;

  private final List<FrameSlotAllocator> allocators = new ArrayList<>();

  /** An allocator of this test's own, shut down after it. */
  private FrameSlotAllocator allocator(final long budgetBytes, final int idleBuffers) {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(budgetBytes, idleBuffers);
    allocators.add(allocator);
    return allocator;
  }

  /**
   * Shut the allocators down here and not in a {@code finally} of the test: a shutdown is refused
   * while a buffer is lent, and from a {@code finally} that refusal would replace the assertion that
   * left the buffer lent. Here it is reported beside it, and a test that passes but leaks a lease
   * fails.
   */
  @AfterEach
  void shutDownAllocators() {
    for (final FrameSlotAllocator allocator : allocators) {
      allocator.shutdown();
    }
  }

  @Test
  void aBorrowIsRoundedUpToTheNextCapacity() {
    assertEquals(SLOT_4K, FrameScratch.classForSize(1));
    assertEquals(SLOT_4K, FrameScratch.classForSize(4 * KIB));
    assertEquals(1, FrameScratch.classForSize(4 * KIB + 1));
    assertEquals(5, FrameScratch.classForSize(128 * KIB));
    assertEquals(SLOT_256K, FrameScratch.classForSize(128 * KIB + 1));
    assertEquals(SLOT_256K, FrameScratch.classForSize(256 * KIB));
    assertEquals(REGION_512K, FrameScratch.classForSize(256 * KIB + 1));
    assertEquals(REGION_1M, FrameScratch.classForSize(MIB));
    assertEquals(REGION_16M - 1, FrameScratch.classForSize(8 * MIB));
    assertEquals(REGION_16M, FrameScratch.classForSize(8 * MIB + 4),
        "the default coalesced span plus its last length header needs the 16 MiB capacity");
    assertEquals(REGION_16M, FrameScratch.classForSize(16 * MIB));
    assertEquals(FrameScratch.EXACT_CLASS, FrameScratch.classForSize(16 * MIB + 1));
    assertEquals(FrameScratch.EXACT_CLASS, FrameScratch.classForSize(Long.MAX_VALUE));
    for (int capacityClass = 0; capacityClass < FrameScratch.CLASS_COUNT; capacityClass++) {
      final long capacity = FrameScratch.capacityOf(capacityClass);
      assertEquals(4 * KIB << capacityClass, capacity, "capacities double from 4 KiB");
      assertEquals(capacityClass, FrameScratch.classForSize(capacity));
      assertEquals(capacityClass, FrameScratch.classForSize(capacity - 1));
    }

    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    try (ScratchLease slot = new ScratchLease(allocator).acquire(100, ScratchUse.READ);
        ScratchLease region = new ScratchLease(allocator).acquire(300 * KIB, ScratchUse.READ);
        ScratchLease exact = new ScratchLease(allocator).acquire(16 * MIB + 5, ScratchUse.READ)) {
      assertEquals(4 * KIB, slot.capacity());
      assertEquals(4 * KIB, slot.segment().byteSize());
      assertEquals(512 * KIB, region.capacity());
      assertEquals(16 * MIB + 4 * KIB, exact.capacity(), "beyond the largest capacity: exact, in whole pages");
      assertSame(Arena.global().scope(), slot.segment().scope(), "no per-access liveness check");
      assertSame(Arena.global().scope(), region.segment().scope());
      final ScratchStats stats = allocator.scratchStats();
      assertEquals(SLOT_4K, stats.capacityIndex(100));
      assertEquals(REGION_512K, stats.capacityIndex(300 * KIB));
      assertEquals(FrameScratch.EXACT_CLASS, stats.capacityIndex(16 * MIB + 5));
      assertEquals(100 + 300 * KIB + 16 * MIB + 5, stats.requestedBytes());
      assertEquals(4 * KIB + 512 * KIB + 16 * MIB + 4 * KIB, stats.roundedBytes());
      assertEquals(stats.roundedBytes(), stats.lentBytes());
    }
  }

  @Test
  void aReturnedBufferIsLentAgainWithItsBackingAndItsViews() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final long committedBefore = allocator.getPhysicalMemoryBytes();
    final ScratchLease lease = new ScratchLease(allocator);
    long slotAddress = 0L;
    long regionAddress = 0L;
    ByteBuffer slotBuffer = null;
    ByteBuffer regionBuffer = null;
    for (int round = 0; round < 100; round++) {
      lease.acquire(100 * KIB, ScratchUse.SERIALIZATION);
      if (round == 0) {
        slotAddress = lease.segment().address();
        slotBuffer = lease.buffer();
      }
      assertEquals(slotAddress, lease.segment().address(), "the same slot serves every borrow");
      assertSame(slotBuffer, lease.buffer(), "the buffer view is built once");
      assertEquals(0, lease.buffer().position(), "cleared for each borrow");
      assertEquals(128 * KIB, lease.buffer().limit());
      assertEquals(ByteOrder.LITTLE_ENDIAN, lease.buffer().order());
      lease.buffer().order(ByteOrder.BIG_ENDIAN).position(17).limit(40);
      lease.close();

      lease.acquire(3 * MIB, ScratchUse.READ);
      if (round == 0) {
        regionAddress = lease.segment().address();
        regionBuffer = lease.buffer();
      }
      assertEquals(regionAddress, lease.segment().address(), "the same region serves every borrow");
      assertSame(regionBuffer, lease.buffer());
      lease.close();
    }
    final ScratchStats stats = allocator.scratchStats();
    assertEquals(200, stats.totalAcquisitions());
    assertEquals(2, stats.totalCreated(), "one slot buffer and one region, created once");
    assertEquals(198, stats.totalReused());
    assertEquals(0, stats.totalReclaimed());
    assertEquals(100, stats.acquisitions(ScratchUse.SERIALIZATION));
    assertEquals(100, stats.acquisitions(ScratchUse.READ));
    assertEquals(100 * 100 * KIB, stats.requestedBytes(ScratchUse.SERIALIZATION));
    assertEquals(100 * 3 * MIB, stats.requestedBytes(ScratchUse.READ));
    assertEquals(100 * (128 * KIB + 4 * MIB), stats.roundedBytes());
    assertEquals(0, stats.lentBytes());
    assertEquals(128 * KIB + 4 * MIB, stats.idleBytes(), "both stay resident for the next borrow");
    assertEquals(committedBefore + 128 * KIB + 4 * MIB, allocator.getPhysicalMemoryBytes(),
        "charged once, however often it is borrowed");
    assertEquals(4 * MIB, allocator.getRecycledSlabBytes(), "an idle region counts against the recycle bound");
  }

  @Test
  void aWarmBorrowAllocatesNothing() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final ScratchLease lease = new ScratchLease(allocator);
    final ScratchLease nested = new ScratchLease(allocator);
    long sink = 0L;
    for (int warmup = 0; warmup < 20_000; warmup++) {
      sink += borrowTwice(lease, nested, warmup);
    }
    final ScratchStats before = allocator.scratchStats();
    final long thread = Thread.currentThread().threadId();
    // The least of several batches: a borrow that allocated would allocate in every batch (at least
    // 16 bytes 60,000 times), while the virtual machine's own one-off events on this thread, such as
    // re-creating scalar-replaced objects when it deoptimizes a frame, land in some batch at most.
    long leastAllocated = Long.MAX_VALUE;
    for (int batch = 0; batch < 5; batch++) {
      final long start = bean.getThreadAllocatedBytes(thread);
      for (int round = 0; round < 20_000; round++) {
        sink += borrowTwice(lease, nested, round);
      }
      leastAllocated = Math.min(leastAllocated, bean.getThreadAllocatedBytes(thread) - start);
    }
    final ScratchStats after = allocator.scratchStats();
    assertEquals(5 * 60_000, after.totalAcquisitions() - before.totalAcquisitions());
    assertEquals(0, after.totalCreated() - before.totalCreated(), "every borrow reused a resident buffer");
    assertEquals(0, leastAllocated, "a warm borrow builds no view, no handle and no bookkeeping object");
    assertTrue(sink != 0L);
  }

  /**
   * A slot borrow with a nested one of the same capacity, as a read with a fallback read; a region.
   */
  private static long borrowTwice(final ScratchLease lease, final ScratchLease nested, final int round) {
    lease.acquire(128 * KIB, ScratchUse.READ);
    final ByteBuffer buffer = lease.buffer();
    buffer.putInt(0, round);
    nested.acquire(100 * KIB, ScratchUse.CODEC);
    SegmentAccess.putInt(nested.segment(), 0L, round + 1);
    long sum = buffer.getInt(0) + SegmentAccess.getInt(nested.segment(), 0L);
    nested.close();
    lease.close();
    lease.acquire(MIB, ScratchUse.READ);
    sum += lease.capacity();
    lease.close();
    return sum;
  }

  @Test
  void aBufferIsReturnedExactlyOnce() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final ScratchLease lease = new ScratchLease(allocator).acquire(8 * KIB, ScratchUse.PAGE);
    assertTrue(lease.isHeld());
    assertThrows(IllegalStateException.class, () -> lease.acquire(8 * KIB, ScratchUse.PAGE),
        "a lease holds one buffer");
    lease.close();
    assertFalse(lease.isHeld());
    lease.close();
    lease.close();
    assertThrows(IllegalStateException.class, lease::segment, "a closed lease gives out no view");
    assertThrows(IllegalStateException.class, lease::buffer);
    assertThrows(IllegalStateException.class, lease::capacity);
    ScratchStats stats = allocator.scratchStats();
    assertEquals(1, stats.totalIdleBuffers(), "one buffer, in the idle array once");
    assertEquals(0, stats.staleReleases(), "the lease itself swallows a repeated close");

    // Below the lease: a return that names an era the buffer has left is rejected by the buffer.
    final FrameScratch source = allocator.scratch();
    final ScratchCarrier carrier = source.acquire(8 * KIB, ScratchUse.PAGE, true);
    assertNotNull(carrier);
    final long firstEra = carrier.era;
    assertEquals(1L, firstEra & 1L, "odd while lent");
    assertTrue(source.release(carrier, firstEra));
    assertFalse(source.release(carrier, firstEra), "a repeated return");
    final ScratchCarrier again = source.acquire(8 * KIB, ScratchUse.PAGE, true);
    assertSame(carrier, again, "the resident buffer is lent again");
    assertEquals(firstEra + 2, again.era, "in a new era");
    assertFalse(source.release(carrier, firstEra), "a stale return cannot take the buffer from its new borrower");
    assertFalse(source.release(carrier, firstEra + 1), "nor can an era the buffer was idle in");
    stats = allocator.scratchStats();
    assertEquals(3, stats.staleReleases());
    assertEquals(1, stats.totalLentBuffers(), "the new borrower still holds it");
    assertEquals(0, stats.totalIdleBuffers());
    assertTrue(source.release(again, again.era));
    assertEquals(1, allocator.scratchStats().totalIdleBuffers());
  }

  @Test
  void aBorrowCanBeHandedToAnotherThreadAndRacingClosesReturnItOnce() throws Exception {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      for (int round = 0; round < 200; round++) {
        final ScratchLease lease = new ScratchLease(allocator).acquire(600 * KIB, ScratchUse.IMPORT);
        SegmentAccess.putLong(lease.segment(), 0L, round);
        SegmentAccess.putLong(lease.segment(), lease.capacity() - Long.BYTES, ~round);
        final int expected = round;
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<Long>> closers = new ArrayList<>();
        closers.add(pool.submit(() -> {
          // The consumer: reads what the producer wrote, then closes.
          final long first = SegmentAccess.getLong(lease.segment(), 0L);
          final long last = SegmentAccess.getLong(lease.segment(), lease.capacity() - Long.BYTES);
          assertEquals(expected, first);
          assertEquals(~expected, last);
          start.countDown();
          lease.close();
          return first;
        }));
        for (int closer = 0; closer < 3; closer++) {
          closers.add(pool.submit(() -> {
            start.await();
            lease.close();
            return 0L;
          }));
        }
        for (final Future<Long> closer : closers) {
          closer.get();
        }
        assertFalse(lease.isHeld());
      }
      final ScratchStats stats = allocator.scratchStats();
      assertEquals(200, stats.totalAcquisitions());
      assertEquals(0, stats.staleReleases(), "three of four closes found nothing to return");
      assertEquals(0, stats.totalLentBuffers());
      assertEquals(stats.totalCreated() - stats.totalReclaimed(), stats.totalIdleBuffers());
      assertEquals(stats.totalIdleBuffers() * MIB, stats.idleBytes());
      assertEquals(stats.idleBytes(), allocator.getRecycledSlabBytes());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void scratchSlotsStayOutOfTheFrameCountersAndIdleBuffersAreNotActive() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final MemorySegment frame = allocator.allocate(128 * KIB);
    final int frameClass = FrameSlotAllocator.indexForSize(128 * KIB);
    final long activeBefore = allocator.getActiveMemoryBytes();
    final long committedBefore = allocator.getPhysicalMemoryBytes();
    assertEquals(1, allocator.liveSlotCount(frameClass));
    assertEquals(1, allocator.allocateCount(frameClass));

    final ScratchLease first = new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ);
    final ScratchLease second = new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ);
    assertEquals(activeBefore + 256 * KIB, allocator.getActiveMemoryBytes(), "a lent buffer is caller-owned");
    assertEquals(committedBefore + 256 * KIB, allocator.getPhysicalMemoryBytes());
    first.close();
    second.close();
    assertEquals(activeBefore, allocator.getActiveMemoryBytes(),
        "an idle buffer keeps its slot but is lent to nobody: a leak check sees the baseline");
    assertEquals(committedBefore + 256 * KIB, allocator.getPhysicalMemoryBytes(), "its slot stays committed");
    assertEquals(1, allocator.liveSlotCount(frameClass), "only the page frame is a live frame slot");
    assertEquals(1, allocator.allocateCount(frameClass));
    assertEquals(0, allocator.releaseCount(frameClass));
    assertEquals(0, allocator.recycledSlotCount(frameClass), "the idle buffers still own their slots");
    allocator.release(frame);
    assertEquals(1, allocator.releaseCount(frameClass));
    assertEquals(activeBefore - 128 * KIB, allocator.getActiveMemoryBytes());
  }

  @Test
  void anIdleSlotBufferGivesItsSlotToAPageFrameAndALentOneNever() {
    // After protecting the execution eighth, one 256 KiB slot fits; a second does not.
    final FrameSlotAllocator allocator = allocator(512 * KIB, IDLE);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    try {
      assertEquals(0, allocator.scratchReserveBytes(), "no additional progress reserve for this tiny budget");
      final ScratchLease lease = new ScratchLease(allocator).acquire(256 * KIB, ScratchUse.SERIALIZATION);
      SegmentAccess.putLong(lease.segment(), 0L, 0x5EEDL);
      assertNull(allocator.allocateSlot(256 * KIB), "the budget is committed and the scratch buffer is lent");
      assertEquals(0x5EEDL, SegmentAccess.getLong(lease.segment(), 0L), "a lent buffer is never reclaimed");
      assertEquals(0, allocator.scratchStats().totalReclaimed());

      final long scratchAddress = lease.segment().address();
      lease.close();
      assertEquals(1, allocator.scratchStats().totalIdleBuffers());
      final FrameSlotAllocator.FrameSlot two = allocator.allocateSlot(256 * KIB);
      assertNotNull(two, "the idle buffer's slot serves the page frame");
      assertEquals(scratchAddress, two.segment().address(), "the very slot the scratch buffer held");
      final ScratchStats stats = allocator.scratchStats();
      assertEquals(1, stats.reclaimed()[SLOT_256K]);
      assertEquals(0, stats.totalIdleBuffers());
      assertEquals(0, pressure.get(), "no page was evicted to find that slot");
      assertEquals(256 * KIB, allocator.getPhysicalMemoryBytes(), "the execution floor remains protected");

      assertFalse(new ScratchLease(allocator).tryAcquire(256 * KIB, ScratchUse.READ),
          "an optional borrow is refused while the committed slot holds a frame");
      assertEquals(1, allocator.scratchStats().refusals());
      assertEquals(256 * KIB, allocator.scratchStats().refusedBytes());
      assertEquals(0, pressure.get(), "an optional borrow never asks the caches for relief");
      two.close();
      final ScratchLease reborrowed = new ScratchLease(allocator);
      assertTrue(reborrowed.tryAcquire(256 * KIB, ScratchUse.READ), "a released frame's slot serves scratch again");
      reborrowed.close();
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
    }
  }

  @Test
  void theEndOfTheBudgetIsCommittedByScratchAlone() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    final ScratchLease read = new ScratchLease(allocator);
    final ScratchLease run = new ScratchLease(allocator);
    final ScratchLease tooMuch = new ScratchLease(allocator);
    try {
      final long headroom = allocator.oversizedHeadroomBytes();
      final long reserve = allocator.scratchReserveBytes();
      assertEquals(BUDGET / 16, headroom);
      assertEquals(headroom / 4, reserve, "a quarter of the progress headroom, in addition to the execution floor");
      assertTrue(allocator.reserveSlabCommittedBytes(BUDGET - BUDGET / 8 - headroom),
          "page frames stop at the headroom");
      assertFalse(allocator.reserveSlabCommittedBytes(1));
      assertTrue(allocator.reserveCommittedBytes(headroom - reserve), "slabs and oversized stop at the reserve");
      assertFalse(allocator.reserveCommittedBytes(1));
      assertNull(allocator.allocateSlab(4 * KIB), "a slab is refused inside the scratch reserve");

      // 4 MiB are left, and only scratch may commit them.
      assertTrue(read.tryAcquire(128 * KIB, ScratchUse.READ), "a read still gets its buffer");
      assertTrue(run.tryAcquire(2 * MIB, ScratchUse.READ));
      assertFalse(tooMuch.tryAcquire(2 * MIB, ScratchUse.READ), "2 MiB + 128 KiB of the 4 MiB reserve are taken");
      assertFalse(tooMuch.isHeld());
      assertEquals(1, allocator.scratchStats().refusals());
      assertEquals(2 * MIB, allocator.scratchStats().refusedBytes());
      assertEquals(0, pressure.get(), "optional borrows ask nobody for relief");
      assertTrue(tooMuch.tryAcquire(MIB, ScratchUse.READ), "what still fits is served");
      assertEquals(BUDGET - BUDGET / 8 - (MIB - 128 * KIB), allocator.getPhysicalMemoryBytes(),
          "committed: everything but the 896 KiB of the reserve no borrow asked for");
      read.close();
      run.close();
      tooMuch.close();
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
    }
  }

  @Test
  void aMandatoryBorrowThatNoBudgetHoldsFailsAtOnce() {
    final FrameSlotAllocator allocator = allocator(4 * MIB, IDLE);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    try {
      final ScratchLease lease = new ScratchLease(allocator);
      assertThrows(OutOfMemoryError.class, () -> lease.acquire(8 * MIB, ScratchUse.READ));
      assertFalse(lease.isHeld());
      assertEquals(0, pressure.get(), "evicting pages cannot make an 8 MiB buffer fit a 4 MiB budget");
      assertEquals(1, allocator.scratchStats().refusals());
      assertEquals(0, allocator.getPhysicalMemoryBytes(), "the refusal left nothing charged");
      assertThrows(IllegalArgumentException.class, () -> lease.acquire(0, ScratchUse.READ));
      assertThrows(IllegalArgumentException.class, () -> lease.tryAcquire(-1, ScratchUse.READ));
      assertThrows(NullPointerException.class, () -> lease.acquire(1, null));
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
    }
  }

  @Test
  void idleRegionsShareTheRecycleBoundAndAreUnmappedBeforeACommitmentIsRefused() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    allocator.setSlabRecycleLimitBytesForTesting(3 * MIB);
    final long committedBefore = allocator.getPhysicalMemoryBytes();
    final ScratchLease a = new ScratchLease(allocator).acquire(2 * MIB, ScratchUse.READ);
    final ScratchLease b = new ScratchLease(allocator).acquire(2 * MIB, ScratchUse.READ);
    final ScratchLease c = new ScratchLease(allocator).acquire(MIB, ScratchUse.READ);
    assertEquals(0, allocator.getRecycledSlabBytes(), "a lent region is not recyclable memory");
    assertEquals(committedBefore + 5 * MIB, allocator.getPhysicalMemoryBytes());
    a.close();
    assertEquals(2 * MIB, allocator.getRecycledSlabBytes());
    b.close();
    assertEquals(2 * MIB, allocator.getRecycledSlabBytes(), "the second 2 MiB region does not fit the 3 MiB bound");
    assertEquals(committedBefore + 3 * MIB, allocator.getPhysicalMemoryBytes(), "it was unmapped instead");
    c.close();
    assertEquals(3 * MIB, allocator.getRecycledSlabBytes(), "exactly the bound is kept");
    ScratchStats stats = allocator.scratchStats();
    assertEquals(2, stats.totalIdleBuffers());
    assertEquals(1, stats.totalReclaimed());

    // A slab that only fits once idle regions are unmapped is admitted, largest region first.
    final long room = BUDGET - BUDGET / 8 - allocator.scratchReserveBytes() - allocator.getPhysicalMemoryBytes();
    final MemorySegment slab = allocator.allocateSlab(room + MIB);
    assertNotNull(slab, "idle scratch is given back before a commitment is refused");
    stats = allocator.scratchStats();
    assertEquals(2, stats.reclaimed()[FrameScratch.classForSize(2 * MIB)],
        "the largest idle region went first, after the one the bound had turned away");
    assertEquals(0, stats.reclaimed()[REGION_1M],
        "the request was 1 MiB short: the 1 MiB region stays resident for its next borrower");
    assertEquals(MIB, allocator.getRecycledSlabBytes());
    assertTrue(allocator.releaseSlab(slab));

    allocator.setSlabRecycleLimitBytesForTesting(0);
    assertEquals(0, allocator.scratchStats().totalIdleBuffers(), "lowering the bound drains the idle regions");
    final ScratchLease unpooled = new ScratchLease(allocator).acquire(MIB, ScratchUse.READ);
    final long committedLent = allocator.getPhysicalMemoryBytes();
    unpooled.close();
    assertEquals(committedLent - MIB, allocator.getPhysicalMemoryBytes(), "with the bound at zero a return unmaps");
  }

  @Test
  void regionsBeyondTheIdleArrayStayResidentUpToTheRecycleBound() {
    // Two idle slots per capacity: the population of a reader, not of a grouped scan's workspace.
    final FrameSlotAllocator allocator = allocator(BUDGET, 2);
    allocator.setSlabRecycleLimitBytesForTesting(8 * MIB);
    final long committedBefore = allocator.getPhysicalMemoryBytes();
    final ScratchLease[] leases = new ScratchLease[10];
    final Set<Long> addresses = new HashSet<>();
    for (int i = 0; i < leases.length; i++) {
      leases[i] = new ScratchLease(allocator).acquire(MIB, ScratchUse.READ);
      addresses.add(leases[i].segment().address());
    }
    for (final ScratchLease lease : leases) {
      lease.close();
    }
    ScratchStats stats = allocator.scratchStats();
    assertEquals(8, stats.idleBuffers()[REGION_1M], "the bound in bytes decides, not the length of the idle array");
    assertEquals(2, stats.reclaimed()[REGION_1M], "the two the bound turned away were unmapped");
    assertEquals(8 * MIB, allocator.getRecycledSlabBytes());
    assertEquals(committedBefore + 8 * MIB, allocator.getPhysicalMemoryBytes());

    // The next scan is served by what stayed resident: nothing is mapped.
    final long created = stats.totalCreated();
    for (int i = 0; i < 8; i++) {
      leases[i] = new ScratchLease(allocator).acquire(MIB, ScratchUse.READ);
      assertTrue(addresses.contains(leases[i].segment().address()), "a region the last scan returned");
    }
    stats = allocator.scratchStats();
    assertEquals(created, stats.totalCreated());
    assertEquals(0, stats.idleBuffers()[REGION_1M]);
    assertEquals(0, allocator.getRecycledSlabBytes(), "a lent region is not recyclable memory");
    for (int i = 0; i < 8; i++) {
      leases[i].close();
    }
    assertEquals(8 * MIB, allocator.getRecycledSlabBytes());

    // Spare regions are given back like the idle array's: here, when the bound is lowered.
    allocator.setSlabRecycleLimitBytesForTesting(0);
    assertEquals(0, allocator.scratchStats().totalIdleBuffers());
    assertEquals(committedBefore, allocator.getPhysicalMemoryBytes());
  }

  @Test
  void theIdleBoundPerCapacityIsExact() {
    final FrameSlotAllocator allocator = allocator(BUDGET, 2);
    assertEquals(2, allocator.scratch().idleBuffersPerCapacity());
    final ScratchLease[] leases = new ScratchLease[5];
    for (int i = 0; i < leases.length; i++) {
      leases[i] = new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.CODEC);
    }
    assertEquals(5, allocator.scratchStats().lentBuffers()[SLOT_4K]);
    for (final ScratchLease lease : leases) {
      lease.close();
    }
    final ScratchStats stats = allocator.scratchStats();
    assertEquals(2, stats.idleBuffers()[SLOT_4K], "two stay resident");
    assertEquals(3, stats.reclaimed()[SLOT_4K], "the others gave their slots back");
    assertEquals(3, allocator.recycledSlotCount(SLOT_4K), "to the class, for the next frame or borrow");
    assertEquals(5 * 4 * KIB, allocator.getPhysicalMemoryBytes(), "slot commitment is permanent");
    assertEquals(4, allocator(MIB, 3).scratch().idleBuffersPerCapacity(), "the idle arrays are a power of two long");
    assertThrows(IllegalArgumentException.class, () -> new FrameSlotAllocator(MIB, -1));
  }

  @Test
  void growKeepsThePrefixAndReturnsTheOldBuffer() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final ScratchLease lease = new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.SERIALIZATION);
    for (int i = 0; i < 4 * KIB; i += Long.BYTES) {
      SegmentAccess.putLong(lease.segment(), i, 0xA5A5_0000L + i);
    }
    final MemorySegment small = lease.segment();
    assertFalse(lease.grow(4 * KIB, 4 * KIB), "large enough already");
    assertSame(small, lease.segment());
    assertTrue(lease.grow(MIB + 1, 3 * KIB));
    assertEquals(2 * MIB, lease.capacity());
    for (int i = 0; i < 3 * KIB; i += Long.BYTES) {
      assertEquals(0xA5A5_0000L + i, SegmentAccess.getLong(lease.segment(), i));
    }
    ScratchStats stats = allocator.scratchStats();
    assertEquals(1, stats.idleBuffers()[SLOT_4K], "the old buffer went back");
    assertEquals(1, stats.lentBuffers()[FrameScratch.classForSize(2 * MIB)]);
    assertEquals(2, stats.acquisitions(ScratchUse.SERIALIZATION), "the replacement serves the same use");
    assertThrows(IllegalArgumentException.class, () -> lease.grow(8 * MIB, 2 * MIB + 1));
    assertThrows(IllegalArgumentException.class, () -> lease.grow(8 * MIB, -1));
    lease.close();
    assertThrows(IllegalStateException.class, () -> lease.grow(8 * MIB, 0));
    stats = allocator.scratchStats();
    assertEquals(0, stats.totalLentBuffers());
    assertEquals(0, stats.staleReleases());
  }

  @Test
  @ResourceLock(POISON_LOCK)
  void aBufferIsNotZeroedAndTheTestSeamPoisonsAReturnedOne() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final boolean previous = FrameScratch.setPoisonOnReturnForTesting(false);
    try {
      final ScratchLease lease = new ScratchLease(allocator).acquire(64 * KIB, ScratchUse.CODEC);
      SegmentAccess.putLong(lease.segment(), 0L, 0x1234_5678L);
      final MemorySegment stale = lease.segment();
      lease.close();
      lease.acquire(64 * KIB, ScratchUse.CODEC);
      assertEquals(0x1234_5678L, SegmentAccess.getLong(lease.segment(), 0L),
          "a borrow sees what the last borrower left: nothing zeroes scratch");
      FrameScratch.setPoisonOnReturnForTesting(true);
      lease.close();
      assertEquals(0x5A5A_5A5A_5A5A_5A5AL, SegmentAccess.getLong(stale, 0L),
          "under the seam a view kept past the close reads poison");
      lease.acquire(64 * KIB, ScratchUse.CODEC);
      assertEquals(0x5A5A_5A5A_5A5A_5A5AL, SegmentAccess.getLong(lease.segment(), 64 * KIB - Long.BYTES),
          "and a borrower that assumes zeros fails");
      lease.close();
    } finally {
      FrameScratch.setPoisonOnReturnForTesting(previous);
    }
  }

  @Test
  void concurrentBorrowersNeverShareABuffer() throws Exception {
    final int workers = 8;
    final int rounds = 5_000;
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<?>> futures = new ArrayList<>();
      for (int worker = 0; worker < workers; worker++) {
        final long mark = 0x1000_0000L * (worker + 1);
        futures.add(pool.submit(() -> {
          start.await();
          final ScratchLease lease = new ScratchLease(allocator);
          final ScratchLease nested = new ScratchLease(allocator);
          for (int round = 0; round < rounds; round++) {
            // Three capacities per round, so borrowers meet in the same idle arrays.
            final long bytes = round % 3 == 0
                ? 16 * KIB
                : round % 3 == 1
                    ? 200 * KIB
                    : 700 * KIB;
            lease.acquire(bytes, ScratchUse.READ);
            final MemorySegment segment = lease.segment();
            SegmentAccess.putLong(segment, 0L, mark + round);
            SegmentAccess.putLong(segment, lease.capacity() - Long.BYTES, mark - round);
            nested.acquire(16 * KIB, ScratchUse.CODEC);
            SegmentAccess.putLong(nested.segment(), 0L, -mark);
            Thread.onSpinWait();
            assertEquals(mark + round, SegmentAccess.getLong(segment, 0L), "another borrower wrote this buffer");
            assertEquals(mark - round, SegmentAccess.getLong(segment, lease.capacity() - Long.BYTES));
            assertEquals(-mark, SegmentAccess.getLong(nested.segment(), 0L));
            nested.close();
            lease.close();
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> future : futures) {
        future.get();
      }
      final ScratchStats stats = allocator.scratchStats();
      assertEquals(2L * workers * rounds, stats.totalAcquisitions());
      assertEquals(0, stats.totalLentBuffers());
      assertEquals(0, stats.lentBytes());
      assertEquals(0, stats.staleReleases());
      assertEquals(0, stats.refusals());
      assertEquals(stats.totalCreated() - stats.totalReclaimed(), stats.totalIdleBuffers(),
          "every buffer is either resident or given back, none lost");
      assertTrue(stats.totalIdleBuffers() - stats.idleBuffers()[REGION_1M] <= 2L * IDLE,
          "the idle arrays bounded the slot buffers under contention");
      assertTrue(stats.idleBuffers()[REGION_1M] <= workers,
          "regions are bounded in bytes: no more stay than were ever lent at once");
      assertEquals(0, allocator.getActiveMemoryBytes(), "nothing is lent");
      assertEquals(stats.idleBuffers()[REGION_1M] * MIB, allocator.getRecycledSlabBytes());
      assertEquals(0, allocator.liveSlotCount(FrameSlotAllocator.indexForSize(16 * KIB)));
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void shutdownGivesIdleBuffersBackAndIsRefusedWhileOneIsLent() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final ScratchLease slot = new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.PAGE);
    final ScratchLease region = new ScratchLease(allocator).acquire(MIB, ScratchUse.READ);
    new ScratchLease(allocator).acquire(64 * KIB, ScratchUse.PAGE).close();
    assertThrows(IllegalStateException.class, allocator::shutdown, "a lent slot buffer blocks shutdown");
    slot.close();
    assertThrows(IllegalStateException.class, allocator::shutdown, "a lent region blocks shutdown");
    SegmentAccess.putLong(region.segment(), 0L, 7L);
    assertEquals(7L, SegmentAccess.getLong(region.segment(), 0L), "and is still mapped after the refusal");
    region.close();
    allocator.shutdown();
    assertEquals(0, allocator.getPhysicalMemoryBytes(), "idle slots and regions were all given back");
    assertEquals(0, allocator.getRecycledSlabBytes());
    final ScratchStats stats = allocator.scratchStats();
    assertEquals(0, stats.totalIdleBuffers());
    assertEquals(stats.totalCreated(), stats.totalReclaimed());
    assertThrows(IllegalStateException.class, () -> new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.PAGE));
  }

  @Test
  void anExactlySizedBufferIsUnmappedWhenItIsReturned() {
    final FrameSlotAllocator allocator = allocator(BUDGET, IDLE);
    final long committedBefore = allocator.getPhysicalMemoryBytes();
    final long activeBefore = allocator.getActiveMemoryBytes();
    final ScratchLease lease = new ScratchLease(allocator).acquire(20 * MIB + 1, ScratchUse.READ);
    assertEquals(20 * MIB + 4 * KIB, lease.capacity());
    assertEquals(committedBefore + 20 * MIB + 4 * KIB, allocator.getPhysicalMemoryBytes());
    assertEquals(activeBefore + 20 * MIB + 4 * KIB, allocator.getActiveMemoryBytes());
    SegmentAccess.putLong(lease.segment(), 20 * MIB, 99L);
    assertEquals(99L, lease.buffer().getLong((int) (20 * MIB)));
    lease.close();
    assertEquals(committedBefore, allocator.getPhysicalMemoryBytes());
    assertEquals(activeBefore, allocator.getActiveMemoryBytes());
    assertEquals(0, allocator.getRecycledSlabBytes(), "never kept idle");
    final ScratchStats stats = allocator.scratchStats();
    assertEquals(1, stats.created()[FrameScratch.EXACT_CLASS]);
    assertEquals(1, stats.reclaimed()[FrameScratch.EXACT_CLASS]);
    assertEquals(0, stats.capacityBytes()[FrameScratch.EXACT_CLASS]);
  }
}
