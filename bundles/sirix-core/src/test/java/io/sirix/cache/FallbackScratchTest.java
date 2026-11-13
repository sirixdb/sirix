/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scratch buffers on an allocator other than the frame-slot allocator (the legacy pool allocators
 * kept for rollback): the same lease, served by that allocator's own budgeted allocation up to its
 * largest segment and by a separately bounded mapped region beyond, with nothing kept between
 * borrows.
 */
final class FallbackScratchTest {

  private static final long KIB = 1024L;
  private static final long MIB = 1024L * 1024L;

  /** An allocator that is not a {@link FrameSlotAllocator} and counts what scratch asks of it. */
  private static final class CountingAllocator implements MemorySegmentAllocator {
    private final FrameSlotAllocator delegate;
    private final long maxBufferSize;
    final AtomicInteger allocations = new AtomicInteger();
    final AtomicInteger releases = new AtomicInteger();

    CountingAllocator(final FrameSlotAllocator delegate, final long maxBufferSize) {
      this.delegate = delegate;
      this.maxBufferSize = maxBufferSize;
    }

    @Override
    public void init(final long maxBufferSize) {
      // initialized by its delegate
    }

    @Override
    public boolean isInitialized() {
      return true;
    }

    @Override
    public void free() {
      delegate.free();
    }

    @Override
    public MemorySegment allocate(final long size) {
      allocations.incrementAndGet();
      return delegate.allocate(size);
    }

    @Override
    public void release(final MemorySegment segment) {
      releases.incrementAndGet();
      delegate.release(segment);
    }

    @Override
    public long getMaxBufferSize() {
      return maxBufferSize;
    }

    @Override
    public void resetSegment(final MemorySegment segment) {
      delegate.resetSegment(segment);
    }
  }

  @Test
  void aggregationAdmissionAndAllocationUseTheSamePageRoundedCapacity() {
    final FrameSlotAllocator delegate = new FrameSlotAllocator(64L << 20);
    final CountingAllocator allocator = new CountingAllocator(delegate, 64L << 20);
    try (AggregationGrant grant = allocator.aggregationBudget().acquire(8L << 20, 8L << 20)) {
      final long[][] cases = {{1L, 4096L}, {4097L, 8192L}, {262_145L, 266_240L}};
      for (final long[] sample : cases) {
        assertEquals(sample[1], grant.allocationCapacity(sample[0]));
        try (ScratchLease lease = grant.borrow(sample[0])) {
          assertEquals(sample[1], lease.capacity());
          assertEquals(sample[1], grant.usedBytes());
        }
        assertEquals(0L, grant.usedBytes());
      }
    } finally {
      delegate.shutdown();
    }
  }

  // The lease's repeated close() is what is asserted; the allocator has shutdown(), not close().
  @SuppressWarnings("java:S2093")
  @Test
  void aBorrowWithinTheLargestSegmentIsTheAllocatorsOwnAllocation() {
    final FrameSlotAllocator frames = new FrameSlotAllocator(64 * MIB);
    final CountingAllocator allocator = new CountingAllocator(frames, 64 * MIB);
    try {
      assertSame(FallbackScratch.of(allocator), ScratchSource.of(allocator), "one source per allocator");
      final ScratchLease lease = new ScratchLease(allocator);
      for (int round = 0; round < 3; round++) {
        lease.acquire(100 * KIB, ScratchUse.READ);
        assertEquals(128 * KIB, lease.capacity(), "the allocator's segment size");
        assertEquals(128 * KIB, frames.getActiveMemoryBytes(), "charged by the allocator like a page frame");
        SegmentAccess.putLong(lease.segment(), 0L, round);
        assertEquals(round, lease.buffer().getLong(0));
        lease.close();
        lease.close();
        assertEquals(0, frames.getActiveMemoryBytes(), "nothing stays resident between borrows");
      }
      assertEquals(3, allocator.allocations.get());
      assertEquals(3, allocator.releases.get(), "a repeated close releases nothing twice");
      assertSame(ScratchStats.EMPTY, allocator.scratchStats(), "this source keeps no counters");
    } finally {
      frames.shutdown();
    }
  }

  @Test
  void aLargerBorrowMapsABoundedRegionOfItsOwn() {
    final FrameSlotAllocator frames = new FrameSlotAllocator(64 * MIB);
    final CountingAllocator allocator = new CountingAllocator(frames, 64 * MIB);
    final FallbackScratch source = FallbackScratch.of(allocator);
    final ScratchLease first = new ScratchLease(allocator);
    final ScratchLease second = new ScratchLease(allocator);
    final ScratchLease third = new ScratchLease(allocator);
    try {
      assertEquals(8 * MIB, source.largeLimitBytes(),
          "mapped scratch uses exactly its declared eighth, even for small budgets");
      first.acquire(5 * MIB + 1, ScratchUse.READ);
      assertEquals(5 * MIB + 4 * KIB, first.capacity(), "whole pages");
      assertEquals(5 * MIB + 4 * KIB, source.mappedBytes());
      SegmentAccess.putLong(first.segment(), 5 * MIB, 5L);
      assertEquals(5L, SegmentAccess.getLong(first.segment(), 5 * MIB));
      assertTrue(second.tryAcquire(2 * MIB, ScratchUse.READ));
      assertFalse(third.tryAcquire(2 * MIB, ScratchUse.READ), "5 MiB + 2 MiB + 2 MiB exceed the 8 MiB bound");
      assertThrows(OutOfMemoryError.class, () -> third.acquire(2 * MIB, ScratchUse.READ),
          "a mandatory borrow beyond the bound fails; it never falls back to unbounded memory");
      assertFalse(third.isHeld());
      assertEquals(7 * MIB + 4 * KIB, source.mappedBytes(), "the refusals left nothing charged");
      first.close();
      assertTrue(third.tryAcquire(2 * MIB, ScratchUse.READ), "a return makes room");
      second.close();
      third.close();
      assertEquals(0, source.mappedBytes());
      assertEquals(0, allocator.allocations.get(), "the allocator has no allocation this large");

      final ScratchCarrier carrier = source.acquire(MIB, ScratchUse.CODEC, true);
      assertNotNull(carrier);
      assertTrue(source.release(carrier, carrier.era));
      assertFalse(source.release(carrier, carrier.era - 1), "a repeated return unmaps nothing twice");
      assertEquals(0, source.mappedBytes());
    } finally {
      first.close();
      second.close();
      third.close();
      frames.shutdown();
    }
  }

  @Test
  void aLiveFallbackGrantKeepsTheSameAdmissionLedgerBeforeItsFirstBorrow() {
    final FrameSlotAllocator frames = new FrameSlotAllocator(64 * MIB);
    final CountingAllocator allocator = new CountingAllocator(frames, 64 * MIB);
    final AggregationBudget budget = allocator.aggregationBudget();
    try (AggregationGrant grant = budget.acquire(budget.capacity(), budget.capacity())) {
      // The source is weakly indexed, so retaining the allocator alone would not keep it alive.
      // A grant must anchor it even with no active carrier yet.
      System.gc();
      assertSame(budget, allocator.aggregationBudget());
      assertEquals(grant.capacity(), allocator.aggregationBudget().grantedBytes());
      try (ScratchLease lease = grant.borrow(4097)) {
        assertEquals(8192, grant.usedBytes());
        SegmentAccess.putLong(lease.segment(), 4096, 29L);
        assertEquals(29L, SegmentAccess.getLong(lease.segment(), 4096));
      }
      assertEquals(0, grant.usedBytes());
    } finally {
      frames.shutdown();
    }
    assertEquals(0, budget.grantedBytes());
  }

  @Test
  void theBoundOnMappedBorrowsGrowsWithTheAllocatorsBudget() {
    final FrameSlotAllocator frames = new FrameSlotAllocator(MIB);
    try {
      assertEquals(128 * MIB, FallbackScratch.of(new CountingAllocator(frames, 1024 * MIB)).largeLimitBytes());
    } finally {
      frames.shutdown();
    }
  }
}
