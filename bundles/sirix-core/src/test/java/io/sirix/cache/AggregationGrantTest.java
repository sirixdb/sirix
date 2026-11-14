/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CancellationException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AggregationGrantTest {
  @Test
  void allocationCapacityMatchesEveryRoundedRegionActuallyBorrowed() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
    try (AggregationGrant grant = allocator.aggregationBudget().acquire(8L << 20, 8L << 20)) {
      final long[][] cases = {{1L, 4096L}, {4097L, 8192L}, {262_145L, 524_288L}};
      for (final long[] sample : cases) {
        assertEquals(sample[1], grant.allocationCapacity(sample[0]));
        try (ScratchLease lease = grant.borrow(sample[0])) {
          assertEquals(sample[1], lease.capacity());
          assertEquals(sample[1], grant.usedBytes());
        }
        assertEquals(0L, grant.usedBytes());
      }
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void shutdownRespectsGrantsBeforeTheirFirstMappingAndRejectsStaleAdmission() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    final AggregationBudget budget = allocator.aggregationBudget();
    try (AggregationGrant grant = budget.acquire(4096, 4096)) {
      assertThrows(IllegalStateException.class, allocator::shutdown);
      try (ScratchLease region = grant.borrow(4096)) {
        assertEquals(4096, region.capacity());
      }
    }
    try (AggregationGrant incremental = budget.incremental()) {
      allocator.shutdown();
      assertThrows(IllegalStateException.class, () -> budget.acquire(4096, 4096));
      assertThrows(IllegalStateException.class, budget::incremental);
      assertThrows(IllegalStateException.class, () -> incremental.borrow(4096));
      assertEquals(0L, budget.grantedBytes());
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void historicalPageClassesAndWarmSlabsStillLeaveTheExecutionFloor() {
    final long bytes = 16L << 20;
    final FrameSlotAllocator allocator = new FrameSlotAllocator(bytes);
    final List<MemorySegment> pages = new ArrayList<>();
    MemorySegment cached = null;
    try {
      for (final long size : MemorySegmentAllocator.SEGMENT_SIZES) {
        pages.add(allocator.allocate(size));
      }
      pages.forEach(allocator::release);
      pages.clear();
      final long historical = allocator.getPhysicalMemoryBytes();
      assertTrue(historical > 0L, "returned page slots remain physically committed");
      final long cacheLimit = bytes - bytes / AggregationBudget.DIVISOR - allocator.scratchReserveBytes();
      try (ScratchLease progress = new ScratchLease(allocator).acquire(256L << 10, ScratchUse.READ)) {
        cached = allocator.allocateSlab(cacheLimit - historical - progress.capacity());
        assertNotNull(cached);
        assertEquals(cacheLimit, allocator.nonExecutionCommittedBytes());
        assertNull(allocator.allocateSlab(4096));
        try (AggregationGrant grant = allocator.aggregationBudget().acquire(bytes / 8, bytes / 8);
            ScratchLease execution = grant.borrow(bytes / 8)) {
          execution.segment().fill((byte) 0x17);
          progress.segment().fill((byte) 0x29);
          assertTrue(allocator.getPhysicalMemoryBytes() <= bytes);
          assertEquals(bytes / 8, grant.usedBytes());
        }
      }
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
    } finally {
      pages.forEach(allocator::release);
      if (cached != null) {
        allocator.releaseSlab(cached);
      }
      allocator.shutdown();
    }
  }

  @Test
  void fullCacheCannotConsumeExecutionOrTheProgressReserve() {
    final long budget = 64L << 20;
    final FrameSlotAllocator allocator = new FrameSlotAllocator(budget);
    final long execution = budget / AggregationBudget.DIVISOR;
    final long cacheLimit = budget - execution - allocator.scratchReserveBytes();
    final MemorySegment cached = allocator.allocateSlab(cacheLimit);
    assertNotNull(cached);
    assertNull(allocator.allocateSlab(4096));
    final List<ScratchLease> regions = new ArrayList<>();
    try (AggregationGrant grant = allocator.aggregationBudget().acquire(execution, execution)) {
      for (long used = 0; used < execution; used += 128L << 10) {
        regions.add(grant.borrow(128L << 10));
      }
      assertEquals(execution, grant.usedBytes());
      assertEquals(cacheLimit, allocator.nonExecutionCommittedBytes());
      assertThrows(OutOfMemoryError.class, () -> grant.borrow(4096));
      assertThrows(IllegalStateException.class, grant::close);
      // The execution floor never includes the final mandatory read/codec capacity.
      try (ScratchLease progress = new ScratchLease(allocator).acquire(512L << 10, ScratchUse.READ)) {
        assertTrue(allocator.getPhysicalMemoryBytes() <= budget);
        progress.segment().fill((byte) 0x19);
      }
      regions.forEach(ScratchLease::close);
      regions.clear();
      assertEquals(0, grant.usedBytes());
    } finally {
      regions.forEach(ScratchLease::close);
      allocator.releaseSlab(cached);
      allocator.shutdown();
    }
  }

  @Test
  void grantsAreExclusiveAndTheNextScanReceivesReturnedCapacity() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    final AggregationBudget budget = allocator.aggregationBudget();
    final AggregationGrant first = budget.acquire(budget.capacity(), budget.capacity());
    final CountDownLatch starting = new CountDownLatch(1);
    final FutureTask<Long> second = new FutureTask<>(() -> {
      starting.countDown();
      try (AggregationGrant grant = budget.acquire(budget.capacity(), budget.capacity());
          var _ = grant.borrow(64L << 10)) {
        assertTrue(budget.grantedBytes() <= budget.capacity());
        return grant.capacity();
      }
    });
    final Thread waiter = new Thread(second, "aggregation-grant-test");
    try {
      waiter.start();
      assertTrue(starting.await(10, TimeUnit.SECONDS));
      assertEquals(budget.capacity(), budget.grantedBytes());
      first.close();
      assertEquals(budget.capacity(), second.get(10, TimeUnit.SECONDS));
      assertEquals(0L, budget.grantedBytes());
    } finally {
      first.close();
      waiter.interrupt();
      waiter.join();
      allocator.shutdown();
    }
  }

  @Test
  void interruptedAdmissionCannotRetainCapacity() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    final AggregationBudget budget = allocator.aggregationBudget();
    final CountDownLatch starting = new CountDownLatch(1);
    final FutureTask<Boolean> second = new FutureTask<>(() -> {
      starting.countDown();
      try (var _ = budget.acquire(budget.capacity(), budget.capacity())) {
        throw new AssertionError("the first scan still owns all capacity");
      } catch (final CancellationException _) {
        return Thread.currentThread().isInterrupted();
      }
    });
    final Thread waiter = new Thread(second, "interrupted-aggregation-grant-test");
    try (AggregationGrant first = budget.acquire(budget.capacity(), budget.capacity())) {
      waiter.start();
      assertTrue(starting.await(10, TimeUnit.SECONDS));
      waiter.interrupt();
      assertTrue(second.get(10, TimeUnit.SECONDS));
      assertEquals(first.capacity(), budget.grantedBytes());
    } finally {
      waiter.interrupt();
      waiter.join();
      allocator.shutdown();
    }
    assertEquals(0L, budget.grantedBytes());
  }

  @Test
  void roundedCapacityAndGrowthPeakAreChargedBeforeBorrowing() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    final AggregationBudget budget = allocator.aggregationBudget();
    try (AggregationGrant grant = budget.acquire(8192, 8192); ScratchLease lease = grant.borrow(1)) {
      assertEquals(4096, grant.usedBytes());
      assertThrows(OutOfMemoryError.class, () -> lease.grow(4097, 1));
      assertEquals(4096, lease.capacity());
      assertEquals(4096, grant.usedBytes(), "a refused growth retains only the original region");
      final long beyondCapacity = budget.capacity() + 1;
      assertThrows(OutOfMemoryError.class, () -> budget.acquire(beyondCapacity, beyondCapacity));
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void aReusedMappedBufferMovesBetweenTheSameTwoChargesExactlyOnce() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(8L << 20);
    final AggregationBudget budget = allocator.aggregationBudget();
    long address;
    try (AggregationGrant grant = budget.acquire(64L << 10, 64L << 10)) {
      try (ScratchLease lease = grant.borrow(64L << 10)) {
        address = lease.segment().address();
        assertEquals(0, allocator.nonExecutionCommittedBytes());
        assertEquals(64L << 10, allocator.getPhysicalMemoryBytes());
      }
      assertEquals(64L << 10, allocator.nonExecutionCommittedBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes(), "an idle mapped small buffer is not a frame slot");
      try (ScratchLease lease = grant.borrow(64L << 10)) {
        assertEquals(address, lease.segment().address());
        assertEquals(0, allocator.nonExecutionCommittedBytes());
        assertEquals(64L << 10, allocator.getPhysicalMemoryBytes());
        lease.close();
        lease.close();
      }
      assertEquals(0, grant.usedBytes());
      assertEquals(64L << 10, allocator.nonExecutionCommittedBytes());
    } finally {
      allocator.shutdown();
    }
  }
}
