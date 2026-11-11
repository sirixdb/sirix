/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FrameScratchReclamationTest {
  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;

  private enum Target {
    SMALL, REGION, EXACT, SLAB, OVERSIZED, FRAME
  }

  @Test
  void idleSmallRegionsMakeRoomForALargerMandatoryBorrow() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(MIB, 4);
    final List<ScratchLease> leases = new ArrayList<>(4);
    try {
      for (int buffer = 0; buffer < 3; buffer++) {
        leases.add(new ScratchLease(allocator).acquire(256 * KIB, ScratchUse.READ));
      }
      leases.add(new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ));
      assertEquals(896 * KIB, allocator.getPhysicalMemoryBytes());
      SegmentAccess.putLong(leases.getFirst().segment(), 0L, 59L);
      try (ScratchLease refused = new ScratchLease(allocator)) {
        assertFalse(refused.tryAcquire(4 * KIB, ScratchUse.CODEC));
        assertEquals(0L, allocator.scratchStats().totalReclaimed());
        assertEquals(59L, SegmentAccess.getLong(leases.getFirst().segment(), 0L));
      }
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      assertEquals(0L, allocator.getActiveMemoryBytes());
      assertEquals(0L, allocator.getRecycledSlabBytes());
      try (ScratchLease larger = new ScratchLease(allocator).acquire(512 * KIB, ScratchUse.READ)) {
        assertEquals(512 * KIB, larger.capacity());
        SegmentAccess.putLong(larger.segment(), 512 * KIB - Long.BYTES, 37L);
        assertEquals(37L, SegmentAccess.getLong(larger.segment(), 512 * KIB - Long.BYTES));
        assertTrue(allocator.scratchStats().totalReclaimed() >= 2L);
        assertTrue(allocator.getPhysicalMemoryBytes() <= 896 * KIB);
      }
      reclaimAndAssertBaseline(allocator, 0L);
    } finally {
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      allocator.shutdown();
    }
  }

  @Test
  void smallerIdleRegionsAlsoServeALargerSmallCapacity() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(MIB, 8);
    final List<ScratchLease> leases = new ArrayList<>(7);
    try {
      for (int buffer = 0; buffer < 7; buffer++) {
        leases.add(new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.IMPORT));
      }
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      try (ScratchLease larger = new ScratchLease(allocator).acquire(256 * KIB, ScratchUse.READ)) {
        assertEquals(256 * KIB, larger.capacity());
        assertTrue(allocator.scratchStats().reclaimed()[FrameScratch.classForSize(128 * KIB)] >= 2L);
        assertTrue(allocator.getPhysicalMemoryBytes() <= 896 * KIB);
      }
      reclaimAndAssertBaseline(allocator, 0L);
    } finally {
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      allocator.shutdown();
    }
  }

  @ParameterizedTest
  @EnumSource(Target.class)
  void everyAdmissionFamilyCanReclaimIdleSmallRegions(final Target target) {
    final long budget = 64 * MIB;
    final FrameSlotAllocator allocator = new FrameSlotAllocator(budget, 2);
    final List<ScratchLease> leases = new ArrayList<>(96);
    MemorySegment resident = null;
    try {
      allocator.setSlabRecycleLimitBytesForTesting(budget);
      resident = allocator.allocateSlab(32 * MIB);
      assertNotNull(resident);
      for (int buffer = 0; buffer < 96; buffer++) {
        leases.add(new ScratchLease(allocator).acquire(256 * KIB, ScratchUse.IMPORT));
      }
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      assertEquals(56 * MIB, allocator.getPhysicalMemoryBytes());
      assertEquals(allocator.getSlabBytes(), allocator.getActiveMemoryBytes());
      assertEquals(94 * 256 * KIB, allocator.getRecycledSlabBytes());
      final long frameBytes;
      switch (target) {
        case SMALL, REGION, EXACT -> {
          final long bytes = target == Target.SMALL
              ? 128 * KIB
              : target == Target.REGION
                  ? 16 * MIB
                  : 17 * MIB + 1L;
          try (ScratchLease lease = new ScratchLease(allocator).acquire(bytes, ScratchUse.READ)) {
            assertTrue(lease.capacity() >= bytes);
            SegmentAccess.putLong(lease.segment(), bytes - Long.BYTES, 41L);
            assertEquals(41L, SegmentAccess.getLong(lease.segment(), bytes - Long.BYTES));
          }
          frameBytes = 0L;
        }
        case SLAB -> {
          final MemorySegment slab = allocator.allocateSlab(17 * MIB);
          assertNotNull(slab);
          SegmentAccess.putLong(slab, slab.byteSize() - Long.BYTES, 43L);
          assertEquals(43L, SegmentAccess.getLong(slab, slab.byteSize() - Long.BYTES));
          assertTrue(allocator.releaseSlab(slab));
          frameBytes = 0L;
        }
        case OVERSIZED -> {
          final MemorySegment oversized = allocator.allocate(17 * MIB);
          assertNotNull(oversized);
          SegmentAccess.putLong(oversized, oversized.byteSize() - Long.BYTES, 47L);
          assertEquals(47L, SegmentAccess.getLong(oversized, oversized.byteSize() - Long.BYTES));
          allocator.release(oversized);
          frameBytes = 0L;
        }
        case FRAME -> {
          final FrameSlotAllocator.FrameSlot frame = allocator.allocateSlot(256 * KIB);
          assertNotNull(frame);
          frame.close();
          frameBytes = 256 * KIB;
        }
        default -> throw new AssertionError(target);
      }
      assertTrue(allocator.scratchStats().totalReclaimed() > 0L);
      assertTrue(allocator.getPhysicalMemoryBytes() <= 56 * MIB);
      assertTrue(allocator.releaseSlab(resident));
      resident = null;
      reclaimAndAssertBaseline(allocator, frameBytes);
    } finally {
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      if (resident != null) {
        allocator.releaseSlab(resident);
      }
      allocator.shutdown();
    }
  }

  @Test
  void aLargerIdleRegionCanServeASmallerMandatoryRequestWithoutExtraCommitment() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(MIB, 2);
    try {
      allocator.setSlabRecycleLimitBytesForTesting(MIB);
      new ScratchLease(allocator).acquire(512 * KIB, ScratchUse.READ).close();
      new ScratchLease(allocator).acquire(256 * KIB, ScratchUse.READ).close();
      new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ).close();
      assertEquals(896 * KIB, allocator.getPhysicalMemoryBytes());
      try (ScratchLease smaller = new ScratchLease(allocator).acquire(64 * KIB, ScratchUse.CODEC)) {
        assertTrue(smaller.capacity() >= 64 * KIB);
        SegmentAccess.putLong(smaller.segment(), 64 * KIB - Long.BYTES, 53L);
        assertEquals(53L, SegmentAccess.getLong(smaller.segment(), 64 * KIB - Long.BYTES));
        assertTrue(allocator.getPhysicalMemoryBytes() <= 896 * KIB);
      }
      reclaimAndAssertBaseline(allocator, 0L);
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void loweringTheRecycleLimitSkipsUnchargedLargerIdleArrays() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB, 1);
    try {
      final ScratchLease large = new ScratchLease(allocator).acquire(256 * KIB, ScratchUse.READ);
      final ScratchLease small = new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.READ);
      final ScratchLease spare = new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.CODEC);
      large.close();
      small.close();
      spare.close();
      assertEquals(4 * KIB, allocator.getRecycledSlabBytes());
      assertEquals(264 * KIB, allocator.getPhysicalMemoryBytes());
      allocator.setSlabRecycleLimitBytesForTesting(0L);
      assertEquals(0L, allocator.getRecycledSlabBytes());
      assertEquals(260 * KIB, allocator.getPhysicalMemoryBytes());
      final ScratchStats stats = allocator.scratchStats();
      assertEquals(1L, stats.reclaimed()[FrameScratch.classForSize(4 * KIB)]);
      assertEquals(0L, stats.reclaimed()[FrameScratch.classForSize(256 * KIB)]);
      assertEquals(2L, stats.totalIdleBuffers());
      reclaimAndAssertBaseline(allocator, 0L);
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void aSmallRegionMovesBetweenOrdinaryAndGrantAccountingWithoutExtraCommitment() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(MIB, 1);
    try {
      final long address;
      try (ScratchLease ordinary = new ScratchLease(allocator).acquire(1L, ScratchUse.READ)) {
        address = ordinary.segment().address();
        assertEquals(4 * KIB, allocator.getActiveMemoryBytes());
      }
      assertEquals(0L, allocator.getActiveMemoryBytes());
      try (AggregationGrant grant = allocator.aggregationBudget().acquire(4 * KIB, 4 * KIB)) {
        try (ScratchLease execution = grant.borrow(1L)) {
          assertEquals(address, execution.segment().address());
          assertEquals(4 * KIB, grant.usedBytes());
          assertEquals(4 * KIB, allocator.getActiveMemoryBytes());
          assertEquals(0L, allocator.nonExecutionCommittedBytes());
        }
        assertEquals(0L, grant.usedBytes());
        assertEquals(4 * KIB, allocator.nonExecutionCommittedBytes());
        assertEquals(0L, allocator.getActiveMemoryBytes());
        try (ScratchLease ordinary = new ScratchLease(allocator).acquire(1L, ScratchUse.READ)) {
          assertEquals(address, ordinary.segment().address());
          assertEquals(4 * KIB, allocator.getActiveMemoryBytes());
          assertEquals(4 * KIB, allocator.getPhysicalMemoryBytes());
        }
      }
      reclaimAndAssertBaseline(allocator, 0L);
    } finally {
      allocator.shutdown();
    }
  }

  private static void reclaimAndAssertBaseline(final FrameSlotAllocator allocator, final long frameBytes) {
    allocator.setSlabRecycleLimitBytesForTesting(0L);
    allocator.scratch().reclaimAllIdle();
    final ScratchStats stats = allocator.scratchStats();
    assertEquals(0L, stats.totalLentBuffers());
    assertEquals(0L, stats.totalIdleBuffers());
    assertEquals(stats.totalCreated(), stats.totalReclaimed());
    assertEquals(0L, stats.lentBytes());
    assertEquals(0L, stats.idleBytes());
    assertEquals(0L, allocator.getActiveMemoryBytes());
    assertEquals(frameBytes, allocator.getPhysicalMemoryBytes());
    assertEquals(frameBytes, allocator.nonExecutionCommittedBytes());
    assertEquals(0L, allocator.getRecycledSlabBytes());
    assertEquals(0L, allocator.getSlabBytes());
    assertEquals(0L, allocator.aggregationBudget().grantedBytes());
    for (int capacityClass = 0; capacityClass <= FrameScratch.EXACT_CLASS; capacityClass++) {
      assertEquals(stats.created()[capacityClass], stats.reclaimed()[capacityClass]);
    }
    for (int capacityClass = 0; capacityClass < FrameScratch.SMALL_CLASSES; capacityClass++) {
      assertEquals(0, allocator.liveSlotCount(capacityClass));
    }
  }
}
