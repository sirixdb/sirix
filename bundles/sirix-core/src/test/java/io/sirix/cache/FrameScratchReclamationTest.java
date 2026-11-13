/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.FrameSlotAllocator.FrameSlot;
import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class FrameScratchReclamationTest {
  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;

  private enum Target {
    SMALL, REGION, EXACT, SLAB, OVERSIZED, FRAME
  }

  @ParameterizedTest
  @ValueSource(longs = {128 * 1024L, 512 * 1024L, 17 * 1024 * 1024L + 1L})
  void impossibleScratchAdmissionKeepsIdleMappings(final long requested) {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(32 * MIB, 4);
    MemorySegment resident = null;
    try {
      resident = allocator.allocateSlab(28 * MIB - 4 * KIB);
      assertNotNull(resident);
      SegmentAccess.putLong(resident, 0L, 59L);
      final long address;
      try (ScratchLease scratch = new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.READ)) {
        address = scratch.segment().address();
      }
      final ScratchStats before = allocator.scratchStats();
      try (ScratchLease refused = new ScratchLease(allocator)) {
        assertFalse(refused.tryAcquire(requested, ScratchUse.CODEC));
      }
      assertEquals(28 * MIB, allocator.getPhysicalMemoryBytes());
      assertEquals(before.totalCreated(), allocator.scratchStats().totalCreated());
      assertEquals(before.totalReclaimed(), allocator.scratchStats().totalReclaimed());
      assertEquals(59L, SegmentAccess.getLong(resident, 0L));
      try (ScratchLease scratch = new ScratchLease(allocator).acquire(4 * KIB, ScratchUse.READ)) {
        assertEquals(address, scratch.segment().address());
      }
    } finally {
      if (resident != null) {
        assertTrue(allocator.releaseSlab(resident));
      }
      allocator.shutdown();
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
    assertEquals(0L, allocator.nonExecutionCommittedBytes());
    assertEquals(0L, allocator.getActiveMemoryBytes());
    assertEquals(0L, allocator.getRecycledSlabBytes());
  }

  // The leases fill an array, are returned in borrow order and are borrowed again after that.
  @SuppressWarnings("java:S2093")
  @Test
  void sparesAreBorrowedNewestFirstAndReclaimedOldestFirst() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4 * MIB, 1);
    final ScratchLease[] leases = new ScratchLease[4];
    final long[] addresses = new long[4];
    try {
      for (int buffer = 0; buffer < leases.length; buffer++) {
        leases[buffer] = new ScratchLease(allocator).acquire(64 * KIB, ScratchUse.READ);
        addresses[buffer] = leases[buffer].segment().address();
      }
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      assertEquals(64 * KIB, allocator.scratch().reclaimIdleRegions(64 * KIB, true));
      for (int buffer = 0; buffer < 3; buffer++) {
        leases[buffer].acquire(64 * KIB, ScratchUse.READ);
      }
      assertEquals(addresses[0], leases[0].segment().address());
      assertEquals(addresses[3], leases[1].segment().address());
      assertEquals(addresses[2], leases[2].segment().address());
      assertEquals(4L, allocator.scratchStats().totalCreated());
      assertEquals(1L, allocator.scratchStats().totalReclaimed());
    } finally {
      for (final ScratchLease lease : leases) {
        if (lease != null) {
          lease.close();
        }
      }
      allocator.shutdown();
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
    assertEquals(0L, allocator.nonExecutionCommittedBytes());
    assertEquals(0L, allocator.getRecycledSlabBytes());
  }

  @Test
  void impossibleFrameAdmissionKeepsIdleScratchAndSlabsMapped() {
    final long budget = 64 * MIB;
    final FrameSlotAllocator allocator = new FrameSlotAllocator(budget, 4);
    final List<FrameSlot> frames = new ArrayList<>();
    try {
      final long frameLimit = budget - budget / 8 - allocator.oversizedHeadroomBytes();
      for (long bytes = 0L; bytes < frameLimit; bytes += 256 * KIB) {
        final FrameSlot frame = allocator.allocateSlot(256 * KIB);
        assertNotNull(frame);
        frames.add(frame);
      }
      final MemorySegment slab = allocator.allocateSlab(4 * KIB);
      assertNotNull(slab);
      assertTrue(allocator.releaseSlab(slab));
      final long scratchAddress;
      try (ScratchLease scratch = new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ)) {
        scratchAddress = scratch.segment().address();
      }
      final long committed = allocator.getPhysicalMemoryBytes();
      final ScratchStats before = allocator.scratchStats();
      assertNull(allocator.allocateSlot(4 * KIB));
      assertEquals(committed, allocator.getPhysicalMemoryBytes());
      assertEquals(committed, allocator.nonExecutionCommittedBytes());
      assertEquals(4 * KIB, allocator.getRecycledSlabBytes());
      assertEquals(before.totalCreated(), allocator.scratchStats().totalCreated());
      assertEquals(before.totalReclaimed(), allocator.scratchStats().totalReclaimed());
      try (ScratchLease scratch = new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ)) {
        assertEquals(scratchAddress, scratch.segment().address());
      }
      final MemorySegment reused = allocator.allocateSlab(4 * KIB);
      assertNotNull(reused);
      assertEquals(slab.address(), reused.address());
      assertTrue(allocator.releaseSlab(reused));
    } finally {
      for (final FrameSlot frame : frames) {
        frame.close();
      }
      allocator.shutdown();
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
    assertEquals(0L, allocator.nonExecutionCommittedBytes());
    assertEquals(0L, allocator.getActiveMemoryBytes());
    assertEquals(0L, allocator.getRecycledSlabBytes());
  }

  // The leases fill an array; two are returned early and borrowed again while four stay lent.
  @SuppressWarnings("java:S2093")
  @ParameterizedTest
  @CsvSource({"FRAME, 4", "SLAB, 4", "REGION, 4", "FRAME, 1", "SLAB, 1", "REGION, 1"})
  void lentSmallBuffersCannotFundImpossibleAdmission(final Target target, final int idleBuffers) {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64 * MIB, idleBuffers);
    final List<FrameSlot> frames = new ArrayList<>(204);
    final ScratchLease[] leases = new ScratchLease[6];
    final long[] addresses = new long[2];
    try {
      for (int frame = 0; frame < 204; frame++) {
        final FrameSlot slot = allocator.allocateSlot(256 * KIB);
        assertNotNull(slot);
        frames.add(slot);
      }
      for (int buffer = 0; buffer < leases.length; buffer++) {
        leases[buffer] = new ScratchLease(allocator).acquire(256 * KIB, ScratchUse.READ);
        SegmentAccess.putLong(leases[buffer].segment(), 0L, buffer + 1L);
      }
      for (int buffer = 0; buffer < addresses.length; buffer++) {
        addresses[buffer] = leases[buffer].segment().address();
        leases[buffer].close();
      }
      assertEquals(52 * MIB + 512 * KIB, allocator.getPhysicalMemoryBytes());
      switch (target) {
        case FRAME -> assertNull(allocator.allocateSlot(128 * KIB));
        case SLAB -> assertFalse(allocator.reserveCommittedBytes(3 * MIB + 128 * KIB));
        case REGION -> assertFalse(allocator.reserveScratchCommittedBytes(4 * MIB + 128 * KIB));
        default -> throw new AssertionError(target);
      }
      assertEquals(52 * MIB + 512 * KIB, allocator.getPhysicalMemoryBytes());
      assertEquals(0L, allocator.scratchStats().totalReclaimed());
      for (int buffer = 0; buffer < addresses.length; buffer++) {
        leases[buffer].acquire(256 * KIB, ScratchUse.READ);
        final long address = leases[buffer].segment().address();
        assertTrue(address == addresses[0] || address == addresses[1]);
      }
      assertNotEquals(leases[0].segment().address(), leases[1].segment().address());
      for (int buffer = 2; buffer < leases.length; buffer++) {
        assertEquals(buffer + 1L, SegmentAccess.getLong(leases[buffer].segment(), 0L));
      }
      assertEquals(6L, allocator.scratchStats().totalCreated());
    } finally {
      for (final ScratchLease lease : leases) {
        if (lease != null) {
          lease.close();
        }
      }
      for (final FrameSlot frame : frames) {
        frame.close();
      }
      allocator.shutdown();
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
    assertEquals(0L, allocator.nonExecutionCommittedBytes());
  }

  // The leases are held in a list and returned in borrow order mid-test, not at a block's end.
  @SuppressWarnings("java:S2093")
  @Test
  void admissionReclaimsTheRealShortfallAboveTheFrameLimit() {
    final long budget = 64 * MIB;
    final FrameSlotAllocator allocator = new FrameSlotAllocator(budget, 4);
    final List<FrameSlot> frames = new ArrayList<>();
    final List<ScratchLease> leases = new ArrayList<>(3);
    try {
      final long frameLimit = budget - budget / 8 - allocator.oversizedHeadroomBytes();
      for (long bytes = 0L; bytes < frameLimit - 256 * KIB; bytes += 256 * KIB) {
        final FrameSlot frame = allocator.allocateSlot(256 * KIB);
        assertNotNull(frame);
        frames.add(frame);
      }
      final MemorySegment slab = allocator.allocateSlab(4 * KIB);
      assertNotNull(slab);
      assertTrue(allocator.releaseSlab(slab));
      for (int buffer = 0; buffer < 3; buffer++) {
        leases.add(new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ));
      }
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      final long committed = allocator.getPhysicalMemoryBytes();
      assertEquals(frameLimit + 132 * KIB, committed);
      final FrameSlot frame = allocator.allocateSlot(128 * KIB);
      assertNotNull(frame);
      frames.add(frame);
      assertEquals(frameLimit, allocator.getPhysicalMemoryBytes());
      assertEquals(260 * KIB, committed + 128 * KIB - allocator.getPhysicalMemoryBytes());
      assertEquals(2L, allocator.scratchStats().totalReclaimed());
      assertEquals(1L, allocator.scratchStats().totalIdleBuffers());
      assertEquals(0L, allocator.getRecycledSlabBytes());
    } finally {
      for (final ScratchLease lease : leases) {
        lease.close();
      }
      for (final FrameSlot frame : frames) {
        frame.close();
      }
      allocator.shutdown();
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
    assertEquals(0L, allocator.nonExecutionCommittedBytes());
    assertEquals(0L, allocator.getActiveMemoryBytes());
  }

  // The leases are held in a list and returned in borrow order mid-test, not at a block's end.
  @SuppressWarnings("java:S2093")
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

  // The leases are held in a list and returned in borrow order mid-test, not at a block's end.
  @SuppressWarnings("java:S2093")
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

  // The leases are held in a list and returned in borrow order mid-test, not at a block's end.
  @SuppressWarnings("java:S2093")
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
      for (final long idleBytes : new long[] {512 * KIB, 256 * KIB, 128 * KIB}) {
        try (var _ = new ScratchLease(allocator).acquire(idleBytes, ScratchUse.READ)) {
          // Returned at once: the test starts from the three idle regions these borrows leave behind.
        }
      }
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

  // The three leases are returned in borrow order, which try-with-resources would reverse.
  @SuppressWarnings("java:S2093")
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
