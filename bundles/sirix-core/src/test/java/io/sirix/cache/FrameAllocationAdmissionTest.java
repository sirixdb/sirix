/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.CompactImageStore.Block;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock("FrameSlotAllocator.pressureListener")
final class FrameAllocationAdmissionTest {
  private static final long KIB = 1024L;
  private static final long MIB = 1024 * KIB;

  private enum Kind {
    SCRATCH, FRAME, OVERSIZED
  }

  @ParameterizedTest
  @CsvSource({"262144, 204800, 229376", "16777216, 15728640, 14680064", "33554432, 30408704, 29360128"})
  void roundedScratchAboveItsCeilingNeverCallsPressure(final long budget, final long requested, final long ceiling) {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(budget);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    try (ScratchLease lease = new ScratchLease(allocator)) {
      assertFalse(lease.tryAcquire(requested, ScratchUse.READ));
      final OutOfMemoryError failure =
          assertThrows(OutOfMemoryError.class, () -> lease.acquire(requested, ScratchUse.READ));
      assertTrue(failure.getMessage().contains(Long.toString(ceiling)));
      assertEquals(0, pressure.get());
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
      assertEquals(0L, allocator.scratch().regionBytes());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      allocator.shutdown();
    }
  }

  @Test
  void roundedPageFrameAboveItsCeilingNeverCallsPressure() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(256 * KIB);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    try {
      assertNull(allocator.allocateSlot(200 * KIB));
      final OutOfMemoryError failure = assertThrows(OutOfMemoryError.class, () -> allocator.allocate(200 * KIB));
      assertTrue(failure.getMessage().contains("229376"));
      assertEquals(0, pressure.get());
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      allocator.shutdown();
    }
  }

  @Test
  void cacheSlabsAndOversizedAllocationsRespectTheirCeilingBeforePressure() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    try {
      assertNull(allocator.allocateSlab(14 * MIB + 1L));
      final OutOfMemoryError failure = assertThrows(OutOfMemoryError.class, () -> allocator.allocate(15 * MIB));
      assertTrue(failure.getMessage().contains("14680064"));
      assertEquals(0, pressure.get());
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
      assertEquals(0L, allocator.nonExecutionCommittedBytes());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      allocator.shutdown();
    }
  }

  @Test
  void compactPoolSlabsChargeNoneOfTheFrameAllowance() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64 * MIB);
    // A pool ceiling of one slab: its own share is the only thing that bounds it.
    final CompactImageStore store = new CompactImageStore(allocator, 256 * KIB);
    // Commit the whole permanent frame allowance, as a peak of page frames does.
    final long frameAllowance = 64 * MIB - 8 * MIB - allocator.oversizedHeadroomBytes();
    assertTrue(allocator.reserveSlabCommittedBytes(frameAllowance));
    assertFalse(allocator.reserveSlabCommittedBytes(1), "the frame allowance is spent");
    try {
      final List<Block> images = new ArrayList<>(4);
      try {
        for (int i = 0; i < 4; i++) {
          // Four 64 KiB blocks are one slab of the largest class: the pool serves them although no
          // page frame could commit another byte, because it charges a different budget.
          final Block image = store.acquire(65536);
          assertNotNull(image, "a compact image does not compete with page frames for commitment");
          images.add(image);
        }
        assertEquals(frameAllowance, allocator.nonExecutionCommittedBytes(),
            "the pool's slab charged none of the allocator's commitment");
        assertEquals(256 * KIB, store.mappedSlabBytes());
        assertNull(store.acquire(65536), "the pool stops at its own ceiling");
      } finally {
        for (final Block image : images) {
          image.run();
        }
      }
      // A returned block is reused instead of growing, which is how the pool stays inside its
      // ceiling without anything having to be handed back.
      final Block reused = store.acquire(65536);
      assertNotNull(reused);
      assertEquals(256 * KIB, store.mappedSlabBytes(), "reuse, not growth");
      reused.run();
      // The headroom the frame allowance left is intact: nothing the pool did touched it.
      final MemorySegment oversized = allocator.allocate(512 * KIB);
      assertNotNull(oversized);
      allocator.release(oversized);
      final MemorySegment reclaimable = allocator.allocateSlab(512 * KIB);
      assertNotNull(reclaimable, "reclaimable storage retains its existing admission allowance");
      assertTrue(allocator.releaseSlab(reclaimable));
    } finally {
      allocator.refundCommittedBytes(frameAllowance);
      assertTrue(store.releaseIdleSlabs());
      allocator.shutdown();
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void requestsThatFitStillRecoverThroughPressure(final Kind kind) {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    final MemorySegment held = allocator.allocateSlab(14 * MIB);
    assertNotNull(held);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(() -> {
      pressure.incrementAndGet();
      allocator.releaseSlab(held);
    });
    try {
      switch (kind) {
        case SCRATCH -> {
          try (ScratchLease scratch = new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ)) {
            assertEquals(128 * KIB, scratch.capacity());
          }
        }
        case FRAME, OVERSIZED -> {
          final long bytes = kind == Kind.FRAME
              ? 4 * KIB
              : 512 * KIB;
          final MemorySegment allocation = allocator.allocate(bytes);
          assertNotNull(allocation);
          assertEquals(bytes, allocation.byteSize());
          allocator.release(allocation);
        }
      }
      assertEquals(1, pressure.get());
      assertEquals(0L, allocator.getActiveMemoryBytes());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      if (allocator.getSlabBytes() != 0L) {
        allocator.releaseSlab(held);
      }
      allocator.shutdown();
    }
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
    assertEquals(0L, allocator.nonExecutionCommittedBytes());
    assertEquals(0L, allocator.getRecycledSlabBytes());
  }
}
