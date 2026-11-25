/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.MemorySegment;
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
