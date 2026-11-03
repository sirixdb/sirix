/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cache-owned slabs: charged to the physical budget before mapping, global-scope views, refunded
 * exactly once, releasable from any thread, and never served through page-cache pressure.
 */
final class FrameSlotAllocatorSlabTest {
  private static final long BUDGET = 4L << 20;

  @Test
  void slabsAreChargedBeforeMappingAndRefundedExactlyOnce() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    MemorySegment slab = null;
    try {
      final long committedBefore = allocator.getPhysicalMemoryBytes();
      final long activeBefore = allocator.getActiveMemoryBytes();
      slab = allocator.allocateSlab(100_000);
      assertNotNull(slab);
      assertEquals(102_400, slab.byteSize(), "charged at whole 4 KiB pages: 25 of them");
      assertSame(Arena.global().scope(), slab.scope(), "no per-access liveness check");
      assertEquals(102_400, allocator.getSlabBytes());
      assertEquals(committedBefore + 102_400, allocator.getPhysicalMemoryBytes());
      assertEquals(activeBefore + 102_400, allocator.getActiveMemoryBytes());
      SegmentAccess.putLong(slab, 8, 42L);
      SegmentAccess.putInt(slab, 102_396, 7);
      assertEquals(42L, SegmentAccess.getLong(slab, 8));
      assertEquals(7, SegmentAccess.getInt(slab, 102_396));
      assertThrows(IllegalStateException.class, allocator::shutdown, "live slabs block shutdown");
      final ExecutorService other = Executors.newSingleThreadExecutor();
      final MemorySegment mapped = slab;
      try {
        assertTrue(other.submit(() -> allocator.releaseSlab(mapped)).get(), "release from a foreign thread");
      } finally {
        other.shutdown();
      }
      assertFalse(allocator.releaseSlab(slab), "a repeated release is rejected");
      slab = null;
      assertEquals(0, allocator.getSlabBytes());
      assertEquals(committedBefore, allocator.getPhysicalMemoryBytes());
      assertEquals(activeBefore, allocator.getActiveMemoryBytes());
      assertThrows(IllegalArgumentException.class, () -> allocator.allocateSlab(0));
    } finally {
      if (slab != null && allocator.getSlabBytes() != 0) {
        allocator.releaseSlab(slab);
      }
      allocator.shutdown();
    }
  }

  @Test
  void aSlabBeyondTheBudgetIsRefusedWithoutEvictingPageCaches() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    try {
      assertNull(allocator.allocateSlab(BUDGET + 1), "a refusal, not an error, and no pressure");
      final MemorySegment first = allocator.allocateSlab(BUDGET / 2);
      assertNotNull(first);
      assertNull(allocator.allocateSlab(BUDGET / 2 + 4096), "retained plus request exceeds the budget");
      final MemorySegment second = allocator.allocateSlab(BUDGET / 2);
      assertNotNull(second, "exactly the remaining budget fits");
      assertEquals(0, pressure.get(), "slabs never evict persisted pages");
      assertTrue(allocator.releaseSlab(first));
      assertTrue(allocator.releaseSlab(second));
      assertEquals(0, allocator.getSlabBytes());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      allocator.shutdown();
    }
  }

  @Test
  void aStaleHandleWhoseAddressWasReusedCannotReleaseTheNewSlab() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    try {
      final MemorySegment first = allocator.allocateSlab(4096);
      assertNotNull(first);
      assertTrue(allocator.releaseSlab(first));
      final MemorySegment second = allocator.allocateSlab(4096);
      assertNotNull(second);
      if (second.address() == first.address()) {
        assertFalse(allocator.releaseSlab(first), "exact-instance identity protects the reused address");
        assertEquals(4096, allocator.getSlabBytes());
      }
      assertTrue(allocator.releaseSlab(second));
    } finally {
      allocator.shutdown();
    }
  }
}
