/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MEM-102: same-class reuse and the read path are unchanged. After warmup the allocator adds no
 * allocation to a same-class allocate/release cycle, to a cycle that refills from the class's own
 * recycled set, or to an optimistic read ({@code acquireVersion}/{@code validateVersion}, the
 * {@code readStamp}/{@code validateStamp} of the chapter). This is the allocation half of the
 * CONV-302 comparison of step 1, and a regression guard: it describes behavior that already holds,
 * and the change that lands MEM-103/104 must keep it.
 *
 * <p>
 * "Unchanged" permits a fixed 40-byte frame handle in the clean allocation fork's 64-bit JVM
 * layout (compressed references and class pointers, 8-byte alignment, ordinary object headers),
 * plus a JDK segment slice measured independently. Neither allowance executes the production
 * handle constructor. The scoped-segment control uses only the JDK API. Optimistic reads must
 * allocate nothing, and a positive control proves the measurement sees allocation.
 */
@Isolated
@MemPlanned(step = 1, value = {"MEM-102"})
final class FrameReuseAllocationBudgetTest {

  private static final int CLASS_IDX = 1;
  private static final long SLOT_BYTES = FrameSlotAllocator.SIZE_CLASSES[CLASS_IDX];
  private static final int CYCLES = 16_384;
  private static final int PASSES = 9;
  private static final int REFILL = 64;
  private static final long HANDLE_BYTES = 40L;
  private static volatile Object sink;

  @Test
  @DisplayName("MEM-102: a same-class allocateSlot/close cycle allocates no more than its handle")
  void sameClassSlotCycleAddsNoAllocation() {
    final ThreadMXBean bean = threadBean();
    final FrameSlotAllocator allocator = new FrameSlotAllocator(32L * 1024 * 1024);
    try {
      final MemorySegment backing = Arena.global().allocate(2 * SLOT_BYTES);
      sink = allocator.allocateSlot(SLOT_BYTES); // mint one slot, then recycle it forever
      ((FrameSlotAllocator.FrameSlot) sink).close();
      final long thread = Thread.currentThread().threadId();
      long measured = 0L;
      long control = 0L;
      for (int pass = 0; pass < PASSES; pass++) {
        measured = slotCycles(allocator, bean, thread);
        control = CYCLES * HANDLE_BYTES + sliceControl(backing, bean, thread);
      }
      assertTrue(measured <= control, "allocateSlot/close allocated " + measured + " bytes over " + CYCLES
          + " cycles, more than the " + control + " bytes of the handle and slice they return");
      assertEquals(0, allocator.liveSlotCount(CLASS_IDX), "every measured slot was released");
    } finally {
      sink = null;
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-102: allocate/release of a segment from the class's recycled set adds no allocation")
  void sameClassSegmentCycleAddsNoAllocation() {
    final ThreadMXBean bean = threadBean();
    final FrameSlotAllocator allocator = new FrameSlotAllocator(32L * 1024 * 1024);
    try {
      final MemorySegment backing = Arena.global().allocate(2 * SLOT_BYTES);
      allocator.release(allocator.allocate(SLOT_BYTES));
      final long thread = Thread.currentThread().threadId();
      long measured = 0L;
      long control = 0L;
      for (int pass = 0; pass < PASSES; pass++) {
        long before = bean.getThreadAllocatedBytes(thread);
        for (int cycle = 0; cycle < CYCLES; cycle++) {
          final MemorySegment segment = allocator.allocate(SLOT_BYTES);
          sink = segment;
          allocator.release(segment);
        }
        measured = bean.getThreadAllocatedBytes(thread) - before;
        before = bean.getThreadAllocatedBytes(thread);
        for (int cycle = 0; cycle < CYCLES; cycle++) {
          sink = backing.asSlice(0, SLOT_BYTES).reinterpret(SLOT_BYTES, Arena.ofShared(), null);
        }
        control = bean.getThreadAllocatedBytes(thread) - before;
      }
      assertTrue(measured <= control, "allocate/release allocated " + measured + " bytes over " + CYCLES
          + " cycles, more than the " + control + " bytes of the scoped segment they return");
      assertEquals(0, allocator.liveSlotCount(CLASS_IDX));
    } finally {
      sink = null;
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-102: refilling a class from its own recycled set adds no allocation")
  void refillFromTheRecycledSetAddsNoAllocation() {
    final ThreadMXBean bean = threadBean();
    final FrameSlotAllocator allocator = new FrameSlotAllocator(32L * 1024 * 1024);
    try {
      final MemorySegment backing = Arena.global().allocate(2 * SLOT_BYTES);
      final FrameSlotAllocator.FrameSlot[] held = new FrameSlotAllocator.FrameSlot[REFILL];
      for (int index = 0; index < REFILL; index++) {
        held[index] = allocator.allocateSlot(SLOT_BYTES);
      }
      for (final FrameSlotAllocator.FrameSlot slot : held) {
        slot.close();
      }
      assertEquals(REFILL, allocator.recycledSlotCount(CLASS_IDX), "precondition: the class's recycled set is full");
      final long thread = Thread.currentThread().threadId();
      final int rounds = CYCLES / REFILL;
      long measured = 0L;
      long control = 0L;
      for (int pass = 0; pass < PASSES; pass++) {
        long before = bean.getThreadAllocatedBytes(thread);
        for (int round = 0; round < rounds; round++) {
          for (int index = 0; index < REFILL; index++) {
            held[index] = allocator.allocateSlot(SLOT_BYTES);
          }
          for (int index = 0; index < REFILL; index++) {
            held[index].close();
          }
        }
        measured = bean.getThreadAllocatedBytes(thread) - before;
        control = CYCLES * HANDLE_BYTES + sliceControl(backing, bean, thread);
      }
      assertTrue(measured <= control, "a refill cycle allocated " + measured + " bytes, more than the " + control
          + " bytes of the handles and slices it returns");
      assertEquals(REFILL, allocator.recycledSlotCount(CLASS_IDX), "the cycle returned every slot to the recycled set");
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-102: an optimistic read (acquireVersion + validateVersion) allocates nothing")
  void optimisticReadAllocatesNothing() {
    final ThreadMXBean bean = threadBean();
    final FrameSlotAllocator allocator = new FrameSlotAllocator(32L * 1024 * 1024);
    try (FrameSlotAllocator.FrameSlot slot = allocator.allocateSlot(SLOT_BYTES)) {
      final long thread = Thread.currentThread().threadId();
      long bytes = 0L;
      int valid = 0;
      for (int pass = 0; pass < PASSES; pass++) {
        valid = 0;
        final long before = bean.getThreadAllocatedBytes(thread);
        for (int read = 0; read < CYCLES; read++) {
          final long stamp = allocator.acquireVersion(slot.classIndex(), slot.slotIndex());
          if (allocator.validateVersion(slot.classIndex(), slot.slotIndex(), stamp)) {
            valid++;
          }
        }
        bytes = bean.getThreadAllocatedBytes(thread) - before;
      }
      assertEquals(0L, bytes, "an optimistic read must not allocate");
      assertEquals(CYCLES, valid, "the measured reads validated: the owner never moved");
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-102: the measurement sees allocation (positive control)")
  void measurementSeesAllocation() {
    final ThreadMXBean bean = threadBean();
    final long thread = Thread.currentThread().threadId();
    final long before = bean.getThreadAllocatedBytes(thread);
    for (int index = 0; index < CYCLES; index++) {
      sink = new byte[64];
    }
    final long bytes = bean.getThreadAllocatedBytes(thread) - before;
    sink = null;
    assertTrue(bytes >= (long) CYCLES * 64L, "a deliberate 64-byte allocation per cycle must show, saw " + bytes);
  }

  private static long slotCycles(final FrameSlotAllocator allocator, final ThreadMXBean bean, final long thread) {
    final long before = bean.getThreadAllocatedBytes(thread);
    for (int cycle = 0; cycle < CYCLES; cycle++) {
      final FrameSlotAllocator.FrameSlot slot = allocator.allocateSlot(SLOT_BYTES);
      sink = slot;
      slot.close();
    }
    return bean.getThreadAllocatedBytes(thread) - before;
  }

  private static long sliceControl(final MemorySegment backing, final ThreadMXBean bean, final long thread) {
    final long before = bean.getThreadAllocatedBytes(thread);
    for (int cycle = 0; cycle < CYCLES; cycle++) {
      sink = backing.asSlice(0, SLOT_BYTES);
    }
    return bean.getThreadAllocatedBytes(thread) - before;
  }

  private static ThreadMXBean threadBean() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    return bean;
  }
}
