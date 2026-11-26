/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import com.sun.management.ThreadMXBean;
import io.sirix.cache.RecordingVirtualMemory.Call;
import io.sirix.cache.RecordingVirtualMemory.Kind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MEM-109 — Class capacity covers the frame budget. For <em>B</em> of 256 MiB, 16 GiB and 64 GiB
 * every class reserves at least <em>L_f</em>, rounded up to whole slots; never-minted capacity has
 * no materialized per-slot version or scope metadata; a refusing fake fails initialization with the
 * class and the requested bytes in the error and releases every earlier region.
 *
 * <p>
 * The capacity arithmetic runs on a fake that maps nothing, so no test reserves hundreds of GiB of
 * address space. Filling real 64 KiB frames past 32 GiB is not required (the chapter says the fake
 * verifies the address-space arithmetic).
 */
@Isolated
@MemPlanned(step = 1, value = {"MEM-109"})
final class ClassCapacityTest {

  private static final long MIB = MemPoolFixture.MIB;
  private static final long GIB = 1024L * MIB;

  @ParameterizedTest(name = "B = {0} MiB")
  @ValueSource(longs = {256L, 16L * 1024L, 64L * 1024L})
  @DisplayName("MEM-109 every class reserves at least L_f, rounded up to whole slots")
  void everyClassReservesTheFrameLimit(final long budgetMiB) {
    final RecordingVirtualMemory vm = RecordingVirtualMemory.withOsPageBytes(4096L);
    vm.withoutMapping();
    final FrameSlotAllocator allocator = MemPoolSeam.newAllocator(budgetMiB * MIB, vm);
    try {
      final long frameLimit = FrameBudgetOracle.frameCommitLimit(allocator);
      final List<Long> reserved = vm.reservedBytes();
      assertTrue(reserved.size() >= FrameSlotAllocator.SIZE_CLASSES.length, "one reservation per size class");
      for (int classIdx = 0; classIdx < FrameSlotAllocator.SIZE_CLASSES.length; classIdx++) {
        final long slotSize = FrameSlotAllocator.SIZE_CLASSES[classIdx];
        final long wholeSlots = (frameLimit + slotSize - 1L) / slotSize;
        assertTrue(reserved.get(classIdx) >= wholeSlots * slotSize,
            "MEM-109: class " + classIdx + " reserved " + reserved.get(classIdx) + " bytes, fewer than " + wholeSlots
                + " slots of " + slotSize + " (L_f = " + frameLimit + ")");
        assertEquals(0L, reserved.get(classIdx) % slotSize, "whole slots only");
        assertTrue(allocator.slotCount(classIdx) >= wholeSlots, "MEM-109: class " + classIdx + " must address "
            + wholeSlots + " slots, not " + allocator.slotCount(classIdx));
      }
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-109 never-minted capacity costs no per-slot version or scope metadata on the heap")
  void neverMintedCapacityIsLazy() {
    final var bean = ManagementFactory.getThreadMXBean();
    assumeTrue(bean instanceof ThreadMXBean, "needs the com.sun.management thread bean");
    final ThreadMXBean allocations = (ThreadMXBean) bean;
    assumeTrue(allocations.isThreadAllocatedMemorySupported());
    if (!allocations.isThreadAllocatedMemoryEnabled()) {
      allocations.setThreadAllocatedMemoryEnabled(true);
    }
    // Warm up: class loading and first-use tables are not what is measured.
    final RecordingVirtualMemory warm = RecordingVirtualMemory.withOsPageBytes(4096L);
    warm.withoutMapping();
    MemPoolSeam.newAllocator(256L * MIB, warm).shutdown();

    final RecordingVirtualMemory vm = RecordingVirtualMemory.withOsPageBytes(4096L);
    vm.withoutMapping();
    final long thread = Thread.currentThread().threadId();
    final long before = allocations.getThreadAllocatedBytes(thread);
    final FrameSlotAllocator allocator = MemPoolSeam.newAllocator(64L * GIB, vm);
    final long allocated = allocations.getThreadAllocatedBytes(thread) - before;
    try {
      long totalSlots = 0L;
      for (int classIdx = 0; classIdx < FrameSlotAllocator.SIZE_CLASSES.length; classIdx++) {
        totalSlots += allocator.slotCount(classIdx);
      }
      // Fixed per-class tables plus a few bits per slot (the recycled set, the returned set); a per-slot
      // version (8 bytes) or scope reference would be sixteen times this.
      final long bound = totalSlots / 2L + 8L * MIB;
      assertTrue(totalSlots > 16_000_000L, "the 64 GiB budget addresses tens of millions of slots: " + totalSlots);
      assertTrue(allocated <= bound, "MEM-109: initializing " + totalSlots + " slots allocated " + allocated
          + " bytes on the heap; never-minted capacity may cost at most " + bound);
    } finally {
      allocator.shutdown();
    }
  }

  @ParameterizedTest(name = "class {0} refused")
  @ValueSource(ints = {0, 3, 6})
  @DisplayName("MEM-109 a refused reservation fails initialization naming the class and bytes, and releases the earlier regions")
  void aRefusedReservationFailsLoudlyAndRollsBack(final int refusedClass) {
    final RecordingVirtualMemory vm = RecordingVirtualMemory.withOsPageBytes(4096L);
    vm.refuseReservationOf(refusedClass);

    Throwable failure = null;
    FrameSlotAllocator allocator = null;
    try {
      allocator = MemPoolSeam.newAllocator(256L * MIB, vm);
    } catch (final Throwable refused) {
      failure = refused;
    }

    assertNotNull(failure, "MEM-109: capacity is never silently reduced; initialization must fail");
    assertTrue(!(failure instanceof AssertionError) || vm.callsOf(Kind.RESERVE).size() > refusedClass,
        "the seam, not the allocator, failed: " + failure.getMessage());
    final List<Call> reservations = vm.callsOf(Kind.RESERVE);
    final Call refused = reservations.get(refusedClass);
    assertTrue(refused.injectedFailure(), "the fake refused the reservation of class " + refusedClass);
    final String message = String.valueOf(failure.getMessage());
    assertTrue(message.contains(Long.toString(refused.bytes())),
        "MEM-109: the error names the requested bytes " + refused.bytes() + ": " + message);
    assertTrue(
        message.contains("class " + refusedClass)
            || message.contains(Long.toString(FrameSlotAllocator.SIZE_CLASSES[refusedClass])),
        "MEM-109: the error names the class " + refusedClass + ": " + message);
    assertEquals(refusedClass, vm.callsOf(Kind.RELEASE_REGION).size(),
        "MEM-109: every region reserved before the refusal is released");
    assertEquals(null, allocator, "no half-initialized allocator escapes");
  }
}
