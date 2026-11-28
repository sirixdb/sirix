/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-102 — Same-class reuse and the read path are unchanged. A warmed allocate/release cycle
 * within one class, and a cycle that refills from the class's own recycled set, issue zero
 * {@link VirtualMemory} calls (recording fake); no transfer runs, no pressure fires and the MEM-402
 * counters do not move, because counters are updated only on the refusal path. The allocation side
 * of MEM-102 is {@code FrameReuseAllocationBudgetTest}.
 */
@MemPlanned(step = 1, value = {"MEM-102", "MEM-402"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class FrameReuseSyscallFreeTest {

  private static final int CYCLES = 2_000;

  @ParameterizedTest(name = "class {0}")
  @ValueSource(ints = {0, 3, 4, 6})
  @DisplayName("MEM-102 a warmed allocate/release cycle within one class makes no OS call")
  void warmedSameClassCycleIsSyscallFree(final int classIdx) {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L);
        MemPoolSeam.PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      fixture.release(fixture.allocate(classIdx));
      final int mark = fixture.vm.mark();
      final long transfersBefore = MemPoolSeam.transfers(fixture.allocator, classIdx);
      for (int cycle = 0; cycle < CYCLES; cycle++) {
        fixture.release(fixture.allocate(classIdx));
      }
      assertEquals(List.of(), fixture.vm.callsSince(mark), "MEM-102: a same-class cycle must not reach the OS");
      assertEquals(transfersBefore, MemPoolSeam.transfers(fixture.allocator, classIdx),
          "MEM-402: counters are updated only on the refusal path");
      assertEquals(0, pressure.fired(), "no reservation was refused, so no pressure fires");
    }
  }

  @Test
  @DisplayName("MEM-102 refilling a class from its own recycled set makes no OS call and runs no transfer")
  void refillFromTheOwnRecycledSetIsSyscallFree() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L);
        MemPoolSeam.PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      final int classIdx = 4;
      final List<FrameSlotAllocator.FrameSlot> first = fixture.allocate(classIdx, 128);
      fixture.releaseAll(first);
      assertEquals(128, fixture.allocator.recycledSlotCount(classIdx));
      final int mark = fixture.vm.mark();
      final long slotsReturnedBefore = MemPoolSeam.slotsReturned(fixture.allocator, classIdx);

      for (int round = 0; round < 20; round++) {
        final List<FrameSlotAllocator.FrameSlot> again = fixture.allocate(classIdx, 128);
        assertEquals(0, fixture.allocator.recycledSlotCount(classIdx), "the refill drained the recycled set");
        fixture.releaseAll(again);
      }

      assertEquals(List.of(), fixture.vm.callsSince(mark),
          "MEM-102: refilling from the recycled set must not reach the OS");
      assertEquals(slotsReturnedBefore, MemPoolSeam.slotsReturned(fixture.allocator, classIdx));
      assertEquals(0L, MemPoolSeam.returnedTotal(fixture.allocator), "nothing was ever returned to the OS");
      assertEquals(0, pressure.fired());
      fixture.assertAccounting("after the refills");
    }
  }

  @Test
  @DisplayName("MEM-102 optimistic reads (acquire and validate a version) make no OS call")
  void readPathMakesNoOsCall() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      final FrameSlotAllocator.FrameSlot slot = fixture.allocate(2);
      final int mark = fixture.vm.mark();
      for (int read = 0; read < CYCLES; read++) {
        final long version = fixture.allocator.acquireVersion(slot.classIndex(), slot.slotIndex());
        assertEquals(slot.versionAtAlloc(), version);
        assertTrue(fixture.allocator.validateVersion(slot.classIndex(), slot.slotIndex(), version));
      }
      assertEquals(List.of(), fixture.vm.callsSince(mark));
    }
  }
}
