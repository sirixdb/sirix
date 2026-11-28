/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.RecordingVirtualMemory.Kind;
import io.sirix.utils.OS;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-105 — Re-charge before minting. With returned slots in class <em>c</em> and none recycled,
 * the next allocation of <em>c</em> reserves the bytes, takes a returned slot and leaves
 * {@code nextFreshIndex} (the minted count) unchanged; with the reservation refused it claims
 * nothing and the returned count is unchanged; a re-charged slot is handed out with the normal
 * odd/even version transition.
 */
@MemPlanned(step = 1, value = {"MEM-105", "MEM-402"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class FrameRechargeTest {

  private static final int CLASS_IDX = 4;
  private static final long SLOT = FrameSlotAllocator.SIZE_CLASSES[CLASS_IDX];
  private static final long TWO_MIB = 2L * MemPoolFixture.MIB;

  /**
   * The 64 KiB class holds <em>L_f</em>; its first 32 slots (2 MiB) are released and a 2 MiB
   * non-frame reservation returns them to the OS. Afterwards 32 slots are returned, none is recycled
   * and the budget is full again (the reservation holds the freed 2 MiB).
   */
  private static long[] returnThirtyTwoSlots(final MemPoolFixture fixture) {
    final List<FrameSlotAllocator.FrameSlot> all = fixture.fill(CLASS_IDX);
    assertEquals(448, all.size());
    fixture.releaseAll(all.subList(0, 32).stream().toList());
    assertTrue(fixture.reserveNonFrame(TWO_MIB), "the reservation returns the 32 recycled slots");
    assertEquals(32L, MemPoolSeam.returned(fixture.allocator, CLASS_IDX));
    assertEquals(0, fixture.allocator.recycledSlotCount(CLASS_IDX));
    final long[] versions = new long[32];
    for (int slot = 0; slot < versions.length; slot++) {
      versions[slot] = fixture.allocator.acquireVersion(CLASS_IDX, slot);
    }
    return versions;
  }

  @Test
  @DisplayName("MEM-105 the next allocation re-charges a returned slot instead of minting")
  void reChargeTakesAReturnedSlotAndMintsNothing() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      final long[] versionsBefore = returnThirtyTwoSlots(fixture);
      final long mintedBefore = MemPoolSeam.minted(fixture.allocator, CLASS_IDX);
      fixture.refundNonFrame(TWO_MIB);
      final long chargedBefore = FrameBudgetOracle.chargedFrameBytes(fixture.allocator);
      final int mark = fixture.vm.mark();

      final FrameSlotAllocator.FrameSlot slot = fixture.allocate(CLASS_IDX);

      assertNotNull(slot, "MEM-105: the freed budget admits a re-charge");
      assertEquals(mintedBefore, MemPoolSeam.minted(fixture.allocator, CLASS_IDX),
          "MEM-105: a returned slot is taken before a fresh index is minted (nextFreshIndex unchanged)");
      assertTrue(slot.slotIndex() < 32, "the slot is one of the returned ones: " + slot.slotIndex());
      assertEquals(31L, MemPoolSeam.returned(fixture.allocator, CLASS_IDX));
      assertEquals(chargedBefore + SLOT, FrameBudgetOracle.chargedFrameBytes(fixture.allocator),
          "the bytes were reserved before the slot was handed out");
      assertEquals(SLOT, MemPoolSeam.bytesRecharged(fixture.allocator, CLASS_IDX), "MEM-402 counts the re-charge");
      assertEquals(0L, slot.versionAtAlloc() & 1L, "the handed-out version is even");
      assertTrue(slot.versionAtAlloc() > versionsBefore[slot.slotIndex()],
          "MEM-105: the normal odd/even transition: strictly greater than the version it was returned with");
      assertEquals(slot.versionAtAlloc(), fixture.allocator.acquireVersion(CLASS_IDX, slot.slotIndex()));
      assertEquals(List.of(),
          fixture.vm.callsSince(mark).stream().filter(call -> call.kind() == Kind.RELEASE_RUN).toList(),
          "a re-charge releases nothing");
      fixture.assertAccounting("the re-charge");
    }
  }

  @Test
  @DisplayName("MEM-105 on macOS the re-charge issues the re-use advice for the slot's run")
  void macReChargeAdvisesTheRun() {
    Assumptions.assumeTrue(OS.isMacOSX(), "MADV_FREE_REUSE is issued on macOS only (MEM-106)");
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      returnThirtyTwoSlots(fixture);
      fixture.refundNonFrame(TWO_MIB);
      final int mark = fixture.vm.mark();
      assertNotNull(fixture.allocate(CLASS_IDX));
      assertTrue(fixture.vm.callsSince(mark).stream().anyMatch(call -> call.kind() == Kind.RECHARGE),
          "MEM-106: re-charge on macOS issues MADV_FREE_REUSE");
    }
  }

  @Test
  @DisplayName("MEM-105 a refused reservation claims nothing: the returned count is unchanged")
  void aRefusedReservationLeavesTheReturnedSetUntouched() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      returnThirtyTwoSlots(fixture);
      final long mintedBefore = MemPoolSeam.minted(fixture.allocator, CLASS_IDX);
      final long rechargedBefore = MemPoolSeam.bytesRecharged(fixture.allocator, CLASS_IDX);

      assertNull(fixture.allocate(CLASS_IDX), "the budget is full: the re-charge reservation is refused");

      assertEquals(32L, MemPoolSeam.returned(fixture.allocator, CLASS_IDX), "MEM-105: nothing was claimed");
      assertEquals(mintedBefore, MemPoolSeam.minted(fixture.allocator, CLASS_IDX));
      assertEquals(rechargedBefore, MemPoolSeam.bytesRecharged(fixture.allocator, CLASS_IDX));
      fixture.assertAccounting("the refused re-charge");
    }
  }

  @Test
  @DisplayName("MEM-105 all returned slots are re-charged before any fresh index is minted")
  void returnedSlotsAreExhaustedBeforeMinting() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      returnThirtyTwoSlots(fixture);
      final long mintedBefore = MemPoolSeam.minted(fixture.allocator, CLASS_IDX);
      fixture.refundNonFrame(TWO_MIB);

      for (int taken = 0; taken < 32; taken++) {
        assertNotNull(fixture.allocate(CLASS_IDX), "re-charge " + taken);
        assertEquals(mintedBefore, MemPoolSeam.minted(fixture.allocator, CLASS_IDX),
            "still no fresh index after " + taken);
      }
      assertEquals(0L, MemPoolSeam.returned(fixture.allocator, CLASS_IDX));
      assertEquals(32L * SLOT, MemPoolSeam.bytesRecharged(fixture.allocator, CLASS_IDX));
    }
  }
}
