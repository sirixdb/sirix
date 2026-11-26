/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.RecordingVirtualMemory.Call;
import io.sirix.cache.RecordingVirtualMemory.Kind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-104 — granule alignment. With the OS page size <em>P</em> at 16 KiB and 64 KiB (fake), a 4
 * KiB slot is returned only when all slots of its page are claimed; otherwise it goes back to the
 * recycled set, still charged. The granule is the larger of the slot size and <em>P</em>, so a run
 * starts and ends on a granule boundary and covers only whole OS pages that lie inside claimed
 * slots.
 */
@MemPlanned(step = 1, value = {"MEM-104"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class GranuleAlignmentTest {

  @ParameterizedTest(name = "class {0} (slots of {1} B), OS page {2}")
  @CsvSource({"0, 4096, 16384", "0, 4096, 65536", "1, 8192, 65536", "2, 16384, 65536", "6, 262144, 65536"})
  @DisplayName("MEM-104 only whole OS pages inside claimed slots are returned; the rest stays recycled and charged")
  void onlyWholePagesAreReturned(final int classIdx, final long slotSize, final long osPageBytes) {
    try (MemPoolFixture fixture = MemPoolFixture.create(osPageBytes)) {
      final FrameSlotAllocator allocator = fixture.allocator;
      final long granule = Math.max(slotSize, osPageBytes);
      final int slotsPerGranule = (int) (granule / slotSize);
      final List<FrameSlotAllocator.FrameSlot> all = fixture.fill(classIdx);
      final int granules = all.size() / slotsPerGranule;
      // Every third granule keeps one owned slot: its other slots can never form an OS page.
      final Set<Integer> heldGranules = new HashSet<>();
      final List<FrameSlotAllocator.FrameSlot> free = new ArrayList<>();
      int partiallyFreeSlots = 0;
      for (final FrameSlotAllocator.FrameSlot slot : all) {
        final int granuleIdx = slot.slotIndex() / slotsPerGranule;
        final boolean holdsThisGranule = granuleIdx % 3 == 0;
        final boolean isTheHeldOne = slot.slotIndex() % slotsPerGranule == (slotsPerGranule > 1
            ? 1
            : 0);
        if (holdsThisGranule && isTheHeldOne) {
          heldGranules.add(granuleIdx);
        } else {
          free.add(slot);
          if (holdsThisGranule) {
            partiallyFreeSlots++;
          }
        }
      }
      fixture.releaseAll(free);
      final int recycledBefore = allocator.recycledSlotCount(classIdx);
      assertEquals(free.size(), recycledBefore);
      final int mark = fixture.vm.mark();

      assertTrue(fixture.reserveNonFrame(1 * MemPoolFixture.MIB), "whole free granules exist to cover the shortfall");

      long returnedSlots = 0L;
      for (final Call run : fixture.vm.callsSince(mark)) {
        if (run.kind() != Kind.RELEASE_RUN) {
          continue;
        }
        assertEquals(0L, run.address() % osPageBytes, "a run starts on an OS page boundary: " + run);
        assertEquals(0L, run.bytes() % osPageBytes, "a run ends on an OS page boundary: " + run);
        assertEquals(0L, (run.firstSlot() * slotSize) % granule, "a run starts on a granule boundary: " + run);
        assertEquals(0L, (run.slotCount() * slotSize) % granule, "a run covers whole granules: " + run);
        for (long slot = run.firstSlot(); slot < run.firstSlot() + run.slotCount(); slot++) {
          assertFalse(heldGranules.contains((int) (slot / slotsPerGranule)),
              "MEM-104: a page with an owned slot is never returned: slot " + slot);
        }
        returnedSlots += run.slotCount();
      }
      assertTrue(returnedSlots > 0L, "the transfer returned something");
      assertEquals(returnedSlots, MemPoolSeam.returned(allocator, classIdx));
      assertTrue(allocator.recycledSlotCount(classIdx) >= partiallyFreeSlots,
          "MEM-104: slots that cannot form an OS page stay in the recycled set");
      fixture.assertAccounting("the partial transfer");
      assertTrue(granules > heldGranules.size());
    }
  }

  @ParameterizedTest(name = "class 0, OS page {0}")
  @CsvSource({"16384", "65536"})
  @DisplayName("MEM-104 slots of a page with an owned slot are never returned even when the shortfall is not covered")
  void partialPagesStayChargedWhenTheShortfallCannotBeCovered(final long osPageBytes) {
    try (MemPoolFixture fixture = MemPoolFixture.create(osPageBytes)) {
      final FrameSlotAllocator allocator = fixture.allocator;
      final int slotsPerPage = (int) (osPageBytes / 4096L);
      final List<FrameSlotAllocator.FrameSlot> all = fixture.fill(0);
      // One owned slot in every page: not a single whole OS page can be returned.
      final List<FrameSlotAllocator.FrameSlot> free = new ArrayList<>();
      for (final FrameSlotAllocator.FrameSlot slot : all) {
        if (slot.slotIndex() % slotsPerPage != 0) {
          free.add(slot);
        }
      }
      fixture.releaseAll(free);
      final long chargedBefore = FrameBudgetOracle.chargedFrameBytes(allocator);
      final int recycledBefore = allocator.recycledSlotCount(0);

      assertFalse(fixture.reserveNonFrame(1 * MemPoolFixture.MIB),
          "no granule-releasable bytes exist, so the shortfall cannot be covered");

      assertEquals(List.of(), fixture.vm.callsOf(Kind.RELEASE_RUN), "no OS call may cover a page with an owned slot");
      assertEquals(chargedBefore, FrameBudgetOracle.chargedFrameBytes(allocator), "nothing is refunded");
      assertEquals(recycledBefore, allocator.recycledSlotCount(0), "the slots went back to the recycled set, charged");
      assertEquals(0L, MemPoolSeam.returned(allocator, 0));
      fixture.assertAccounting("the refused transfer");
    }
  }
}
