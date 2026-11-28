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
 * MEM-101 — Accounting identity. After every operation of a seeded sequence of allocate / release /
 * transfer / re-charge / shutdown over all seven classes, for every class
 * {@code minted = owned + recycled + inTransfer + returned} and
 * {@code chargedFrames = slotSize * (owned + recycled + inTransfer)}, process-wide
 * {@code committedBytes = sum(chargedFrames) + non-frame charges}, frame charges never exceed
 * <em>L_f</em>, and after {@code shutdown()} {@code committedBytes} is 0. Deterministic, over the
 * recording {@link VirtualMemory} fake; the jqwik twin is {@code FrameBudgetAccountingProperty}.
 *
 * <p>
 * Planned (CONV-102): needs the injectable {@code VirtualMemory} and the per-class counters of step
 * 1.
 */
@MemPlanned(step = 1, value = {"MEM-101"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class FrameBudgetAccountingTest {

  @ParameterizedTest(name = "seed {0}")
  @ValueSource(longs = {1L, 2L, 3L, 0xC0FFEEL, 0xDEADBEEFL, 20261126L})
  @DisplayName("MEM-101 identities and the limit hold after every operation of a seeded sequence")
  void identitiesHoldAfterEveryOperation(final long seed) {
    FrameBudgetSequence.runSeeded(seed, 400);
  }

  @ParameterizedTest(name = "OS page {0}")
  @ValueSource(longs = {4096L, 16384L, 65536L})
  @DisplayName("MEM-101 the identities do not depend on the OS page size")
  void identitiesHoldForEveryPageSize(final long osPageBytes) {
    final int[] codes = new int[300];
    for (int i = 0; i < codes.length; i++) {
      codes[i] = i % FrameBudgetSequence.OPERATIONS;
    }
    FrameBudgetSequence.run(7L, codes, osPageBytes);
  }

  @Test
  @DisplayName("MEM-101 a cross-class transfer keeps both identities: refund only after the release, re-charge before the hand-out")
  void crossClassTransferKeepsTheIdentities() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      final List<FrameSlotAllocator.FrameSlot> big = fixture.fill(4);
      assertEquals(448, big.size(), "the 64 KiB class fills L_f = 28 MiB exactly");
      fixture.assertAccounting("filled");
      fixture.releaseAll(big);
      fixture.assertAccounting("released");
      assertEquals(448, fixture.allocator.recycledSlotCount(4));

      final FrameSlotAllocator.FrameSlot small = fixture.allocate(0);
      assertTrue(small != null,
          "MEM-103: a 4 KiB frame must be served by returning budget from the recycled 64 KiB class");
      fixture.assertAccounting("4 KiB frame after the transfer");
      assertTrue(MemPoolSeam.returnedTotal(fixture.allocator) > 0L, "the transfer returned slots of the 64 KiB class");

      fixture.release(small);
      final List<FrameSlotAllocator.FrameSlot> again = fixture.fill(4);
      assertTrue(!again.isEmpty(), "re-charge: returned 64 KiB slots are handed out again");
      fixture.assertAccounting("re-charged");
    }
  }
}
