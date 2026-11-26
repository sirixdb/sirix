/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.RecordingVirtualMemory.Call;
import io.sirix.cache.RecordingVirtualMemory.Kind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-103 — Cross-class transfer on demand, cases (a) to (e) of the chapter, plus MEM-402's
 * counters. Every case uses an independent allocator with <em>B</em> = 256 MiB and the default
 * reserves, so <em>L_f</em> = 208 MiB, the scratch ceiling is 224 MiB and the slab ceiling is 220
 * MiB.
 *
 * <ul>
 * <li>(a) the production sequence, on a real allocator and the real OS backend: fill the 64 KiB
 * class to <em>L_f</em>, release every slot, allocate one 4 KiB frame. The baseline throws
 * {@link OutOfMemoryError} after its 10 s wait; the change succeeds without firing the pressure
 * listener.</li>
 * <li>(b) scratch: 208 MiB of frames plus 16 MiB of live scratch exhaust the 224 MiB ceiling;
 * slabs: 208 MiB of frames plus 12 MiB of live slabs exhaust the 220 MiB ceiling. The next 4 KiB
 * reservation is refused while the frames are owned; after the frames are released and the
 * non-frame charges kept, a mandatory scratch reservation and a slab allocation succeed without
 * pressure.</li>
 * <li>(c) batch bounds: with byte shortfalls of 1 byte, one granule and one batch + 1 the refunded
 * bytes are at least the shortfall and at most the shortfall plus one batch.</li>
 * <li>(d) no recycled slot of the requesting frame's own class is returned while that class's
 * recycled set is non-empty.</li>
 * <li>(e) with recycled bytes in three other classes, slots are taken from the class holding the
 * most recycled bytes first, re-ranked after each batch.</li>
 * </ul>
 */
@MemPlanned(step = 1, value = {"MEM-103", "MEM-402"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class CrossClassTransferTest {

  private static final long MIB = MemPoolFixture.MIB;
  private static final long KIB = MemPoolFixture.KIB;
  private static final long BUDGET = 256L * MIB;

  /** A real allocator whose 64 KiB class was filled to <em>L_f</em>; the frames, in slot order. */
  private static List<FrameSlotAllocator.FrameSlot> fillSixtyFourKiB(final FrameSlotAllocator allocator) {
    final List<FrameSlotAllocator.FrameSlot> frames = new ArrayList<>();
    FrameSlotAllocator.FrameSlot slot;
    while ((slot = allocator.allocateSlot(64 * KIB)) != null) {
      frames.add(slot);
    }
    assertEquals(FrameBudgetOracle.frameCommitLimit(allocator), frames.size() * 64 * KIB,
        "setup: the class must be filled to L_f");
    assertEquals(208L * MIB, FrameBudgetOracle.frameCommitLimit(allocator), "B = 256 MiB with the default reserves");
    return frames;
  }

  private static void closeAll(final List<FrameSlotAllocator.FrameSlot> frames) {
    for (final FrameSlotAllocator.FrameSlot frame : frames) {
      frame.close();
    }
  }

  /**
   * An allocation whose {@link OutOfMemoryError} (the baseline's answer) fails the test instead of
   * aborting the run.
   */
  private static MemorySegment allocateOrFail(final FrameSlotAllocator allocator, final long bytes) {
    try {
      return allocator.allocate(bytes);
    } catch (final OutOfMemoryError exhausted) {
      throw new AssertionError("MEM-103 (a): the baseline throws OutOfMemoryError here; the change must transfer budget"
          + " instead: " + exhausted.getMessage(), exhausted);
    }
  }

  @Test
  @DisplayName("MEM-103 (a) a 4 KiB frame is served after the 64 KiB class filled the budget and was released, with no pressure")
  void productionSequenceSucceedsWithoutPressure() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    try (MemPoolSeam.PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      closeAll(fillSixtyFourKiB(allocator));
      final MemorySegment frame = allocateOrFail(allocator, 4096L);
      assertNotNull(frame);
      assertEquals(0, pressure.fired(), "MEM-103: the transfer runs before the pressure listener fires");
      allocator.release(frame);
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-103 (b) with the scratch ceiling exhausted by frames and live scratch, a mandatory scratch reservation is served after the frames are released")
  void scratchIsServedByReturningFrames() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    try (MemPoolSeam.PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      final List<FrameSlotAllocator.FrameSlot> frames = fillSixtyFourKiB(allocator);
      assertTrue(allocator.reserveScratchCommittedBytes(16 * MIB),
          "16 MiB of live scratch exhaust the 224 MiB ceiling");
      assertFalse(allocator.reserveScratchCommittedBytes(4096L), "refused while the frames are owned");
      assertEquals(0, pressure.fired(), "an optional reservation does not ask for pressure");
      closeAll(frames);
      try {
        assertTrue(allocator.reserveScratchCommittedBytes(4096L, true));
      } catch (final OutOfMemoryError exhausted) {
        throw new AssertionError(
            "MEM-103 (b): the mandatory reservation must transfer budget: " + exhausted.getMessage(), exhausted);
      }
      assertEquals(0, pressure.fired(), "MEM-103: served without pressure or parking");
      allocator.refundCommittedBytes(4096L);
      allocator.refundCommittedBytes(16 * MIB);
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-103 (b) with the slab ceiling exhausted by frames and live slabs, a slab is served after the frames are released")
  void slabIsServedByReturningFrames() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    try (MemPoolSeam.PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      final List<FrameSlotAllocator.FrameSlot> frames = fillSixtyFourKiB(allocator);
      final MemorySegment live = allocator.allocateSlab(12 * MIB);
      assertNotNull(live, "12 MiB of live slabs exhaust the 220 MiB ceiling");
      assertNull(allocator.allocateSlab(4096L), "refused while the frames are owned");
      closeAll(frames);
      final MemorySegment slab;
      try {
        slab = allocator.allocateSlab(4096L);
      } catch (final OutOfMemoryError exhausted) {
        throw new AssertionError("MEM-103 (b): a slab reservation must transfer budget: " + exhausted.getMessage(),
            exhausted);
      }
      assertNotNull(slab, "MEM-103 (b): a slab reservation must claim recycled frames of another class");
      assertEquals(0, pressure.fired());
      allocator.releaseSlab(slab);
      allocator.releaseSlab(live);
    } finally {
      allocator.shutdown();
    }
  }

  /**
   * Fill {@code classIdx} to <em>L_f</em>, exhaust the non-frame ceiling with live scratch, then
   * release the frames.
   */
  private static void fillHoldScratchAndRelease(final MemPoolFixture fixture, final int classIdx) {
    final List<FrameSlotAllocator.FrameSlot> frames = fixture.fill(classIdx);
    assertEquals(fixture.frameLimit(), FrameBudgetOracle.chargedFrameBytes(fixture.allocator),
        "setup: the class filled the frame limit exactly");
    final long headroom = FrameBudgetOracle.scratchCommitLimit(fixture.allocator) - fixture.frameLimit();
    assertTrue(fixture.reserveNonFrame(headroom), "live scratch exhausts the non-frame ceiling");
    fixture.releaseAll(frames);
  }

  @ParameterizedTest(name = "class {0}, OS page {1}, shortfall {2}")
  @CsvSource({"4, 4096, 1", "4, 4096, 65536", "4, 4096, 2097153", "4, 16384, 1", "4, 16384, 65536", "4, 16384, 2097153",
      "4, 65536, 1", "4, 65536, 65536", "4, 65536, 2097153", "0, 4096, 1", "0, 4096, 4096", "0, 4096, 2097153",
      "0, 16384, 1", "0, 16384, 16384", "0, 16384, 2097153", "0, 65536, 1", "0, 65536, 65536", "0, 65536, 2097153"})
  @DisplayName("MEM-103 (c) the refund covers the byte shortfall and exceeds it by at most one batch")
  void refundIsBoundedByTheShortfallAndOneBatch(final int classIdx, final long osPageBytes, final long shortfall) {
    try (MemPoolFixture fixture = MemPoolFixture.create(BUDGET, osPageBytes)) {
      fillHoldScratchAndRelease(fixture, classIdx);
      final long chargedBefore = FrameBudgetOracle.chargedFrameBytes(fixture.allocator);

      assertTrue(fixture.reserveNonFrame(shortfall), "the reservation is served by returning recycled frames");

      final long refunded = chargedBefore - FrameBudgetOracle.chargedFrameBytes(fixture.allocator);
      assertTrue(refunded >= shortfall, "refund " + refunded + " must cover the shortfall " + shortfall);
      assertTrue(refunded <= shortfall + MemPoolSeam.BATCH_BYTES,
          "refund " + refunded + " exceeds shortfall " + shortfall + " by more than one batch");
      assertEquals(refunded, MemPoolSeam.bytesRefunded(fixture.allocator, classIdx), "MEM-402 counts the refund");
      assertTrue(MemPoolSeam.transfers(fixture.allocator, classIdx) >= 1L, "MEM-402 counts the transfer");
      fixture.assertAccounting("the reservation");
    }
  }

  @Test
  @DisplayName("MEM-103 (d) a frame request never takes from its own class, which is empty of recycled slots when it asks")
  void aFrameRequestClaimsOnlyOtherClasses() {
    try (MemPoolFixture fixture = MemPoolFixture.create(BUDGET, 4096L)) {
      final List<FrameSlotAllocator.FrameSlot> sixtyFour = fixture.allocate(4, (int) (104 * MIB / (64 * KIB)));
      final List<FrameSlotAllocator.FrameSlot> sixteen = fixture.allocate(2, (int) (104 * MIB / (16 * KIB)));
      assertEquals(fixture.frameLimit(), FrameBudgetOracle.chargedFrameBytes(fixture.allocator));
      fixture.releaseAll(sixtyFour);
      final int mark = fixture.vm.mark();

      assertNotNull(fixture.allocate(2), "a 16 KiB frame is served from the 64 KiB class's budget");

      final List<Call> runs =
          fixture.vm.callsSince(mark).stream().filter(call -> call.kind() == Kind.RELEASE_RUN).toList();
      assertTrue(!runs.isEmpty(), "the request had to return budget");
      for (final Call run : runs) {
        assertEquals(4, run.classIdx(), "MEM-103 (d): only the other class's slots are claimed");
      }
      assertEquals(0L, MemPoolSeam.returned(fixture.allocator, 2));
      fixture.releaseAll(sixteen);
    }
  }

  @Test
  @DisplayName("MEM-103 (d) a request whose class has recycled slots takes them and returns nothing to the OS")
  void aFrameRequestWithRecycledSlotsOfItsOwnClassTransfersNothing() {
    try (MemPoolFixture fixture = MemPoolFixture.create(BUDGET, 4096L)) {
      final List<FrameSlotAllocator.FrameSlot> sixtyFour = fixture.allocate(4, (int) (104 * MIB / (64 * KIB)));
      final List<FrameSlotAllocator.FrameSlot> sixteen = fixture.allocate(2, (int) (104 * MIB / (16 * KIB)));
      fixture.releaseAll(sixtyFour);
      fixture.release(sixteen.remove(0));
      final int mark = fixture.vm.mark();
      final long transfers = MemPoolSeam.transfers(fixture.allocator, 4);

      assertNotNull(fixture.allocate(2));

      assertEquals(List.of(), fixture.vm.callsSince(mark),
          "MEM-103 (d)/MEM-102: the recycled slot of its own class is used");
      assertEquals(transfers, MemPoolSeam.transfers(fixture.allocator, 4));
      assertEquals(0L, MemPoolSeam.returnedTotal(fixture.allocator));
    }
  }

  @Test
  @DisplayName("MEM-103 a non-frame request may claim any class")
  void aNonFrameRequestMayClaimAnyClass() {
    try (MemPoolFixture fixture = MemPoolFixture.create(BUDGET, 4096L)) {
      fillHoldScratchAndRelease(fixture, 2);
      assertTrue(fixture.reserveNonFrame(1 * MIB));
      assertTrue(MemPoolSeam.returned(fixture.allocator, 2) > 0L);
    }
  }

  @Test
  @DisplayName("MEM-103 (e) slots are taken from the class holding the most recycled bytes first, re-ranked after each batch")
  void classesAreDrainedMostRecycledBytesFirst() {
    try (MemPoolFixture fixture = MemPoolFixture.create(BUDGET, 4096L)) {
      // 120 MiB owned in the 64 KiB class, 64 MiB in the 16 KiB class and 24 MiB in the 4 KiB class: the
      // whole of L_f.
      final List<FrameSlotAllocator.FrameSlot> sixtyFour = fixture.allocate(4, (int) (120 * MIB / (64 * KIB)));
      final List<FrameSlotAllocator.FrameSlot> sixteen = fixture.allocate(2, (int) (64 * MIB / (16 * KIB)));
      final List<FrameSlotAllocator.FrameSlot> four = fixture.allocate(0, (int) (24 * MIB / (4 * KIB)));
      assertEquals(fixture.frameLimit(), FrameBudgetOracle.chargedFrameBytes(fixture.allocator), "setup fills L_f");
      // Recycled bytes: 15 MiB, 8 MiB and 5 MiB; the rest stays owned.
      fixture.releaseAll(sixtyFour.subList(0, (int) (15 * MIB / (64 * KIB))).stream().toList());
      fixture.releaseAll(sixteen.subList(0, (int) (8 * MIB / (16 * KIB))).stream().toList());
      fixture.releaseAll(four.subList(0, (int) (5 * MIB / (4 * KIB))).stream().toList());
      assertTrue(
          fixture.reserveNonFrame(FrameBudgetOracle.scratchCommitLimit(fixture.allocator) - fixture.frameLimit()),
          "live scratch exhausts the non-frame ceiling");
      final int mark = fixture.vm.mark();

      assertTrue(fixture.reserveNonFrameMandatoryOrFail(12 * MIB));

      // Expected, by 2 MiB batches: 15 -> 13 -> 11 -> 9 -> 7 (class 4 x4), then class 2 holds the most (8
      // -> 6),
      // then class 4 again (7 -> 5): 4, 4, 4, 4, 2, 4. Consecutive batches of one class merge into one
      // entry.
      final List<long[]> merged = new ArrayList<>();
      for (final Call run : fixture.vm.callsSince(mark)) {
        if (run.kind() != Kind.RELEASE_RUN) {
          continue;
        }
        if (!merged.isEmpty() && merged.get(merged.size() - 1)[0] == run.classIdx()) {
          merged.get(merged.size() - 1)[1] += run.bytes();
        } else {
          merged.add(new long[] {run.classIdx(), run.bytes()});
        }
      }
      assertTrue(merged.size() >= 3, "at least three drain phases");
      assertEquals(4L, merged.get(0)[0], "the class holding the most recycled bytes goes first");
      assertEquals(8 * MIB, merged.get(0)[1], "four batches of class 4 until class 2 holds more");
      assertEquals(2L, merged.get(1)[0], "re-ranked after each batch: class 2 now holds the most");
      assertEquals(2 * MIB, merged.get(1)[1]);
      assertEquals(4L, merged.get(2)[0], "re-ranked again: class 4 holds the most once more");
      long returned = 0L;
      for (final long[] phase : merged) {
        returned += phase[1];
      }
      assertTrue(returned >= 12 * MIB && returned <= 12 * MIB + MemPoolSeam.BATCH_BYTES,
          "the transfer stops at the shortfall, rounded up to one batch: " + returned);
      fixture.assertAccounting("the drain");
    }
  }
}
