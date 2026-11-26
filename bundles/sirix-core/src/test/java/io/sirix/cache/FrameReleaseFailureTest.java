/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.RecordingVirtualMemory.Failure;
import io.sirix.cache.RecordingVirtualMemory.Kind;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-107 — Error cases, over a fake that injects failures. MEM-402 counters are asserted here too.
 *
 * <ul>
 * <li>(a) a failed or throwing release call returns that run's slots to the recycled set, refunds
 * nothing, increments the failure counter, and the transfer continues with other runs; a request
 * whose shortfall is then not covered proceeds to pressure, wait and the existing
 * {@link OutOfMemoryError}, whose message includes returned and failed counts;</li>
 * <li>(b) a refunded byte taken by another reservation before the requester retries: the requester
 * retries and the accounting identity holds;</li>
 * <li>(c) a failing re-charge advice does not fail the allocation and is counted;</li>
 * <li>(d) shutdown racing a transfer ends with {@code committedBytes == 0} and no OS call after the
 * regions were released.</li>
 * </ul>
 */
@MemPlanned(step = 1, value = {"MEM-107", "MEM-402"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class FrameReleaseFailureTest {

  private static final int CLASS_IDX = 4;
  private static final long MIB = MemPoolFixture.MIB;

  /**
   * The 64 KiB class holds <em>L_f</em>; its first {@code freeSlots} slots are released, the rest
   * stay owned.
   */
  private static List<FrameSlotAllocator.FrameSlot> fillAndReleaseFirst(final MemPoolFixture fixture,
      final int freeSlots) {
    final List<FrameSlotAllocator.FrameSlot> all = fixture.fill(CLASS_IDX);
    assertEquals(448, all.size());
    fixture.releaseAll(all.subList(0, freeSlots).stream().toList());
    return all;
  }

  @ParameterizedTest
  @EnumSource(Failure.class)
  @DisplayName("MEM-107 (a) a failed or throwing release call keeps its slots recycled, refunds nothing, and the transfer goes on")
  void aFailedRunKeepsItsSlotsAndTheTransferContinues(final Failure mode) {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      fillAndReleaseFirst(fixture, 64);
      fixture.vm.failNextReleaseRuns(1, mode);

      final boolean reserved = assertDoesNotThrow(() -> fixture.reserveNonFrame(2 * MIB),
          "MEM-107: a release failure never throws out of the transfer");

      assertTrue(reserved, "the transfer continued with other runs and covered the shortfall");
      assertEquals(1L, MemPoolSeam.failedReleaseCalls(fixture.allocator, CLASS_IDX), "MEM-402 counts the failed call");
      assertEquals(2 * MIB, MemPoolSeam.bytesRefunded(fixture.allocator, CLASS_IDX),
          "only the successful run refunds; the failed run refunds nothing");
      assertEquals(32L, MemPoolSeam.returned(fixture.allocator, CLASS_IDX));
      assertEquals(32, fixture.allocator.recycledSlotCount(CLASS_IDX), "the failed run's slots are recycled again");
      final FrameSlotAllocator.FrameSlot reused = fixture.allocate(CLASS_IDX);
      assertNotNull(reused, "the failed run's slots are usable");
      fixture.assertAccounting("the failed run");
    }
  }

  @Test
  @DisplayName("MEM-107 (a) when every release fails the request proceeds to pressure, wait and OutOfMemoryError naming returned and failed counts")
  void whenEveryReleaseFailsTheRequestEndsInOutOfMemoryError() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L);
        MemPoolSeam.PressureProbe pressure = MemPoolSeam.installPressureProbe()) {
      fillAndReleaseFirst(fixture, 64);
      fixture.vm.failNextReleaseRuns(Integer.MAX_VALUE, Failure.RETURN_FAILURE);

      final OutOfMemoryError failure =
          assertThrows(OutOfMemoryError.class, () -> fixture.reserveNonFrameMandatory(2 * MIB));

      final String message = failure.getMessage().toLowerCase(Locale.ROOT);
      assertTrue(message.contains("returned"),
          "MEM-107: the message includes the returned count: " + failure.getMessage());
      assertTrue(message.contains("failed"),
          "MEM-107: the message includes the failed-call count: " + failure.getMessage());
      assertTrue(pressure.fired() >= 1, "the existing path fired the pressure listener before giving up");
      assertTrue(MemPoolSeam.failedReleaseCalls(fixture.allocator, CLASS_IDX) >= 1L);
      assertEquals(0L, MemPoolSeam.bytesRefunded(fixture.allocator, CLASS_IDX), "a failed release refunds nothing");
      assertEquals(64, fixture.allocator.recycledSlotCount(CLASS_IDX), "every slot stayed recycled and usable");
      fixture.assertAccounting("the exhausted wait");
    }
  }

  @Test
  @DisplayName("MEM-107 (b) refunded bytes taken by another reservation: the requester retries and the identity holds")
  void aRefundTakenByAnotherReservationIsRetried() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      fillAndReleaseFirst(fixture, 128);
      final AtomicBoolean stolen = new AtomicBoolean();
      final AtomicLong bytesSoFar = new AtomicLong();
      fixture.vm.observeRuns(run -> {
        // The first run after a whole batch went to the OS: that batch's refund has landed. Take it.
        if (bytesSoFar.getAndAdd(run.bytes()) >= MemPoolSeam.BATCH_BYTES && stolen.compareAndSet(false, true)) {
          assertTrue(fixture.reserveNonFrame(2 * MIB), "the stolen bytes were really refunded before the second batch");
        }
      });

      assertTrue(fixture.reserveNonFrameMandatoryOrFail(3 * MIB),
          "the requester retries until the shortfall is covered");

      assertTrue(stolen.get(), "the competing reservation ran inside the transfer");
      assertTrue(MemPoolSeam.transfers(fixture.allocator, CLASS_IDX) >= 2L, "the requester ran another transfer");
      assertEquals(5 * MIB, fixture.nonFrameCharged());
      fixture.assertAccounting("the retried reservation");
    }
  }

  @Test
  @DisplayName("MEM-107 (c) a failing re-charge advice does not fail the allocation and is counted")
  void aFailingReChargeAdviceIsCountedAndHarmless() {
    try (MemPoolFixture fixture = MemPoolFixture.create(4096L)) {
      fillAndReleaseFirst(fixture, 32);
      assertTrue(fixture.reserveNonFrame(2 * MIB));
      fixture.refundNonFrame(2 * MIB);
      fixture.vm.failNextRecharges(1, Failure.RETURN_FAILURE);
      final long failedBefore = MemPoolSeam.failedReleaseCalls(fixture.allocator, CLASS_IDX);

      final FrameSlotAllocator.FrameSlot slot = fixture.allocate(CLASS_IDX);

      assertNotNull(slot, "MEM-107: re-charge advice that fails must not fail the allocation");
      Assumptions.assumeTrue(fixture.vm.callsOf(Kind.RECHARGE).size() >= 1,
          "this platform issues no re-charge advice (macOS issues MADV_FREE_REUSE, MEM-106): run on the macOS lane");
      assertEquals(failedBefore + 1L, MemPoolSeam.failedReleaseCalls(fixture.allocator, CLASS_IDX),
          "MEM-107 (c): the failed advice is counted");
      fixture.assertAccounting("the failed advice");
    }
  }

  @Test
  @DisplayName("MEM-107 (d) shutdown racing a transfer ends with committedBytes 0 and no OS call after the regions went back")
  void shutdownRacingATransferLeavesNothingCharged() throws InterruptedException {
    final MemPoolFixture fixture = MemPoolFixture.create(4096L);
    final FrameSlotAllocator allocator = fixture.allocator;
    fixture.releaseAll(fixture.fill(CLASS_IDX));
    final AtomicBoolean stop = new AtomicBoolean();
    final CountDownLatch started = new CountDownLatch(2);
    final ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
    final Thread[] workers = new Thread[2];
    for (int worker = 0; worker < workers.length; worker++) {
      workers[worker] = new Thread(() -> {
        started.countDown();
        while (!stop.get()) {
          try {
            if (allocator.reserveScratchCommittedBytes(2 * MIB)) {
              allocator.refundCommittedBytes(2 * MIB);
            }
            final FrameSlotAllocator.FrameSlot slot =
                allocator.allocateSlot(FrameSlotAllocator.SIZE_CLASSES[CLASS_IDX]);
            if (slot != null) {
              slot.close();
            }
          } catch (final IllegalStateException shutDown) {
            return;
          } catch (final Throwable failure) {
            unexpected.add(failure);
            return;
          }
        }
      }, "mem-107-worker-" + worker);
      workers[worker].start();
    }
    started.await();
    for (long spin = 0; spin < 200_000_000L && fixture.vm.callsOf(Kind.RELEASE_RUN).isEmpty(); spin++) {
      Thread.onSpinWait();
    }

    IllegalStateException refused = null;
    for (int attempt = 0; attempt < 10_000; attempt++) {
      try {
        allocator.shutdown();
        refused = null;
        break;
      } catch (final IllegalStateException liveOwner) {
        refused = liveOwner;
        Thread.onSpinWait();
      }
    }
    stop.set(true);
    for (final Thread worker : workers) {
      worker.join();
    }

    assertEquals(null, refused, "shutdown completes once the momentary frame owners are gone");
    assertEquals(List.of(), List.copyOf(unexpected), "workers see only the shutdown refusal");
    assertEquals(0L, allocator.getPhysicalMemoryBytes(),
        "MEM-107 (d): committedBytes is 0 after a shutdown that raced a transfer");
    assertEquals(0, fixture.vm.callsAfterRegionsReleased(), "MEM-107 (d): no OS call after the regions were released");
  }
}
