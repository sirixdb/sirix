package io.sirix.access.trx;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the contract of {@link RevisionEpochTracker} after the free-list rewrite
 * and the bumped default slot count.
 */
final class RevisionEpochTrackerTest {

  @Test
  void register_returnsTicketWithUniqueSlots() {
    final RevisionEpochTracker tracker = new RevisionEpochTracker(8);
    final Set<Integer> slotsSeen = new HashSet<>();
    for (int i = 0; i < 8; i++) {
      final RevisionEpochTracker.Ticket ticket = tracker.register(i);
      assertTrue(slotsSeen.add(ticket.getSlotIndex()), "slot " + ticket.getSlotIndex() + " reused while still active");
    }
    assertEquals(8, slotsSeen.size());
  }

  @Test
  void register_throwsWhenCapacityExhausted() {
    final RevisionEpochTracker tracker = new RevisionEpochTracker(4);
    final List<RevisionEpochTracker.Ticket> tickets = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      tickets.add(tracker.register(i));
    }
    final IllegalStateException e = assertThrows(IllegalStateException.class, () -> tracker.register(99));
    assertTrue(e.getMessage().contains("max=4"), "expected capacity in message: " + e.getMessage());
    assertTrue(e.getMessage().contains(RevisionEpochTracker.SLOT_COUNT_PROPERTY),
               "expected override hint in message: " + e.getMessage());
  }

  @Test
  void deregister_recyclesSlotsForReuse() {
    final RevisionEpochTracker tracker = new RevisionEpochTracker(4);
    // Cycle 100x through a 4-slot tracker — only possible if deregister actually frees the slot.
    for (int round = 0; round < 100; round++) {
      final List<RevisionEpochTracker.Ticket> tickets = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        tickets.add(tracker.register(round * 4 + i));
      }
      // Now full — next register must throw.
      assertThrows(IllegalStateException.class, () -> tracker.register(0));
      // Free them all.
      for (final RevisionEpochTracker.Ticket t : tickets) {
        tracker.deregister(t);
      }
    }
  }

  @Test
  void minActiveRevision_reflectsLowestActiveTicket() {
    final RevisionEpochTracker tracker = new RevisionEpochTracker(8);
    tracker.setLastCommittedRevision(50);
    assertEquals(50, tracker.minActiveRevision(), "no active txns → lastCommitted");

    final RevisionEpochTracker.Ticket t10 = tracker.register(10);
    final RevisionEpochTracker.Ticket t30 = tracker.register(30);
    final RevisionEpochTracker.Ticket t5 = tracker.register(5);
    assertEquals(5, tracker.minActiveRevision());

    tracker.deregister(t5);
    assertEquals(10, tracker.minActiveRevision());

    tracker.deregister(t10);
    tracker.deregister(t30);
    assertEquals(50, tracker.minActiveRevision(), "no active txns → lastCommitted");
  }

  @Test
  void deregister_nullTicket_isANoOp() {
    final RevisionEpochTracker tracker = new RevisionEpochTracker(2);
    tracker.deregister(null);
    // Capacity unaffected.
    tracker.register(1);
    tracker.register(2);
  }

  @Test
  void defaultSlotCount_returnsSensibleHeadroom() {
    // Sanity: the default needs to be much larger than the previous 4096 cap so a normal soak
    // does not run out. Hardcoding the exact value would be a test-tightening anti-pattern;
    // assert the order of magnitude instead.
    assertTrue(RevisionEpochTracker.DEFAULT_SLOT_COUNT >= 16_384,
               "default slot count " + RevisionEpochTracker.DEFAULT_SLOT_COUNT + " is too low");
  }

  @Test
  void register_isThreadSafeUnderConcurrentLoad() throws Exception {
    final int slots = 1024;
    final int threads = 16;
    final int opsPerThread = 5_000;
    final RevisionEpochTracker tracker = new RevisionEpochTracker(slots);
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicInteger maxConcurrent = new AtomicInteger();
    final AtomicInteger live = new AtomicInteger();
    try {
      final List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(pool.submit(() -> {
          try {
            start.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          for (int i = 0; i < opsPerThread; i++) {
            final RevisionEpochTracker.Ticket ticket = tracker.register(i);
            assertNotNull(ticket);
            final int now = live.incrementAndGet();
            maxConcurrent.updateAndGet(prev -> Math.max(prev, now));
            // Yield occasionally so other threads interleave.
            if ((i & 0xff) == 0) {
              Thread.yield();
            }
            live.decrementAndGet();
            tracker.deregister(ticket);
          }
        }));
      }
      start.countDown();
      for (final var f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }
    // After all threads complete every slot must be free again — capacity == slots.
    final List<RevisionEpochTracker.Ticket> drain = new ArrayList<>();
    for (int i = 0; i < slots; i++) {
      drain.add(tracker.register(i));
    }
    assertThrows(IllegalStateException.class, () -> tracker.register(0));
    drain.forEach(tracker::deregister);
    // The peak concurrent ticket count must be > 1 (proves the threads actually overlapped).
    assertNotEquals(1, maxConcurrent.get(), "no observable concurrency — test setup is wrong");
  }
}
