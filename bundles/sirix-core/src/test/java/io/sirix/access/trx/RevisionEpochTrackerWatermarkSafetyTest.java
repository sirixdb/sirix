package io.sirix.access.trx;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency-safety tests for {@link RevisionEpochTracker} — the epoch-based MVCC watermark that
 * gates page eviction. The tracker's single critical guarantee is:
 *
 * <blockquote>while any transaction holds a ticket for revision R, {@code minActiveRevision()}
 * must never return a value greater than R</blockquote>
 *
 * because a watermark above R would let the sweeper evict pages revision-R readers still need.
 *
 * <p>The watermark test uses a generation-stamped publication protocol so the assertion is sound
 * under concurrency: a worker publishes (odd generation, revision) via a volatile write only
 * AFTER {@code register} returns, and flips to an even generation BEFORE calling
 * {@code deregister}. A checker that observes the same odd generation before and after reading
 * the watermark has therefore proven the registration spanned the whole read — any watermark
 * above that revision is a real safety violation, not a race in the test.
 */
final class RevisionEpochTrackerWatermarkSafetyTest {

  private static final int WORKERS = 4;
  private static final int CHECKERS = 2;
  /**
   * Stress duration; overridable so mutation testing can use short runs
   * (-Dsirix.stress.run.millis=200) while regular CI keeps the full window.
   */
  private static final long RUN_MILLIS = Long.getLong("sirix.stress.run.millis", 1_500);

  @Test
  @Timeout(60)
  void watermarkNeverExceedsAnActivelyHeldRevision() throws InterruptedException {
    final RevisionEpochTracker tracker = new RevisionEpochTracker(1024);
    final AtomicInteger committedRevision = new AtomicInteger(1);
    tracker.setLastCommittedRevision(1);

    // states[w] packs (generation << 32) | revision; odd generation = holding a ticket.
    final AtomicLongArray states = new AtomicLongArray(WORKERS);
    final AtomicLong violations = new AtomicLong();
    final AtomicReference<String> firstViolation = new AtomicReference<>();
    final AtomicBoolean stop = new AtomicBoolean();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(WORKERS + CHECKERS + 1);

    // Committer: monotonically advances the committed revision.
    pool.execute(() -> {
      await(start);
      while (!stop.get()) {
        final int next = committedRevision.incrementAndGet();
        tracker.setLastCommittedRevision(next);
        Thread.onSpinWait();
      }
    });

    for (int w = 0; w < WORKERS; w++) {
      final int workerIndex = w;
      pool.execute(() -> {
        await(start);
        long generation = 0;
        while (!stop.get()) {
          final int revision = committedRevision.get();
          final RevisionEpochTracker.Ticket ticket = tracker.register(revision);
          generation++; // odd: holding
          states.set(workerIndex, (generation << 32) | (revision & 0xFFFF_FFFFL));
          for (int i = 0; i < 50; i++) {
            Thread.onSpinWait();
          }
          generation++; // even: released (published BEFORE the deregister)
          states.set(workerIndex, generation << 32);
          tracker.deregister(ticket);
        }
      });
    }

    for (int c = 0; c < CHECKERS; c++) {
      pool.execute(() -> {
        await(start);
        int workerIndex = 0;
        while (!stop.get()) {
          workerIndex = (workerIndex + 1) % WORKERS;
          final long before = states.get(workerIndex);
          if (((before >>> 32) & 1) == 0) {
            continue; // not holding
          }
          final int heldRevision = (int) before;
          final int watermark = tracker.minActiveRevision();
          final long after = states.get(workerIndex);
          if (before == after && watermark > heldRevision) {
            violations.incrementAndGet();
            firstViolation.compareAndSet(null,
                "watermark=" + watermark + " > heldRevision=" + heldRevision + " (worker " + workerIndex + ")");
          }
        }
      });
    }

    start.countDown();
    Thread.sleep(RUN_MILLIS);
    stop.set(true);
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "threads must terminate");

    assertEquals(0, violations.get(),
        "Eviction watermark exceeded an actively held revision: " + firstViolation.get());

    // Quiescent state: nothing registered, so the watermark equals the last committed revision.
    assertEquals(tracker.getLastCommittedRevision(), tracker.minActiveRevision());
  }

  @Test
  @Timeout(60)
  void slotsAreNeverDoublyAllocatedAndFreeListSurvivesContention() throws InterruptedException {
    final int slotCount = 8;
    final int threads = 8;
    final RevisionEpochTracker tracker = new RevisionEpochTracker(slotCount);
    final AtomicIntegerArray claims = new AtomicIntegerArray(slotCount);
    final AtomicLong doubleAllocations = new AtomicLong();
    final AtomicBoolean stop = new AtomicBoolean();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(threads);

    for (int t = 0; t < threads; t++) {
      pool.execute(() -> {
        await(start);
        while (!stop.get()) {
          final RevisionEpochTracker.Ticket ticket;
          try {
            ticket = tracker.register(7);
          } catch (final IllegalStateException fullyAllocated) {
            Thread.onSpinWait();
            continue;
          }
          // Exactly one live ticket may reference each slot at any moment.
          if (claims.incrementAndGet(ticket.getSlotIndex()) != 1) {
            doubleAllocations.incrementAndGet();
          }
          Thread.onSpinWait();
          claims.decrementAndGet(ticket.getSlotIndex());
          tracker.deregister(ticket);
        }
      });
    }

    start.countDown();
    Thread.sleep(RUN_MILLIS);
    stop.set(true);
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "threads must terminate");

    assertEquals(0, doubleAllocations.get(), "a slot was handed out to two live tickets at once");

    // The free list must be fully intact: exactly slotCount distinct slots remain allocatable.
    final List<RevisionEpochTracker.Ticket> tickets = new ArrayList<>(slotCount);
    final Set<Integer> slots = new HashSet<>();
    for (int i = 0; i < slotCount; i++) {
      final RevisionEpochTracker.Ticket ticket = tracker.register(1);
      tickets.add(ticket);
      slots.add(ticket.getSlotIndex());
    }
    assertEquals(slotCount, slots.size(), "leaked or duplicated slot indices after the hammering phase");
    assertThrows(IllegalStateException.class, () -> tracker.register(1),
        "no slot may be allocatable beyond the configured capacity");
    for (final RevisionEpochTracker.Ticket ticket : tickets) {
      tracker.deregister(ticket);
    }
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
