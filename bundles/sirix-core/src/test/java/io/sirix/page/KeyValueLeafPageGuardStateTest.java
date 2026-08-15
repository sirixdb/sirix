/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The guard protocol on {@link KeyValueLeafPage}, and the three things it must never do: hand out a
 * guard on a page that closed, close a page a guard is held on, or lose a count to a race.
 *
 * <p>
 * Today these invariants hold largely for free: {@code acquireGuard} and {@code close} are
 * {@code synchronized} on the page, so every check-then-act is atomic by mutual exclusion — at 31.6
 * ns per uncontended acquire/release pair ({@code CursorGuardCostBenchmark}). That cost is the
 * reason to move the count out of its own {@link AtomicInteger} and in beside the orphaned and
 * closed bits, so each operation becomes a single CAS on one word.
 *
 * <p>
 * These tests exist AHEAD of that change, and deliberately: they pin the invariants against the
 * implementation that is known to satisfy them, so the refactor has something to be judged by
 * rather than being argued from first principles afterwards. They must keep passing across it — a
 * green run here before and after is the whole point.
 */
public final class KeyValueLeafPageGuardStateTest {

  private static final int SIXTY_FOUR_KB = 64 * 1024;

  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
  }

  @AfterEach
  void tearDown() {
    if (arena != null) {
      arena.close();
    }
  }

  private KeyValueLeafPage newPage() {
    return new KeyValueLeafPage(1L, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("keyValueLeafPageGuardStateResource").build(), 1,
        arena.allocate(SIXTY_FOUR_KB), null);
  }

  @DisplayName("concurrent acquire/release pairs balance out to a count of zero")
  @Test
  void concurrentAcquireReleasePairsBalance() throws Exception {
    final int threadCount = 8;
    final int pairsPerThread = 20_000;
    final KeyValueLeafPage page = newPage();

    final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final AtomicInteger failedAcquires = new AtomicInteger();
      final List<Future<?>> futures = new ArrayList<>(threadCount);
      for (int t = 0; t < threadCount; t++) {
        futures.add(pool.submit(() -> {
          start.await();
          for (int i = 0; i < pairsPerThread; i++) {
            if (page.acquireGuard()) {
              page.releaseGuard();
            } else {
              failedAcquires.incrementAndGet();
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> f : futures) {
        f.get(60, TimeUnit.SECONDS);
      }

      assertEquals(0, failedAcquires.get(), "a live, unorphaned page must never refuse a guard");
      assertEquals(0, page.getGuardCount(), "every acquire was paired with a release");
      assertFalse(page.isClosed(), "nothing orphaned the page, so nothing may have closed it");
    } finally {
      pool.shutdownNow();
    }
  }

  @DisplayName("close is declined while a guard is held and succeeds once it is released")
  @Test
  void closeIsDeclinedWhileGuarded() {
    final KeyValueLeafPage page = newPage();
    assertTrue(page.acquireGuard(), "precondition: the page can be guarded");

    page.close();
    assertFalse(page.isClosed(), "a guarded page must not close");
    assertEquals(1, page.getGuardCount(), "a declined close must not disturb the count");

    page.releaseGuard();
    assertFalse(page.isClosed(), "releasing the last guard on a LIVE page does not close it");

    page.close();
    assertTrue(page.isClosed(), "an unguarded page closes");

    page.close();
    assertTrue(page.isClosed(), "close is idempotent");
  }

  @DisplayName("an orphan closes on the last release, and not before")
  @Test
  void orphanClosesOnTheLastRelease() {
    final KeyValueLeafPage page = newPage();
    assertTrue(page.acquireGuard(), "first guard");
    // The documented write-cursor case: the page is orphaned by a TIL copy-on-write while the
    // cursor holds it, and a second guard on that same page is still required.
    assertTrue(page.acquireGuard(), "an additional guard on a GUARDED orphan is allowed");

    page.markOrphaned();
    assertTrue(page.isOrphaned());
    assertEquals(2, page.getGuardCount());

    page.releaseGuard();
    assertFalse(page.isClosed(), "an orphan with a guard left must stay open");

    page.releaseGuard();
    assertTrue(page.isClosed(), "the last release closes the orphan");
  }

  @DisplayName("an orphan at zero guards cannot be resurrected")
  @Test
  void orphanAtZeroCannotBeResurrected() {
    final KeyValueLeafPage page = newPage();
    page.markOrphaned();

    assertFalse(page.acquireGuard(), "at zero an orphan may already be mid-teardown");
    assertEquals(0, page.getGuardCount(), "a refused acquire must not leave a count behind");
  }

  @DisplayName("acquire racing close never leaves a page both closed and guarded")
  @Test
  void acquireRacingCloseIsAtomic() throws Exception {
    final int rounds = 4000;
    final ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      int acquiredCount = 0;
      int closedCount = 0;
      for (int round = 0; round < rounds; round++) {
        final KeyValueLeafPage page = newPage();
        final CountDownLatch start = new CountDownLatch(1);

        final Future<Boolean> acquirer = pool.submit(() -> {
          start.await();
          return page.acquireGuard();
        });
        final Future<?> closer = pool.submit(() -> {
          start.await();
          page.close();
          return null;
        });

        start.countDown();
        final boolean acquired = acquirer.get(30, TimeUnit.SECONDS);
        closer.get(30, TimeUnit.SECONDS);

        // The invariant: the two outcomes are mutually exclusive. A page that closed cannot have
        // handed out a guard, and a page that handed out a guard cannot have closed under it.
        if (acquired) {
          acquiredCount++;
          assertFalse(page.isClosed(), "round " + round + ": guard acquired, so close must have been declined");
          assertEquals(1, page.getGuardCount(), "round " + round + ": the acquired guard must be counted");
          page.releaseGuard();
        } else {
          closedCount++;
          assertTrue(page.isClosed(), "round " + round + ": acquire was refused, so the page must be closed");
          assertEquals(0, page.getGuardCount(), "round " + round + ": a refused acquire leaves no count");
        }
      }
      // Not an assertion on the split — the scheduler decides that — but a run in which one side
      // never won would be testing nothing, and silence about it is how such a test rots.
      System.out.printf("%nacquireRacingCloseIsAtomic: acquirer won %d, closer won %d of %d rounds%n", acquiredCount,
          closedCount, rounds);
    } finally {
      pool.shutdownNow();
    }
  }
}
