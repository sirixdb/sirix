package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-threaded invariant stress test for {@link ShardedPageCache} using a stub
 * {@link CacheablePage} that faithfully implements the guard/orphan/close state machine.
 *
 * <p>Invariants verified under concurrent get/getOrLoadAndGuard/put/remove/eviction pressure:
 * <ul>
 *   <li><b>Guard safety</b> — a page handed out by {@code getOrLoadAndGuard} is never closed
 *       while the caller still holds the guard (eviction must skip guarded pages; close must be
 *       deferred to the last {@code releaseGuard}).</li>
 *   <li><b>Guard-count sanity</b> — the guard count never goes negative and teardown never runs
 *       while guards are held.</li>
 *   <li><b>Weight accounting</b> — after quiescence the tracked weight equals exactly
 *       (page size × live mappings): no drift from racing charge/uncharge, and {@code clear()}
 *       returns the account to zero.</li>
 * </ul>
 */
final class ShardedPageCacheInvariantStressTest {

  private static final int THREADS = 6;
  private static final int KEYS = 64;
  private static final long PAGE_SIZE = 1_024;
  // Budget of 16 pages against a 64-key universe forces constant eviction pressure.
  private static final long MAX_WEIGHT = 16 * PAGE_SIZE;
  /**
   * Stress duration; overridable so mutation testing can use short runs
   * (-Dsirix.stress.run.millis=200) while regular CI keeps the full window.
   */
  private static final long RUN_MILLIS = Long.getLong("sirix.stress.run.millis", 1_500);

  /**
   * Stub page implementing the CacheablePage contract: acquireGuard fails once closed; close is
   * guard-aware (a guarded page is only marked for teardown; the last releaseGuard finishes it).
   * Contract violations are counted instead of thrown so the stress threads never die silently.
   */
  private static final class StubPage implements CacheablePage {
    private final long pageKey;
    private final AtomicLong closedWhileGuarded;
    private final AtomicLong negativeGuardCounts;

    private int guards;
    private boolean closeRequested;
    private boolean closed;
    private volatile boolean hot;
    private final AtomicInteger version = new AtomicInteger();

    StubPage(final long pageKey, final AtomicLong closedWhileGuarded, final AtomicLong negativeGuardCounts) {
      this.pageKey = pageKey;
      this.closedWhileGuarded = closedWhileGuarded;
      this.negativeGuardCounts = negativeGuardCounts;
    }

    @Override
    public long getActualMemorySize() {
      return PAGE_SIZE;
    }

    @Override
    public void markAccessed() {
      hot = true;
    }

    @Override
    public boolean isHot() {
      return hot;
    }

    @Override
    public void clearHot() {
      hot = false;
    }

    @Override
    public synchronized boolean acquireGuard() {
      if (closed) {
        return false;
      }
      guards++;
      return true;
    }

    @Override
    public synchronized void releaseGuard() {
      if (guards <= 0) {
        negativeGuardCounts.incrementAndGet();
        return;
      }
      guards--;
      if (guards == 0 && closeRequested && !closed) {
        teardown();
      }
    }

    @Override
    public synchronized int getGuardCount() {
      return guards;
    }

    @Override
    public synchronized boolean isClosed() {
      return closed;
    }

    @Override
    public synchronized void markOrphaned() {
      closeRequested = true;
    }

    @Override
    public synchronized void close() {
      if (closed) {
        return;
      }
      if (guards > 0) {
        // Guard-aware close: defer to the last releaseGuard.
        closeRequested = true;
        return;
      }
      teardown();
    }

    private void teardown() {
      if (guards > 0) {
        closedWhileGuarded.incrementAndGet();
      }
      closed = true;
    }

    @Override
    public void incrementVersion() {
      version.incrementAndGet();
    }

    @Override
    public long getPageKey() {
      return pageKey;
    }

    @Override
    public int getRevision() {
      return 1;
    }

    @Override
    public IndexType getIndexType() {
      return IndexType.DOCUMENT;
    }
  }

  @Test
  @Timeout(120)
  void guardAndWeightInvariantsHoldUnderConcurrentMixedOperations() throws InterruptedException {
    final ShardedPageCache<StubPage> cache = new ShardedPageCache<>(MAX_WEIGHT);
    final PageReference[] refs = new PageReference[KEYS];
    for (int i = 0; i < KEYS; i++) {
      refs[i] = new PageReference().setKey(i);
    }

    final AtomicLong closedWhileGuarded = new AtomicLong();
    final AtomicLong negativeGuardCounts = new AtomicLong();
    final AtomicLong guardedPageObservedClosed = new AtomicLong();
    final AtomicBoolean stop = new AtomicBoolean();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);

    for (int t = 0; t < THREADS; t++) {
      final long seed = 0xC0FFEE + t;
      pool.execute(() -> {
        final Random random = new Random(seed);
        await(start);
        while (!stop.get()) {
          final PageReference ref = refs[random.nextInt(KEYS)];
          switch (random.nextInt(10)) {
            case 0, 1, 2, 3, 4 -> { // guarded read/load (50%)
              final StubPage page = cache.getOrLoadAndGuard(ref,
                  key -> new StubPage(key.getKey(), closedWhileGuarded, negativeGuardCounts));
              if (page != null) {
                for (int i = 0; i < 20; i++) {
                  Thread.onSpinWait();
                }
                if (page.isClosed()) {
                  guardedPageObservedClosed.incrementAndGet();
                }
                page.releaseGuard();
              }
            }
            case 5, 6 -> // unguarded read (20%)
                cache.get(ref);
            case 7 -> // replace (10%)
                cache.put(ref, new StubPage(ref.getKey(), closedWhileGuarded, negativeGuardCounts));
            case 8 -> // remove (10%)
                cache.remove(ref);
            default -> // forced pressure eviction (10%)
                cache.evictUnderPressure();
          }
        }
      });
    }

    start.countDown();
    Thread.sleep(RUN_MILLIS);
    stop.set(true);
    pool.shutdown();
    assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "threads must terminate");

    assertEquals(0, guardedPageObservedClosed.get(),
        "a page was closed while a caller still held its guard (use-after-free hazard)");
    assertEquals(0, closedWhileGuarded.get(), "teardown ran while guards were held");
    assertEquals(0, negativeGuardCounts.get(), "releaseGuard was called more often than acquireGuard");

    // Weight accounting at quiescence: exactly PAGE_SIZE per live mapping, no drift.
    final long expectedWeight = cache.asMap().size() * PAGE_SIZE;
    assertEquals(expectedWeight, cache.getCurrentWeightBytes(),
        "tracked weight drifted from the live mappings (" + cache.asMap().size() + " pages)");

    cache.clear();
    assertEquals(0, cache.asMap().size(), "clear() must empty the cache");
    assertEquals(0, cache.getCurrentWeightBytes(), "clear() must return the weight account to zero");
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
