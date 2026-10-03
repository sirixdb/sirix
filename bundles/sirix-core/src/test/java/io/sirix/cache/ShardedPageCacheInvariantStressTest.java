package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-threaded invariant stress test for {@link ShardedPageCache} using a stub
 * {@link CacheablePage} that faithfully implements the guard/orphan/close state machine.
 *
 * <p>
 * Invariants verified under concurrent get/getOrLoadAndGuard/put/remove/eviction pressure:
 * <ul>
 * <li><b>Guard safety</b> — a page handed out by {@code getOrLoadAndGuard} is never closed while
 * the caller still holds the guard (eviction must skip guarded pages; close must be deferred to the
 * last {@code releaseGuard}).</li>
 * <li><b>Guard-count sanity</b> — the guard count never goes negative and teardown never runs while
 * guards are held.</li>
 * <li><b>Weight accounting</b> — after quiescence the tracked weight equals exactly (page size ×
 * live mappings): no drift from racing charge/uncharge, and {@code clear()} returns the account to
 * zero.</li>
 * <li><b>Key ownership</b> — every live mapping's page remembers exactly that mapping's key, the
 * invariant {@link ShardedPageCache#removePage} reads in the negative direction when a page that
 * records its key has none. A second case drives real {@link HOTLeafPage}s, the only page type that
 * takes that fast path, so the diagnostic guarding it is exercised against concurrent admission,
 * detachment and instance removal rather than only single-threaded.</li>
 * </ul>
 */
final class ShardedPageCacheInvariantStressTest {

  private static final int THREADS = 6;
  private static final int KEYS = 64;
  private static final long PAGE_SIZE = 1_024;
  // Budget of 16 pages against a 64-key universe forces constant eviction pressure.
  private static final long MAX_WEIGHT = 16 * PAGE_SIZE;
  private static final int HOT_KEYS = 16;
  private static final long HOT_MAX_WEIGHT = 16L * 1024L * 1024L;
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
    private volatile PageReference lastCacheKey;
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
    public PageReference lastCacheKey() {
      return lastCacheKey;
    }

    @Override
    public void setLastCacheKey(final PageReference cacheKey) {
      this.lastCacheKey = cacheKey;
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
  void guardAndWeightInvariantsHoldUnderConcurrentMixedOperations() throws Exception {
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
    final List<Future<?>> workers = new ArrayList<>(THREADS);

    for (int t = 0; t < THREADS; t++) {
      final long seed = 0xC0FFEE + t;
      workers.add(pool.submit(() -> {
        final Random random = new Random(seed);
        await(start);
        while (!stop.get()) {
          final PageReference ref = refs[random.nextInt(KEYS)];
          switch (random.nextInt(12)) {
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
            case 5, 6 -> // unguarded read
              cache.get(ref);
            case 7 -> // replace
              cache.put(ref, new StubPage(ref.getKey(), closedWhileGuarded, negativeGuardCounts));
            case 8 -> // remove by key
              cache.remove(ref);
            case 9 -> // remove and hand the page to the caller
              cache.removeAndGet(ref);
            case 10 -> { // instance removal, as the intent log takes a dirty page private
              final StubPage page = cache.get(ref);
              if (page != null) {
                cache.removePage(page);
                cache.removePage(page);
              }
            }
            default -> // forced pressure eviction
              cache.evictUnderPressure();
          }
        }
      }));
    }

    start.countDown();
    Thread.sleep(RUN_MILLIS);
    stop.set(true);
    pool.shutdown();
    assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "threads must terminate");
    for (final Future<?> worker : workers) {
      worker.get(10, TimeUnit.SECONDS);
    }

    assertEquals(0, guardedPageObservedClosed.get(),
        "a page was closed while a caller still held its guard (use-after-free hazard)");
    assertEquals(0, closedWhileGuarded.get(), "teardown ran while guards were held");
    assertEquals(0, negativeGuardCounts.get(), "releaseGuard was called more often than acquireGuard");

    // Weight accounting at quiescence: exactly PAGE_SIZE per live mapping, no drift.
    final long expectedWeight = cache.asMap().size() * PAGE_SIZE;
    assertEquals(expectedWeight, cache.getCurrentWeightBytes(),
        "tracked weight drifted from the live mappings (" + cache.asMap().size() + " pages)");

    // Key ownership at quiescence: a surviving mapping's page must remember exactly that mapping's
    // key. A forgotten or foreign key is the state removePage's no-scan fast path misreads.
    for (final var entry : cache.asMap().entrySet()) {
      assertEquals(entry.getKey(), entry.getValue().lastCacheKey(),
          "a live mapping's page must remember its own mapping key");
    }

    cache.clear();
    assertEquals(0, cache.asMap().size(), "clear() must empty the cache");
    assertEquals(0, cache.getCurrentWeightBytes(), "clear() must return the weight account to zero");
  }

  /**
   * HOT leaves are the only page type whose recorded cache key {@code removePage} trusts, so they are
   * the only way to drive its no-scan fast path. The pool holds one instance per key: a cache key is
   * derived from a page's durable offset, so one page is admitted under one key, and every admission
   * in this case re-publishes the SAME instance — a page the cache never has to retire. The budget is
   * far above the pool's total charge and this case never evicts, so no pooled leaf is ever closed
   * and the fixed pool is allocated exactly once.
   */
  @Test
  @Timeout(120)
  void hotLeafKeyOwnershipHoldsUnderConcurrentAdmissionDetachAndInstanceRemoval() throws Exception {
    try (Arena arena = Arena.ofConfined()) {
      final ShardedPageCache<HOTLeafPage> cache = new ShardedPageCache<>(HOT_MAX_WEIGHT);
      final PageReference[] refs = new PageReference[HOT_KEYS];
      final HOTLeafPage[] leaves = new HOTLeafPage[HOT_KEYS];
      for (int i = 0; i < HOT_KEYS; i++) {
        refs[i] = new PageReference().setKey(i);
        leaves[i] = new HOTLeafPage(i, 1, IndexType.PROJECTION, arena.allocate(HOTLeafPage.DEFAULT_SIZE), null,
            new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
        cache.put(refs[i], leaves[i]);
      }

      final AtomicBoolean stop = new AtomicBoolean();
      final CountDownLatch start = new CountDownLatch(1);
      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      final List<Future<?>> workers = new ArrayList<>(THREADS);

      for (int t = 0; t < THREADS; t++) {
        final long seed = 0xBEEFL + t;
        workers.add(pool.submit(() -> {
          final Random random = new Random(seed);
          await(start);
          while (!stop.get()) {
            final int index = random.nextInt(HOT_KEYS);
            final PageReference ref = refs[index];
            final HOTLeafPage leaf = leaves[index];
            switch (random.nextInt(10)) {
              case 0, 1 -> // re-publish the same instance
                cache.put(ref, leaf);
              case 2 -> // remove by key
                cache.remove(ref);
              case 3 -> // remove and hand the page to the caller
                cache.removeAndGet(ref);
              case 4, 5 -> {
                // The repeat takes the no-scan fast path: the first call forgot the key.
                cache.removePage(leaf);
                cache.removePage(leaf);
              }
              case 6 -> cache.get(ref);
              case 7 -> cache.containsPage(leaf);
              case 8 -> {
                final HOTLeafPage guarded = cache.getAndGuard(ref);
                if (guarded != null) {
                  guarded.releaseGuard();
                }
              }
              default -> {
                final HOTLeafPage guarded = cache.getOrLoadAndGuard(ref, _ -> leaf);
                if (guarded != null) {
                  guarded.releaseGuard();
                }
              }
            }
          }
          return null;
        }));
      }

      start.countDown();
      Thread.sleep(RUN_MILLIS);
      stop.set(true);
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "threads must terminate");
      for (final Future<?> worker : workers) {
        // The ownership diagnostic firing on a legitimate interleaving surfaces here.
        worker.get(10, TimeUnit.SECONDS);
      }

      long expectedWeight = 0L;
      for (int i = 0; i < HOT_KEYS; i++) {
        final HOTLeafPage leaf = leaves[i];
        assertFalse(leaf.isClosed(), "removePage leaves page ownership with the caller — no pooled leaf may close");
        assertEquals(0, leaf.getGuardCount(), "every guard this case took must have been released");
        if (cache.asMap().get(refs[i]) == leaf) {
          assertSame(refs[i], leaf.lastCacheKey(), "a mapped HOT leaf must remember its own mapping key");
          expectedWeight += leaf.getActualMemorySize() + leaf.estimatedRetainedHeapBytes();
        } else {
          assertNull(leaf.lastCacheKey(), "an unmapped HOT leaf must remember no key");
        }
      }
      assertEquals(expectedWeight, cache.getCurrentWeightBytes(),
          "tracked weight drifted from the live HOT mappings (" + cache.asMap().size() + " pages)");

      cache.clear();
      assertEquals(0, cache.asMap().size(), "clear() must empty the cache");
      assertEquals(0, cache.getCurrentWeightBytes(), "clear() must return the weight account to zero");
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
