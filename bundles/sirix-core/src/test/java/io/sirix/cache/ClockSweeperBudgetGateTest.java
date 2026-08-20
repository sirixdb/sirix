package io.sirix.cache;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.index.IndexType;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for the {@link ClockSweeper} budget gate.
 *
 * <p>The sweeper used to run its clock unconditionally on every cycle: any page that was neither
 * HOT nor guarded was evicted regardless of how much headroom the cache budget had. A cache
 * holding 128 MB against an 8 GB budget was therefore emptied ~10 % per cycle, so every analytical
 * scan re-read and re-decompressed the whole resource. Measured on a 109 MB / ~4,000-page store,
 * each pass took ~4,000 cache misses and ~4,100 evictions with <em>zero</em> hits; gating the
 * sweep on a high-water mark took the same query from ~4,180 ms to ~240 ms.
 *
 * <p>These tests pin both directions, because a gate that never evicts would be just as wrong as
 * one that always does:
 * <ul>
 * <li>well under budget → the sweeper must leave the working set alone;</li>
 * <li>at or above the high-water mark → the sweeper must still reclaim.</li>
 * </ul>
 *
 * <p>Every case drives {@link ClockSweeper#sweep()} directly rather than starting the thread, so
 * the result does not depend on sweep-interval timing.
 */
@DisplayName("ClockSweeper budget-gate regression Tests")
class ClockSweeperBudgetGateTest {

  /** Matches {@link FakePage#getActualMemorySize()}, so budgets can be expressed in pages. */
  private static final long PAGE_BYTES = 1024L;

  /** The sweeper's clock scans ~10 % of the map per cycle, so use enough pages to see it move. */
  private static final int PAGE_COUNT = 200;

  private static PageReference keyFor(final long k) {
    return new PageReference().setKey(k).setDatabaseId(0).setResourceId(0);
  }

  /** A global sweeper (databaseId/resourceId 0) — the configuration Databases actually installs. */
  private static ClockSweeper globalSweeperFor(final ShardedPageCache<FakePage> cache, final PageReference probe) {
    final RevisionEpochTracker epochTracker = new RevisionEpochTracker(64);
    epochTracker.setLastCommittedRevision(1);
    return new ClockSweeper(cache.getShard(probe), cache, epochTracker, 1, 0, 0, 0);
  }

  /**
   * Fill {@code cache} with unguarded, non-HOT pages — the exact state the old sweeper evicted.
   *
   * @return the number of pages that landed in the shard the sweeper will scan
   */
  private static int fillUnguardedAndCold(final ShardedPageCache<FakePage> cache,
      final ShardedPageCache.Shard<FakePage> shard) {
    for (int i = 0; i < PAGE_COUNT; i++) {
      final FakePage page = new FakePage(i);
      cache.put(keyFor(i), page);
      // put() marks pages accessed (HOT). Clear it so the HOT second chance cannot be what
      // protects them: this test is about the budget gate, not about the second chance.
      page.clearHot();
    }
    return shard.map.size();
  }

  @Test
  @DisplayName("well under budget, a sweep evicts nothing")
  void sweepIsANoOpWhenFarUnderBudget() {
    // 200 pages of 1 KiB against a 64 MiB budget — ~0.3 % full.
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(64L * 1024L * 1024L);
    final PageReference probe = keyFor(0);
    final ShardedPageCache.Shard<FakePage> shard = cache.getShard(probe);
    final ClockSweeper sweeper = globalSweeperFor(cache, probe);

    final int before = fillUnguardedAndCold(cache, shard);
    final long weightBefore = cache.getCurrentWeightBytes();

    // Several cycles: the clock advances ~10 % per sweep, so one cycle alone could miss pages.
    for (int i = 0; i < 20; i++) {
      sweeper.sweep();
    }

    assertEquals(0L, sweeper.getPagesEvicted(),
        "a cache at ~0.3 % of budget has nothing to reclaim — sweeping it only forces readers to "
            + "re-read and re-decompress pages they are about to ask for again");
    assertEquals(before, shard.map.size(), "no cached page may be dropped while under budget");
    assertEquals(weightBefore, cache.getCurrentWeightBytes(), "tracked weight must be unchanged");
  }

  @Test
  @DisplayName("at the high-water mark, a sweep still reclaims")
  void sweepStillEvictsWhenAtHighWater() {
    // Budget sized so 200 pages sit above the 80 % high-water mark: 200 KiB of pages against a
    // 200 KiB budget is 100 % full.
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(PAGE_COUNT * PAGE_BYTES);
    final PageReference probe = keyFor(0);
    final ShardedPageCache.Shard<FakePage> shard = cache.getShard(probe);
    final ClockSweeper sweeper = globalSweeperFor(cache, probe);

    final int before = fillUnguardedAndCold(cache, shard);

    for (int i = 0; i < 20; i++) {
      sweeper.sweep();
    }

    assertTrue(sweeper.getPagesEvicted() > 0,
        "at/over the high-water mark the sweeper must still reclaim — the gate bounds memory, it "
            + "does not disable eviction");
    assertTrue(shard.map.size() < before, "reclaiming must actually remove entries from the shard");
  }

  @Test
  @DisplayName("guarded pages survive a sweep even under pressure")
  void guardedPagesAreNeverEvicted() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(PAGE_COUNT * PAGE_BYTES);
    final PageReference probe = keyFor(0);
    final ShardedPageCache.Shard<FakePage> shard = cache.getShard(probe);
    final ClockSweeper sweeper = globalSweeperFor(cache, probe);

    fillUnguardedAndCold(cache, shard);

    // Guard one page: a reader is using it right now.
    final PageReference guardedKey = keyFor(0);
    final FakePage guarded = cache.get(guardedKey);
    assertTrue(guarded.acquireGuard(), "test setup: the page must be guardable");

    for (int i = 0; i < 20; i++) {
      sweeper.sweep();
    }

    assertFalse(guarded.isClosed(), "a guarded page must never be closed by the sweeper");
    assertTrue(cache.get(guardedKey) == guarded, "a guarded page must stay in the cache");
  }

  @Test
  @DisplayName("an unbounded cache (budget <= 0) is never swept")
  void unboundedCacheIsNeverSwept() {
    // ShardedPageCache reads a non-positive maximum as "unbounded"; with no budget to defend
    // there is nothing for the sweeper to reclaim toward.
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(0L);
    final PageReference probe = keyFor(0);
    final ShardedPageCache.Shard<FakePage> shard = cache.getShard(probe);
    final ClockSweeper sweeper = globalSweeperFor(cache, probe);

    final int before = fillUnguardedAndCold(cache, shard);

    for (int i = 0; i < 20; i++) {
      sweeper.sweep();
    }

    assertEquals(0L, sweeper.getPagesEvicted(), "an unbounded cache must never be swept");
    assertEquals(before, shard.map.size(), "no page may be dropped from an unbounded cache");
  }

  @Test
  @DisplayName("a guard arriving after the sweep check owns deferred close, not the cache mapping")
  void lateGuardDoesNotLeaveOrphanedMappingCharged() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(PAGE_BYTES);
    final PageReference key = keyFor(999);
    final FakePage page = new FakePage(999);
    cache.put(key, page);
    page.clearHot();
    page.acquireGuardAfterNextZeroObservation();

    globalSweeperFor(cache, key).sweep();

    assertEquals(0L, cache.size(), "the sweeper must detach the mapping despite the late guard");
    assertEquals(0L, cache.getCurrentWeightBytes(), "a holder-owned orphan consumes no cache budget");
    assertTrue(page.isOrphanedForTest());
    assertFalse(page.isClosed(), "physical close must wait for the late guard holder");
    assertEquals(0, page.versionForTest(), "the sweeper must not drift the version under a live holder");

    page.releaseGuard();
    assertTrue(page.isClosed(), "the final guard release must complete the deferred close");
  }

  /** Minimal {@link CacheablePage} test double — fixed 1 KiB weight, software HOT bit + guards. */
  private static final class FakePage implements CacheablePage {
    private final long pageKey;
    private volatile boolean hot;
    private volatile boolean closed;
    private volatile boolean orphaned;
    private volatile boolean injectGuardOnNextCount;
    private final AtomicInteger guards = new AtomicInteger();
    private final AtomicInteger version = new AtomicInteger();

    FakePage(final long pageKey) {
      this.pageKey = pageKey;
    }

    @Override
    public long getActualMemorySize() {
      return PAGE_BYTES;
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
    public boolean acquireGuard() {
      if (closed || (orphaned && guards.get() <= 0)) {
        return false;
      }
      guards.incrementAndGet();
      return true;
    }

    @Override
    public void markOrphaned() {
      orphaned = true;
    }

    @Override
    public void releaseGuard() {
      final int remaining = guards.decrementAndGet();
      if (remaining < 0) {
        throw new IllegalStateException("guard underflow");
      }
      if (remaining == 0 && orphaned) {
        close();
      }
    }

    @Override
    public int getGuardCount() {
      if (injectGuardOnNextCount) {
        injectGuardOnNextCount = false;
        guards.incrementAndGet();
        return 0;
      }
      return guards.get();
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    public void close() {
      if (guards.get() == 0) {
        closed = true;
      }
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
      return 0;
    }

    @Override
    public IndexType getIndexType() {
      return IndexType.DOCUMENT;
    }

    void acquireGuardAfterNextZeroObservation() {
      injectGuardOnNextCount = true;
    }

    boolean isOrphanedForTest() {
      return orphaned;
    }

    int versionForTest() {
      return version.get();
    }
  }
}
