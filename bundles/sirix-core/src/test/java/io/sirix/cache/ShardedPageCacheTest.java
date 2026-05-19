package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ShardedPageCache} eviction protections:
 * <ul>
 * <li>{@link ShardedPageCache#removePage} — instance-granular removal without close, used by the
 * transaction-intent log to shield a dirty, transaction-private page from eviction.</li>
 * <li>The HOT-bit second chance added to {@link ShardedPageCache#evictUnderPressure()} so it
 * matches {@code ClockSweeper.sweep()} / {@code evictIfOverBudget()}.</li>
 * </ul>
 */
@DisplayName("ShardedPageCache eviction-protection Tests")
class ShardedPageCacheTest {

  private static final long PAGE_BYTES = 1024L;

  private static PageReference keyFor(long k) {
    return new PageReference().setKey(k).setDatabaseId(0).setResourceId(0);
  }

  @Test
  @DisplayName("removePage takes a page out of the cache by instance without closing it")
  void removePageEvictsByInstanceWithoutClosing() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference key = keyFor(1);
    final FakePage page = new FakePage(1);
    cache.put(key, page);
    assertSame(page, cache.get(key), "page should be cached after put");

    cache.removePage(page);

    assertNull(cache.get(key), "page must be gone from the cache");
    assertFalse(page.isClosed(), "removePage must NOT close the page — the caller keeps ownership");
    assertEquals(0L, cache.getCurrentWeightBytes(), "tracked weight must drop back to zero");
  }

  @Test
  @DisplayName("removePage leaves other cached pages untouched and is a no-op when absent")
  void removePageNoOpWhenAbsent() {
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(1024L * 1024L);
    final PageReference keptKey = keyFor(1);
    final FakePage kept = new FakePage(1);
    cache.put(keptKey, kept);

    cache.removePage(new FakePage(99));   // not cached — must not throw or disturb anything

    assertSame(kept, cache.get(keptKey), "an unrelated cached page must be untouched");
    assertEquals(PAGE_BYTES, cache.getCurrentWeightBytes());
  }

  @Test
  @DisplayName("evictUnderPressure gives every HOT page a one-pass second chance")
  void evictUnderPressureHonoursHotBit() {
    // Budget 8 KiB; 7 pages of 1 KiB = 7 KiB. 7 KiB <= 1.1*budget so put() does not pre-evict,
    // and 7 KiB > 0.75*budget (= 6 KiB) so evictUnderPressure is active.
    final ShardedPageCache<FakePage> cache = new ShardedPageCache<>(8192L);
    final FakePage[] pages = new FakePage[7];
    for (int i = 0; i < pages.length; i++) {
      pages[i] = new FakePage(i);
      pages[i].markAccessed();   // sets the HOT bit
      cache.put(keyFor(i), pages[i]);
    }
    for (final FakePage p : pages) {
      assertTrue(p.isHot() && !p.isClosed(), "precondition: all pages hot and live");
    }

    // First pass: every page is HOT → all survive, all HOT bits cleared (second chance consumed).
    cache.evictUnderPressure();
    for (final FakePage p : pages) {
      assertTrue(cache.containsPage(p), "a HOT page must survive the first evictUnderPressure pass");
      assertFalse(p.isHot(), "the HOT bit must be cleared (second chance consumed)");
    }

    // Second pass: bits are cold now → eviction proceeds down to the 3/4-budget target.
    cache.evictUnderPressure();
    int closed = 0;
    for (final FakePage p : pages) {
      if (p.isClosed()) {
        closed++;
      }
    }
    assertTrue(closed >= 1, "with no HOT reprieve, evictUnderPressure must now evict a page");
    assertTrue(cache.getCurrentWeightBytes() <= 8192L * 3 / 4,
        "evictUnderPressure must bring the cache down to its 3/4 target");
  }

  /** Minimal {@link CacheablePage} test double — fixed 1 KiB weight, software HOT bit + guards. */
  private static final class FakePage implements CacheablePage {
    private final long pageKey;
    private volatile boolean hot;
    private volatile boolean closed;
    private final AtomicInteger guards = new AtomicInteger();

    FakePage(long pageKey) {
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
      guards.incrementAndGet();
      return true;
    }

    @Override
    public void releaseGuard() {
      guards.decrementAndGet();
    }

    @Override
    public int getGuardCount() {
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
      // test double: version drift is irrelevant to these tests
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
  }
}
