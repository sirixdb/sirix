package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.CASPage;
import io.sirix.page.DeweyIDPage;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.IndirectPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.PathSummaryPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.ValidTimeIndexPage;
import io.sirix.page.VectorPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public final class PageCache implements Cache<PageReference, Page> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PageCache.class);

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, Page> cache;

  /** System property to override the {@link PageCache} entry-count cap. */
  public static final String MAX_ENTRIES_PROPERTY = "sirix.cache.page.max.entries";

  /** Default entry-count cap. */
  public static final int DEFAULT_MAX_ENTRIES = 50_000;

  /**
   * Create a PageCache with an explicit entry-count cap.
   *
   * @param maxEntries the maximum number of cached pages; must be positive.
   */
  public PageCache(final int maxEntries) {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be > 0, got " + maxEntries);
    }
    final RemovalListener<PageReference, Page> removalListener = (PageReference key, Page page, RemovalCause cause) -> {
      assert key != null;
      key.setPage(null);
      assert page != null;

      if (page instanceof KeyValueLeafPage keyValueLeafPage) {
        if (keyValueLeafPage.getGuardCount() > 0) {
          LOGGER.trace("PageCache: Page {} has active guards ({}), skipping close (cause={})", key.getKey(),
              keyValueLeafPage.getGuardCount(), cause);
          return;
        }
        keyValueLeafPage.close();
      }
    };

    // Switched from byte-budget (maximumWeight + uniform weight=1000) to a
    // straightforward entry-count cap. The previous configuration translated a
    // 500 MB byte budget into ~500 k entries, well above the working set of
    // any realistic bitemporal workload — soak runs accumulated one new
    // metadata page per revision (NamePage, PathSummaryPage, IndirectPage)
    // and never approached eviction. Computing real per-page byte size for
    // JVM-heap-backed metadata pages (nested fastutil maps, references) is
    // brittle without instrumentation, so a count cap is both simpler and
    // more predictable for operators (one number, easy to size).
    cache = Caffeine.newBuilder().maximumSize(maxEntries).removalListener(removalListener).recordStats().build();
    LOGGER.info("PageCache created with maxEntries: {}", maxEntries);
  }

  /**
   * Legacy constructor accepting a byte budget. The byte input is ignored — the
   * eviction policy is now count-based ({@link #DEFAULT_MAX_ENTRIES}, override
   * via {@value #MAX_ENTRIES_PROPERTY}). Kept so existing call sites (e.g.
   * {@code BufferManagerImpl}) compile without change.
   *
   * @param maxWeight legacy byte budget — ignored. Set the system property to
   *     change the cap.
   */
  public PageCache(final long maxWeight) {
    this(resolveMaxEntries(maxWeight));
  }

  private static int resolveMaxEntries(final long maxWeightBytes) {
    // System property overrides everything. This is the operator-facing dial.
    final String prop = System.getProperty(MAX_ENTRIES_PROPERTY);
    if (prop != null && !prop.isEmpty()) {
      try {
        final int parsed = Integer.parseInt(prop.trim());
        if (parsed > 0) {
          return parsed;
        }
      } catch (final NumberFormatException ignored) {
        // fall through
      }
    }
    // Legacy byte-budget input: ignore and use the default. The previous
    // byte-budget interpretation produced ~500 k effective entries on a 500 MB
    // budget (uniform weight = 1000), which never evicted under any realistic
    // soak. The default of {@link #DEFAULT_MAX_ENTRIES} is large enough for the
    // hot working set of a typical bitemporal workload (a few revisions of
    // navigation metadata per index type, ~thousands of entries) while leaving
    // headroom for analytical scans. Operators wanting a different size set
    // {@value #MAX_ENTRIES_PROPERTY}.
    return DEFAULT_MAX_ENTRIES;
  }

  @Override
  public ConcurrentMap<PageReference, Page> asMap() {
    return cache.asMap();
  }

  @Override
  public void putIfAbsent(PageReference key, Page value) {
    assert !(value instanceof KeyValueLeafPage) || !value.isClosed();
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public Page get(PageReference key, BiFunction<? super PageReference, ? super Page, ? extends Page> mappingFunction) {
    // Fast path. asMap().compute() locks the key's bin AND invokes the mapping function
    // unconditionally — including on a hit. Every caller here is a pure load-if-absent
    // ((_, _) -> pageReader.read(...)), so going through compute() re-read and re-deserialized a
    // page that was already cached, then overwrote the cache entry with the fresh copy: the cache
    // kept the page alive but never saved the I/O it exists to save. getIfPresent is lock-free.
    final Page hit = cache.getIfPresent(key);
    if (hit != null && !hit.isClosed()) {
      return hit;
    }
    // Miss (or a page closed by an in-flight eviction, which must not be handed out): compute
    // under the bin lock so concurrent misses on the same reference read once. The type policy is
    // enforced here exactly as in put() — a disallowed page never enters the map, but the computed
    // page is still returned to the caller.
    final Page[] computed = new Page[1];
    final Page cached = cache.asMap().compute(key, (k, v) -> {
      if (v != null && !v.isClosed()) {
        computed[0] = v;
        return v;
      }
      final Page page = mappingFunction.apply(k, v);
      computed[0] = page;
      return isCacheablePageType(page) ? page : null;
    });
    return cached != null ? cached : computed[0];
  }

  /**
   * The revision root and its index-root children are per-revision mutable metadata that must not
   * be shared via this cache — ONE policy consulted by both {@link #put} and the compute path of
   * {@link #get(PageReference, BiFunction)}.
   */
  private static boolean isCacheablePageType(final Page page) {
    return page != null && !(page instanceof RevisionRootPage) && !isIndexRootPage(page);
  }

  /**
   * Whether the page is one of the {@link RevisionRootPage}'s direct children whose OWN references
   * are the roots of the per-revision index trees.
   *
   * <p>These are not merely "mutable". Index creation decides whether a tree already exists by
   * probing whether the corresponding slot carries a key, so one shared instance makes a writer
   * observe another transaction's half-built root and skip creating its own — surfacing far away as
   * an index that silently lacks entries. On the read side, sharing one instance across revisions
   * lets a time-travel read of revision N follow revision N+1's index root and see entries that did
   * not exist yet.
   *
   * <p>The list must stay in step with {@code RevisionRootPage}'s {@code *_REFERENCE_OFFSET} slots:
   * a new index type added there and forgotten here is silently mis-shared.
   */
  private static boolean isIndexRootPage(final Page page) {
    return page instanceof PathSummaryPage || page instanceof NamePage || page instanceof CASPage
        || page instanceof PathPage || page instanceof DeweyIDPage || page instanceof VectorPage
        || page instanceof ProjectionIndexPage || page instanceof ValidTimeIndexPage;
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  /**
   * Evict all HOTLeafPages from the cache under memory pressure. Called by the
   * allocator's PressureListener when off-heap budget is exhausted — HOTLeafPages
   * each hold a 65 KB MemorySegment that counts against the global budget. The
   * removal listener fires synchronously on invalidate, closing each page and
   * returning its slot to the allocator.
   */
  public void evictHOTLeafPages() {
    for (final var entry : cache.asMap().entrySet()) {
      if (entry.getValue() instanceof HOTLeafPage) {
        cache.invalidate(entry.getKey());
      }
    }
  }

  @Override
  public Page get(PageReference key) {
    var page = cache.getIfPresent(key);
    return page;
  }

  @Override
  public void put(PageReference key, Page value) {
    assert !(value instanceof KeyValueLeafPage);

    // PageCache is for metadata/indirect pages ONLY, not KeyValueLeafPages
    // KeyValueLeafPages should go in RecordPageCache or RecordPageFragmentCache
    if (value instanceof KeyValueLeafPage) {
      throw new IllegalArgumentException(
          "KeyValueLeafPages must not be stored in PageCache! Use RecordPageCache instead.");
    }

    if (isCacheablePageType(value)) {
      assert key.getKey() != Constants.NULL_ID_LONG;
      cache.put(key, value);
    }
  }

  @Override
  public void putAll(Map<? extends PageReference, ? extends Page> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<PageReference, Page> getAll(Iterable<? extends PageReference> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(PageReference key) {
    cache.invalidate(key);
  }

  @Override
  public void cleanUp() {
    cache.cleanUp();
  }

  @Override
  public void close() {}

  /**
   * Get cache statistics for diagnostics.
   */
  public CacheStatistics getStatistics() {
    com.github.benmanes.caffeine.cache.stats.CacheStats caffeineStats = cache.stats();

    int totalPages = 0;
    long totalWeight = 0;

    for (Page page : cache.asMap().values()) {
      if (page instanceof KeyValueLeafPage kvPage) {
        totalPages++;
        long weight = kvPage.getActualMemorySize();
        totalWeight += weight;
        // Guard counts available via kvPage.getGuardCount() for per-page protection tracking
      }
    }

    return new CacheStatistics(totalPages, 0, // pinnedPages
        totalPages, totalWeight, 0, // pinnedWeight
        totalWeight, caffeineStats.hitCount(), caffeineStats.missCount(), caffeineStats.evictionCount());
  }

  /**
   * Cache statistics for diagnostics.
   */
  public record CacheStatistics(int totalPages, int pinnedPages, int unpinnedPages, long totalWeightBytes,
      long pinnedWeightBytes, long unpinnedWeightBytes, long hitCount, long missCount, long evictionCount) {
    @Override
    public String toString() {
      return String.format(
          "CacheStats[pages=%d (pinned=%d, unpinned=%d), " + "weight=%.2fMB (pinned=%.2fMB, unpinned=%.2fMB), "
              + "hits=%d, misses=%d, evictions=%d, hit-rate=%.1f%%]",
          totalPages, pinnedPages, unpinnedPages, totalWeightBytes / (1024.0 * 1024.0),
          pinnedWeightBytes / (1024.0 * 1024.0), unpinnedWeightBytes / (1024.0 * 1024.0), hitCount, missCount,
          evictionCount, (hitCount + missCount) > 0
              ? 100.0 * hitCount / (hitCount + missCount)
              : 0.0);
    }
  }
}
