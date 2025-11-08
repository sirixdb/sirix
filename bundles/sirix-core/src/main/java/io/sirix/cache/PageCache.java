package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.*;
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

  public PageCache(final int maxWeight) {
    final RemovalListener<PageReference, Page> removalListener = (PageReference key, Page page, RemovalCause cause) -> {
      assert key != null;
      key.setPage(null);
      assert page != null;

      if (page instanceof KeyValueLeafPage keyValueLeafPage) {
        assert keyValueLeafPage.getPinCount() == 0 : "Page must not be pinned: " + keyValueLeafPage.getPinCount();
        
        // Page handles its own cleanup
        LOGGER.trace("PageCache: Closing page {} and releasing segments, cause={}", 
                    key.getKey(), cause);
        LOGGER.debug("PageCache EVICT: closing page {} cause={}", keyValueLeafPage.getPageKey(), cause);
        keyValueLeafPage.close();
      }
    };

    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeight)
                    .weigher((PageReference _, Page value) -> {
                      if (value instanceof KeyValueLeafPage keyValueLeafPage) {
                        if (keyValueLeafPage.getPinCount() > 0) {
                          return 0; // Pinned pages have zero weight (won't be evicted)
                        } else {
                          // Use actual memory segment sizes for accurate tracking
                          return (int) keyValueLeafPage.getActualMemorySize();
                        }
                      } else {
                        return 1000; // Other page types use fixed weight
                      }
                    })
                    .removalListener(removalListener) // FIXED: Use removalListener for ALL removals
                    .recordStats() // Enable statistics for diagnostics
                    .build();
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
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void clear() {
    cache.invalidateAll();
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
      throw new IllegalArgumentException("KeyValueLeafPages must not be stored in PageCache! Use RecordPageCache instead.");
    }
    
    if (!(value instanceof RevisionRootPage) && !(value instanceof PathSummaryPage) && !(value instanceof PathPage)
        && !(value instanceof CASPage) && !(value instanceof NamePage)) {
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
  public void close() {
  }
  
  /**
   * Get cache statistics for diagnostics.
   */
  public CacheStatistics getStatistics() {
    com.github.benmanes.caffeine.cache.stats.CacheStats caffeineStats = cache.stats();
    
    int totalPages = 0;
    int pinnedPages = 0;
    long totalWeight = 0;
    long pinnedWeight = 0;
    
    for (Page page : cache.asMap().values()) {
      if (page instanceof KeyValueLeafPage kvPage) {
        totalPages++;
        long weight = kvPage.getActualMemorySize();
        totalWeight += weight;
        
        if (kvPage.getPinCount() > 0) {
          pinnedPages++;
          pinnedWeight += weight;
        }
      }
    }
    
    return new CacheStatistics(
        totalPages,
        pinnedPages,
        totalPages - pinnedPages,
        totalWeight,
        pinnedWeight,
        totalWeight - pinnedWeight,
        caffeineStats.hitCount(),
        caffeineStats.missCount(),
        caffeineStats.evictionCount()
    );
  }
  
  /**
   * Cache statistics for diagnostics.
   */
  public record CacheStatistics(
      int totalPages,
      int pinnedPages,
      int unpinnedPages,
      long totalWeightBytes,
      long pinnedWeightBytes,
      long unpinnedWeightBytes,
      long hitCount,
      long missCount,
      long evictionCount
  ) {
    @Override
    public String toString() {
      return String.format(
          "CacheStats[pages=%d (pinned=%d, unpinned=%d), " +
          "weight=%.2fMB (pinned=%.2fMB, unpinned=%.2fMB), " +
          "hits=%d, misses=%d, evictions=%d, hit-rate=%.1f%%]",
          totalPages, pinnedPages, unpinnedPages,
          totalWeightBytes / (1024.0 * 1024.0),
          pinnedWeightBytes / (1024.0 * 1024.0),
          unpinnedWeightBytes / (1024.0 * 1024.0),
          hitCount, missCount, evictionCount,
          (hitCount + missCount) > 0 ? 100.0 * hitCount / (hitCount + missCount) : 0.0
      );
    }
  }
}
