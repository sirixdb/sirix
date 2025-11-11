package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public final class RecordPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordPageCache.class);

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, KeyValueLeafPage> cache;

  public RecordPageCache(final int maxWeight) {
    final RemovalListener<PageReference, KeyValueLeafPage> removalListener =
        (PageReference key, KeyValueLeafPage page, RemovalCause cause) -> {
          // Handle ALL removals (eviction, invalidate, clear)
          assert key != null;
          key.setPage(null);
          assert page != null;
          
          // TODO: Will be replaced with version-based eviction logic
          // For now, close all pages on removal
          
          if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
            LOGGER.debug("RecordPageCache EVICT: closing page {} type={} rev={} cause={}", 
                page.getPageKey(), page.getIndexType(), page.getRevision(), cause);
          } else {
            LOGGER.trace("Closing page {} and releasing segments, cause={}", key.getKey(), cause);
          }
          
          page.close();
        };

    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeight)
                    .weigher((PageReference _, KeyValueLeafPage value) -> {
                      // TODO: Will be replaced with custom sharded cache
                      // Use actual memory segment sizes for tracking
                      return (int) value.getActualMemorySize();
                    })
                    .removalListener(removalListener)
                    .recordStats()
                    .build();
  }

  @Override
  public void clear() {
    // CRITICAL: Force pending async evictions to complete
    cache.cleanUp();
    
    // Invalidate all remaining entries
    cache.invalidateAll();
    
    // Final cleanup
    cache.cleanUp();
  }
  
  /**
   * Atomically unpin a page and update its weight in the cache.
   * 
   * @param key the page reference
   * @param trxId the transaction ID doing the unpin
   */
  // TODO: These methods will be replaced with guard-based lifecycle management
  // public void unpinAndUpdateWeight(PageReference key, int trxId) - REMOVED
  // public void pinAndUpdateWeight(PageReference key, int trxId) - REMOVED

  @Override
  public KeyValueLeafPage get(PageReference key) {
    return cache.getIfPresent(key);
  }

  @Override
  public KeyValueLeafPage get(PageReference key,
      BiFunction<? super PageReference, ? super KeyValueLeafPage, ? extends KeyValueLeafPage> mappingFunction) {
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void put(PageReference key, @NonNull KeyValueLeafPage value) {
    cache.put(key, value);
  }

  @Override
  public void putIfAbsent(PageReference key, KeyValueLeafPage value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public void putAll(Map<? extends PageReference, ? extends KeyValueLeafPage> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcurrentMap<PageReference, KeyValueLeafPage> asMap() {
    return cache.asMap();
  }

  @Override
  public Map<PageReference, KeyValueLeafPage> getAll(Iterable<? extends PageReference> keys) {
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
    long totalWeight = 0;
    
    for (KeyValueLeafPage page : cache.asMap().values()) {
      totalPages++;
      long weight = page.getActualMemorySize();
      totalWeight += weight;
    }
    
    // TODO: Update statistics after implementing guard-based system
    return new CacheStatistics(
        totalPages,
        0, // pinnedPages - will be replaced with guardCount tracking
        totalPages,
        totalWeight,
        0, // pinnedWeight
        totalWeight,
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
