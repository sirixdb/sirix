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
          // Handle ALL removals (eviction, invalidate, clear) - not just evictions
          assert key != null;
          key.setPage(null);
          assert page != null;
          
          // CRITICAL: Handle different removal causes appropriately
          // - EXPLICIT (TIL.put() removing page): Skip if pinned (will be closed by TIL)
          // - REPLACED (cache update): Skip if pinned (new version being cached)
          // - SIZE (eviction): Must be unpinned, always close
          // - COLLECTED (GC): Page key was GC'd, page might already be unreachable
          if (cause == RemovalCause.EXPLICIT || cause == RemovalCause.REPLACED) {
            if (page.getPinCount() > 0) {
              // Page still pinned - don't close, will be handled by transaction/TIL
              LOGGER.trace("RecordPage {} removed but NOT closed (cause={}, pinCount={})", 
                          key.getKey(), cause, page.getPinCount());
              return;
            }
            // Unpinned on explicit removal - close it
            LOGGER.trace("RecordPage {} removed and closing (cause={}, unpinned)", key.getKey(), cause);
          } else if (cause == RemovalCause.SIZE) {
            // SIZE evictions must have pinCount == 0
            assert page.getPinCount() == 0 : "Evicted page must not be pinned: " + page.getPinCount();
          }
          
          // Page handles its own cleanup
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
                      if (value.getPinCount() > 0) {
                        return 0; // Pinned pages have zero weight (won't be evicted)
                      }
                      // Use actual memory segment sizes for accurate tracking
                      return (int) value.getActualMemorySize();
                    })
                    .removalListener(removalListener) // FIXED: Use removalListener for ALL removals
                    .recordStats() // Enable statistics for diagnostics
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
  public void unpinAndUpdateWeight(PageReference key, int trxId) {
    // CRITICAL: Use computeIfPresent for atomic operation
    // The decrementPinCount() method is now synchronized, so it's safe from races
    cache.asMap().computeIfPresent(key, (k, page) -> {
      // decrementPinCount() is now synchronized and handles closed pages gracefully
      // It will return silently if the page is already closed or transaction never pinned it
      page.decrementPinCount(trxId);
      
      // Return same instance to trigger weight recalculation
      // Caffeine will call the weigher which checks pin count
      return page;
    });
  }
  
  /**
   * Atomically pin a page and update its weight in the cache.
   * 
   * @param key the page reference
   * @param trxId the transaction ID doing the pin
   */
  public void pinAndUpdateWeight(PageReference key, int trxId) {
    // CRITICAL: Use computeIfPresent for atomic operation
    // The incrementPinCount() method is now synchronized and will throw if page is closed
    cache.asMap().computeIfPresent(key, (k, page) -> {
      try {
        page.incrementPinCount(trxId);
      } catch (IllegalStateException e) {
        // Page was closed - this can happen if eviction occurred between get and pin
        // Return null to remove from cache (page is invalid)
        LOGGER.debug("Attempted to pin closed page {} - removing from cache", key.getKey());
        return null;
      }
      
      // Return same instance to trigger weight recalculation
      // Caffeine will call the weigher which checks pin count
      return page;
    });
  }

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
    int pinnedPages = 0;
    long totalWeight = 0;
    long pinnedWeight = 0;
    
    for (KeyValueLeafPage page : cache.asMap().values()) {
      totalPages++;
      long weight = page.getActualMemorySize();
      totalWeight += weight;
      
      if (page.getPinCount() > 0) {
        pinnedPages++;
        pinnedWeight += weight;
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
