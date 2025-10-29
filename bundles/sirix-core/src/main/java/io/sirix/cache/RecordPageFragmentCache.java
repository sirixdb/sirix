package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * Cache for temporary KeyValueLeafPage fragments loaded during page reconstruction.
 * Fragments are aggressively evicted since they can be reconstructed from disk.
 * Separate from RecordPageCache to avoid mixing temporary vs permanent pages.
 */
public final class RecordPageFragmentCache implements Cache<PageReference, KeyValueLeafPage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordPageFragmentCache.class);

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, KeyValueLeafPage> cache;

  public static final java.util.concurrent.atomic.AtomicLong TOTAL_EVICTIONS = new java.util.concurrent.atomic.AtomicLong();
  public static final java.util.concurrent.atomic.AtomicLong SKIPPED_EXPLICIT = new java.util.concurrent.atomic.AtomicLong();
  
  public RecordPageFragmentCache(final int maxWeight) {
    final RemovalListener<PageReference, KeyValueLeafPage> removalListener =
        (PageReference key, KeyValueLeafPage page, RemovalCause cause) -> {
          assert key != null;
          key.setPage(null);
          assert page != null;
          
          // CRITICAL FIX: For EXPLICIT removal, check if page is pinned
          // - Pinned pages: Skip closing (still in use by transaction)
          // - Unpinned pages: MUST close (being removed from cache, won't be closed elsewhere)
          if (cause == RemovalCause.EXPLICIT || cause == RemovalCause.REPLACED) {
            if (page.getPinCount() > 0) {
              // Still pinned - don't close, transaction will handle it
              SKIPPED_EXPLICIT.incrementAndGet();
              LOGGER.trace("Fragment {} removed but NOT closed (cause={}, pinCount={})", 
                          key.getKey(), cause, page.getPinCount());
              return;
            }
            // Unpinned - close it now, this is the last chance
            LOGGER.trace("Fragment {} removed and closing (cause={}, unpinned)", key.getKey(), cause);
          }
          
          // All removals (SIZE eviction, EXPLICIT if unpinned) should close
          assert page.getPinCount() == 0 : "Fragment page must not be pinned: " + page.getPinCount() 
              + " (pins by transaction: " + page.getPinCountByTransaction() + ")";
          
          // Track evictions
          long evictions = TOTAL_EVICTIONS.incrementAndGet();
          
          // Log every 100th eviction
          if (evictions % 100 == 0) {
            LOGGER.info("RecordPageFragmentCache: {} SIZE evictions, {} EXPLICIT skipped (pinned)", 
                        evictions, SKIPPED_EXPLICIT.get());
          }
          
          page.close();
        };

    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeight)
                    .weigher((PageReference _, KeyValueLeafPage value) -> {
                      if (value.getPinCount() > 0) {
                        return 0; // Pinned fragments have zero weight
                      }
                      return (int) value.getActualMemorySize();
                    })
                    .scheduler(Scheduler.systemScheduler()) // Async eviction for automatic cleanup
                    .removalListener(removalListener)  // Synchronous for clear() operations
                    .build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
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
  public void close() {
  }

  /**
   * Get diagnostic information about cache state.
   */
  public String getDiagnostics() {
    long size = cache.estimatedSize();
    
    long pinnedCount = cache.asMap().values().stream()
                            .filter(p -> p.getPinCount() > 0)
                            .count();
    long unpinnedCount = size - pinnedCount;
    
    long totalPinCount = cache.asMap().values().stream()
                               .mapToLong(KeyValueLeafPage::getPinCount)
                               .sum();
    
    long weightedSizeMB = -1;
    var policy = cache.policy().eviction();
    if (policy.isPresent()) {
      weightedSizeMB = policy.get().weightedSize().orElse(-1L) / (1024 * 1024);
    }
    
    return String.format("RecordPageFragmentCache: total=%d, pinned=%d, unpinned=%d, totalPinCount=%d, weightedSize=%dMB",
                        size, pinnedCount, unpinnedCount, totalPinCount, weightedSizeMB);
  }

  /**
   * Get the Caffeine cache for direct access (for testing).
   */
  public com.github.benmanes.caffeine.cache.Cache<PageReference, KeyValueLeafPage> getCaffeineCache() {
    return cache;
  }
}

