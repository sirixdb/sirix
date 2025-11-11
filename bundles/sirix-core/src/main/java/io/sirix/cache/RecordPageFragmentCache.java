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
          
          // TODO: Will be replaced with version-based eviction logic
          // For now, close all fragment pages on removal
          
          // Track evictions
          long evictions = TOTAL_EVICTIONS.incrementAndGet();
          
          // Log every 100th eviction
          if (evictions % 100 == 0) {
            LOGGER.info("RecordPageFragmentCache: {} evictions", evictions);
          }
          
          // DIAGNOSTIC: Track page closes for leak detection
          if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
            LOGGER.debug("RecordPageFragmentCache closing page {} type={} rev={} cause={}", 
                page.getPageKey(), page.getIndexType(), page.getRevision(), cause);
          }
          
          page.close();
        };

    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeight)
                    .weigher((PageReference _, KeyValueLeafPage value) -> {
                      // TODO: Will be replaced with custom sharded cache
                      return (int) value.getActualMemorySize();
                    })
                    .scheduler(Scheduler.systemScheduler())
                    .removalListener(removalListener)
                    .build();
  }

  @Override
  public void clear() {
    long sizeBefore = cache.estimatedSize();
    
    // CRITICAL: Force all pending async evictions to complete synchronously
    // cleanUp() processes the pending maintenance queue
    cache.cleanUp();
    
    // Then invalidate all remaining entries - this is synchronous
    cache.invalidateAll();
    
    // Final cleanup to ensure all removal listeners completed
    cache.cleanUp();
    
    if (sizeBefore > 0) {
      LOGGER.info("RecordPageFragmentCache.clear(): {} entries cleared", sizeBefore);
    }
  }
  
  /**
   * Atomically unpin a page and update its weight in the cache.
   * This prevents race conditions where another transaction could pin/access the page
   * between unpinning and putting back.
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
   * Get diagnostic information about cache state.
   */
  public String getDiagnostics() {
    long size = cache.estimatedSize();
    
    // TODO: Update to track guard count instead of pin count
    
    long weightedSizeMB = -1;
    var policy = cache.policy().eviction();
    if (policy.isPresent()) {
      weightedSizeMB = policy.get().weightedSize().orElse(-1L) / (1024 * 1024);
    }
    
    return String.format("RecordPageFragmentCache: total=%d, guardCount=TODO, weightedSize=%dMB",
                        size, weightedSizeMB);
  }

  /**
   * Get the Caffeine cache for direct access (for testing).
   */
  public com.github.benmanes.caffeine.cache.Cache<PageReference, KeyValueLeafPage> getCaffeineCache() {
    return cache;
  }
}

