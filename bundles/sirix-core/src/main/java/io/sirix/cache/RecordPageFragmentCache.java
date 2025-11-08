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
                      if (value.getPinCount() > 0) {
                        return 0; // Pinned fragments have zero weight (won't be evicted)
                      }
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
  public void unpinAndUpdateWeight(PageReference key, int trxId) {
    cache.asMap().computeIfPresent(key, (k, page) -> {
      // CRITICAL FIX: Skip if page is closed to prevent assertions
      if (page.isClosed()) {
        return page;  // Return as-is, don't try to unpin
      }
      
      // CRITICAL FIX: Only unpin if this transaction actually pinned the page
      // Must check using the ConcurrentHashMap to avoid race conditions
      var pinCountMap = page.getPinCountByTransaction();
      var counter = pinCountMap.get(trxId);
      if (counter != null && counter > 0) {
        try {
          page.decrementPinCount(trxId);
        } catch (IllegalStateException e) {
          // Race condition: page was unpinned by another thread between check and decrement
          // This is safe to ignore - page is already unpinned
        }
      }
      return page; // Returning same instance triggers weight recalculation
    });
  }
  
  /**
   * Atomically pin a page and update its weight in the cache.
   * 
   * @param key the page reference  
   * @param trxId the transaction ID doing the pin
   */
  public void pinAndUpdateWeight(PageReference key, int trxId) {
    cache.asMap().computeIfPresent(key, (k, page) -> {
      page.incrementPinCount(trxId);
      return page; // Put back triggers weight recalculation
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

