package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Simple page cache with direct eviction control.
 * <p>
 * Uses a single ConcurrentHashMap with clock-based eviction. Simplified from multi-shard design for
 * easier debugging and maintenance.
 * <p>
 * Provides: - Direct control over eviction (revision watermark + guardCount checks) - Clock-based
 * second-chance eviction algorithm - ConcurrentHashMap's built-in lock-free read optimization
 * <p>
 * Inspired by LeanStore/Umbra buffer management architectures.
 * <p>
 * <b>Locking Strategy:</b> - Per-key atomicity via ConcurrentHashMap.compute() - evictionLock
 * prevents concurrent ClockSweeper sweeps and clear() - No global lock - optimized for
 * high-concurrency workloads
 * <p>
 * <b>Note on clear() race:</b> There is a benign race between clear() and concurrent operations.
 * Since clear() is typically called only at shutdown and uses evictionLock for coordination with
 * ClockSweeper, this race is acceptable. Pages use volatile fields and synchronized close() for
 * safety.
 *
 * @author Johannes Lichtenberger
 */
public final class ShardedPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardedPageCache.class);

  private final ConcurrentHashMap<PageReference, KeyValueLeafPage> map = new ConcurrentHashMap<>();
  private final ReentrantLock evictionLock = new ReentrantLock();
  private final Shard shard; // Single shard instance (simplified design)
  private final long maxWeightBytes;
  private final AtomicLong currentWeightBytes = new AtomicLong(0L);

  // ===== CACHE HIT/MISS INSTRUMENTATION =====
  // Use LongAdder for high-contention counters (better scalability than AtomicLong)
  private static final LongAdder CACHE_HITS = new LongAdder();
  private static final LongAdder CACHE_MISSES = new LongAdder();

  /** Get cache hit count for diagnostics */
  public static long getCacheHits() {
    return CACHE_HITS.sum();
  }

  /** Get cache miss count for diagnostics */
  public static long getCacheMisses() {
    return CACHE_MISSES.sum();
  }

  /** Reset cache counters */
  public static void resetCacheCounters() {
    CACHE_HITS.reset();
    CACHE_MISSES.reset();
  }
  // ===== END INSTRUMENTATION =====

  /**
   * Create a new page cache.
   *
   */
  public ShardedPageCache(long maxWeightBytes) {
    this.shard = new Shard(map, evictionLock);
    this.maxWeightBytes = maxWeightBytes;
    LOGGER.info("Created ShardedPageCache (simplified single-map design) with maxWeight={} bytes", maxWeightBytes);
  }

  /**
   * Get the single shard (for ClockSweeper compatibility).
   */
  public Shard getShard(PageReference ref) {
    return shard;
  }

  /**
   * Current tracked weight of cached pages in bytes.
   */
  long getCurrentWeightBytes() {
    return currentWeightBytes.get();
  }

  /**
   * Callback for eviction: adjust the tracked weight.
   */
  void onEvicted(KeyValueLeafPage page, long pageWeight) {
    if (pageWeight <= 0) {
      return;
    }
    currentWeightBytes.addAndGet(-pageWeight);
  }

  /**
   * Compute the weight (bytes) of a cached page.
   */
  long weightOf(KeyValueLeafPage page) {
    if (page == null) {
      return 0L;
    }
    return page.getActualMemorySize();
  }

  /**
   * Shard wrapper for ClockSweeper compatibility. Note: clockHand should only be accessed while
   * holding evictionLock.
   */
  public static final class Shard {
    final ConcurrentHashMap<PageReference, KeyValueLeafPage> map;
    final ReentrantLock evictionLock;
    int clockHand; // Only access while holding evictionLock

    Shard(ConcurrentHashMap<PageReference, KeyValueLeafPage> map, ReentrantLock lock) {
      this.map = map;
      this.evictionLock = lock;
      this.clockHand = 0;
    }
  }

  @Override
  public KeyValueLeafPage get(PageReference key) {
    KeyValueLeafPage page = map.get(key);
    if (page != null) {
      // Benign race: markAccessed() after get() - at worst marks page being evicted
      page.markAccessed(); // Set HOT bit for clock algorithm
    }
    return page;
  }

  @Override
  public KeyValueLeafPage get(PageReference key,
      BiFunction<? super PageReference, ? super KeyValueLeafPage, ? extends KeyValueLeafPage> mappingFunction) {
    // OPTIMIZATION: Lock-free fast path for cache hits
    // ConcurrentHashMap.get() is lock-free and scales better than compute() for reads
    KeyValueLeafPage existing = map.get(key);
    if (existing != null && !existing.isClosed()) {
      // Cache HIT - mark as accessed (benign race with eviction is acceptable)
      existing.markAccessed();
      return existing;
    }

    // Cache MISS or closed entry - use compute() for atomic load-and-store
    // This ensures only one thread loads the page on concurrent misses
    KeyValueLeafPage page = map.compute(key, (k, existingValue) -> {
      // Double-check inside compute() - another thread may have loaded while we waited
      if (existingValue != null && !existingValue.isClosed()) {
        existingValue.markAccessed();
        return existingValue;
      }
      // Cache MISS - call mappingFunction to load
      KeyValueLeafPage newPage = mappingFunction.apply(k, existingValue);
      if (newPage != null && !newPage.isClosed()) {
        newPage.markAccessed(); // Set HOT bit for newly loaded page

        // Adjust tracked weight (replace closed entry if present)
        long newWeight = weightOf(newPage);
        if (existingValue != null) {
          long existingWeight = weightOf(existingValue);
          if (existingWeight > 0) {
            currentWeightBytes.addAndGet(-existingWeight);
          }
        }
        if (newWeight > 0) {
          currentWeightBytes.addAndGet(newWeight);
        }

        if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && newPage.getPageKey() == 0) {
          LOGGER.debug("[CACHE-COMPUTE] Page 0 computed and caching: {} rev={} instance={} guardCount={}",
              newPage.getIndexType(), newPage.getRevision(), System.identityHashCode(newPage), newPage.getGuardCount());
        }
      }
      return newPage;
    });

    evictIfOverBudget();

    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page != null && page.getPageKey() == 0) {
      // Verify it's actually in cache
      KeyValueLeafPage cached = map.get(key);
      boolean inCache = (cached == page);
      LOGGER.debug("[CACHE-VERIFY] Page 0 after compute: {} rev={} instance={} inCache={} cachedInstance={}",
          page.getIndexType(), page.getRevision(), System.identityHashCode(page), inCache, cached != null
              ? System.identityHashCode(cached)
              : "null");
    }

    return page;
  }

  @Override
  public KeyValueLeafPage getAndGuard(PageReference key) {
    // OPTIMIZATION: Lock-free fast path for cache hits
    // Strategy: get() -> acquireGuard() -> verify still valid
    // If page was evicted between get() and acquireGuard(), we detect via isClosed()
    KeyValueLeafPage existing = map.get(key);
    if (existing != null && !existing.isClosed()) {
      // Try to acquire guard on the page we found
      existing.acquireGuard();
      // Verify page is still valid after acquiring guard
      // (another thread may have evicted it between get() and acquireGuard())
      if (!existing.isClosed()) {
        existing.markAccessed();
        return existing;
      }
      // Race detected: page was closed after we acquired guard
      // Release our guard (harmless since page is closed) and fall back to compute()
      existing.releaseGuard();
    }

    // Cache miss or race condition - use compute() for guaranteed atomicity
    return map.compute(key, (k, existingValue) -> {
      if (existingValue != null && !existingValue.isClosed()) {
        // ATOMIC: mark accessed AND acquire guard while holding map lock for this key
        existingValue.markAccessed();
        existingValue.acquireGuard();
        return existingValue;
      }
      // Not in cache or closed - return null
      return null;
    });
  }

  @Override
  public KeyValueLeafPage getOrLoadAndGuard(PageReference key, Function<PageReference, KeyValueLeafPage> loader) {
    // OPTIMIZATION: Lock-free fast path for cache hits (the common case)
    // Strategy: get() -> acquireGuard() -> verify still valid
    // This avoids compute() lock contention for reads, which dominate the workload
    KeyValueLeafPage existing = map.get(key);
    if (existing != null && !existing.isClosed()) {
      // Try to acquire guard on the page we found
      existing.acquireGuard();
      // Verify page is still valid after acquiring guard
      // (ClockSweeper may have evicted it between get() and acquireGuard())
      if (!existing.isClosed()) {
        existing.markAccessed();
        CACHE_HITS.increment();
        return existing;
      }
      // Race detected: page was closed after we acquired guard
      // Release our guard (harmless since page is closed) and fall back to compute()
      existing.releaseGuard();
    }

    // Cache miss or race condition - use compute() for atomic load-and-store
    // This ensures only one thread loads the page on concurrent misses
    KeyValueLeafPage page = map.compute(key, (k, existingInCompute) -> {
      // Double-check inside compute() - another thread may have loaded while we waited
      if (existingInCompute != null && !existingInCompute.isClosed()) {
        // Cache HIT (loaded by another thread) - acquire guard atomically
        CACHE_HITS.increment();
        existingInCompute.markAccessed();
        existingInCompute.acquireGuard();
        return existingInCompute;
      }
      // Cache MISS - load via loader
      CACHE_MISSES.increment();
      KeyValueLeafPage loaded = loader.apply(k);
      if (loaded != null && !loaded.isClosed()) {
        loaded.markAccessed();
        loaded.acquireGuard();
        // Track weight for eviction (fixes bypass bug from direct asMap().compute() usage)
        long newWeight = weightOf(loaded);
        if (existingInCompute != null) {
          // Replace closed entry - subtract old weight
          currentWeightBytes.addAndGet(-weightOf(existingInCompute));
        }
        if (newWeight > 0) {
          currentWeightBytes.addAndGet(newWeight);
        }
      }
      return loaded;
    });
    evictIfOverBudget();
    return page;
  }

  @Override
  public void put(PageReference key, KeyValueLeafPage value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }

    value.markAccessed(); // Set HOT bit BEFORE inserting to ensure it's marked
    map.compute(key, (k, existing) -> {
      long delta = weightOf(value);
      if (existing != null) {
        delta -= weightOf(existing);
      }
      if (delta != 0) {
        currentWeightBytes.addAndGet(delta);
      }
      return value;
    });

    evictIfOverBudget();
  }

  @Override
  public void putIfAbsent(PageReference key, KeyValueLeafPage value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }

    value.markAccessed(); // Set HOT bit BEFORE attempting insert
    KeyValueLeafPage existing = map.putIfAbsent(key, value);

    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && value.getPageKey() == 0) {
      if (existing == null) {
        LOGGER.debug("[CACHE-ADD] Page 0 added to cache: {} rev={} instance={} guardCount={}", value.getIndexType(),
            value.getRevision(), System.identityHashCode(value), value.getGuardCount());
      } else {
        LOGGER.debug("[CACHE-SKIP] Page 0 NOT added (already exists): {} rev={} newInstance={} existingInstance={}",
            value.getIndexType(), value.getRevision(), System.identityHashCode(value),
            System.identityHashCode(existing));
      }
    }
    // If a value already exists, our value wasn't inserted (another thread won the race)
    // but marking it hot is still harmless

    if (existing == null) {
      long newWeight = weightOf(value);
      if (newWeight > 0) {
        currentWeightBytes.addAndGet(newWeight);
      }
      evictIfOverBudget();
    }
  }

  @Override
  public void clear() {
    evictionLock.lock();
    try {
      // CRITICAL: Iterate over snapshot to avoid concurrent modification during iteration
      java.util.List<KeyValueLeafPage> snapshot = new java.util.ArrayList<>(map.values());

      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        LOGGER.debug("ShardedPageCache.clear(): {} pages in snapshot", snapshot.size());
      }

      int closedCount = 0;
      int guardedCount = 0;
      int alreadyClosedCount = 0;

      // Try to close all pages
      // Note: close() is synchronized and checks guardCount internally
      // Guarded pages will NOT be closed (close() returns early if guardCount > 0)
      for (KeyValueLeafPage page : snapshot) {
        boolean wasClosedBefore = page.isClosed();
        int guardCount = page.getGuardCount();

        if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page.getPageKey() == 0) {
          LOGGER.debug("  ShardedPageCache.clear(): Page 0 ({}) rev={} instance={} guardCount={} closed={}",
              page.getIndexType(), page.getRevision(), System.identityHashCode(page), guardCount, wasClosedBefore);
        }

        if (wasClosedBefore) {
          alreadyClosedCount++;
          continue;
        }

        // CRITICAL: Force-release all guards before closing to ensure memory segments are returned
        // Guards prevent eviction during normal operation, but during cache clear we MUST reclaim memory
        while (page.getGuardCount() > 0) {
          page.releaseGuard();
        }

        // Attempt to close
        page.close();

        boolean closedNow = page.isClosed();
        if (closedNow) {
          closedCount++;
          if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page.getPageKey() == 0) {
            if (guardCount > 0) {
              LOGGER.debug("  ShardedPageCache.clear(): Page 0 closed (force-released {} guards)", guardCount);
            } else {
              LOGGER.debug("  ShardedPageCache.clear(): Page 0 closed successfully");
            }
          }
        } else {
          guardedCount++;
          LOGGER.error(
              "  ShardedPageCache.clear(): Page {} ({}) rev={} instance={} FAILED to close even after force-releasing {} guards!",
              page.getPageKey(), page.getIndexType(), page.getRevision(), System.identityHashCode(page), guardCount);
        }
      }

      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        LOGGER.debug("ShardedPageCache.clear(): closed={}, guarded={}, alreadyClosed={}", closedCount, guardedCount,
            alreadyClosedCount);
      }

      // Clear the map
      // WARNING: This removes cache entries even for guarded pages that couldn't be closed
      // This is acceptable for shutdown scenarios (typical clear() use case)
      // If guards are leaked, those pages will be orphaned in memory
      map.clear();
      currentWeightBytes.set(0L);
      shard.clockHand = 0;
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public void remove(PageReference key) {
    KeyValueLeafPage page = map.remove(key);

    // DIAGNOSTIC: Track when Page 0s are removed from cache
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && page != null && page.getPageKey() == 0) {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      String caller = stack.length > 2
          ? stack[2].getMethodName()
          : "unknown";
      LOGGER.warn(
          "[CACHE-REMOVE] Page 0 ({}) rev={} instance={} guardCount={} closed={} - removed by {} WITHOUT closing (intentional for TIL)",
          page.getIndexType(), page.getRevision(), System.identityHashCode(page), page.getGuardCount(), page.isClosed(),
          caller);
    }

    if (page != null) {
      long weight = weightOf(page);
      if (weight > 0) {
        currentWeightBytes.addAndGet(-weight);
      }
    }

    // NOTE: We do NOT close pages here by design
    // This method is called when pages are moved to TransactionIntentLog (TIL takes ownership)
    // TIL will close the pages when the transaction commits/aborts
  }

  @Override
  public java.util.Map<PageReference, KeyValueLeafPage> getAll(Iterable<? extends PageReference> keys) {
    java.util.Map<PageReference, KeyValueLeafPage> result = new java.util.HashMap<>();
    for (PageReference key : keys) {
      KeyValueLeafPage page = get(key);
      if (page != null) {
        result.put(key, page);
      }
    }
    return result;
  }

  @Override
  public void putAll(java.util.Map<? extends PageReference, ? extends KeyValueLeafPage> map) {
    for (java.util.Map.Entry<? extends PageReference, ? extends KeyValueLeafPage> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void toSecondCache() {
    // No-op: we don't have a two-level cache structure
  }

  @Override
  public ConcurrentMap<PageReference, KeyValueLeafPage> asMap() {
    return map;
  }

  @Override
  public void close() {
    clear();
  }


  /**
   * Get the number of shards (always 1 in simplified design).
   *
   * @return shard count (1)
   */
  public int getShardCount() {
    return 1;
  }

  /**
   * Get total number of cached pages.
   *
   * @return total page count
   */
  public long size() {
    return map.size();
  }

  /**
   * Evict cold, unguarded pages until the cache is within the configured memory budget. Uses a simple
   * two-pass approach per page: first clears HOT bit, then evicts on the next pass. This keeps
   * enforcement cheap and virtual-thread friendly.
   */
  private void evictIfOverBudget() {
    if (maxWeightBytes <= 0) {
      return;
    }

    // Fast path without locking
    if (currentWeightBytes.get() <= maxWeightBytes) {
      return;
    }

    if (!evictionLock.tryLock()) {
      return; // Avoid blocking writers; ClockSweeper will also evict
    }

    try {
      var iterator = map.entrySet().iterator();
      while (currentWeightBytes.get() > maxWeightBytes && iterator.hasNext()) {
        var entry = iterator.next();
        PageReference ref = entry.getKey();

        // Keep eviction atomic with respect to other cache operations
        map.compute(ref, (k, page) -> {
          if (page == null) {
            return null;
          }

          // First pass: clear HOT bit
          if (page.isHot()) {
            page.clearHot();
            return page;
          }

          // Skip guarded pages
          if (page.getGuardCount() > 0) {
            if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && LOGGER.isDebugEnabled()) {
              LOGGER.debug("Eviction skipped: guarded page key={} type={} rev={} guards={} hot={}", page.getPageKey(),
                  page.getIndexType(), page.getRevision(), page.getGuardCount(), page.isHot());
            }
            return page;
          }

          long pageWeight = weightOf(page);
          try {
            page.incrementVersion();
            ref.setPage(null);
            page.close();

            if (!page.isClosed()) {
              return page; // Could not close (guard acquired concurrently)
            }

            if (pageWeight > 0) {
              currentWeightBytes.addAndGet(-pageWeight);
            }

            return null; // Successfully evicted
          } catch (Exception e) {
            LOGGER.error("Failed to evict page {} during budget enforcement: {}", page.getPageKey(), e.getMessage());
            return page;
          }
        });
      }

      // If we still exceed budget and diagnostics enabled, log a small sample of guarded pages.
      if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && currentWeightBytes.get() > maxWeightBytes && LOGGER.isWarnEnabled()) {
        logGuardedPagesSample();
      }
    } finally {
      evictionLock.unlock();
    }
  }

  /**
   * Log a small sample of guarded pages to help track down guard leaks.
   */
  private void logGuardedPagesSample() {
    int logged = 0;
    for (KeyValueLeafPage page : map.values()) {
      if (page.getGuardCount() > 0) {
        LOGGER.warn("Guarded page prevents eviction: key={} type={} rev={} guards={} hot={}", page.getPageKey(),
            page.getIndexType(), page.getRevision(), page.getGuardCount(), page.isHot());
        if (++logged >= 5) {
          break; // limit noise
        }
      }
    }
    if (logged == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug("No guarded pages found, but cache over budget. CurrentWeight={} MaxWeight={}",
          currentWeightBytes.get(), maxWeightBytes);
    }
  }

  /**
   * Get diagnostic information about cache state.
   *
   * @return diagnostic string
   */
  public String getDiagnostics() {
    long totalPages = 0;
    long totalMemory = 0;
    long hotPages = 0;

    for (KeyValueLeafPage page : map.values()) {
      totalPages++;
      totalMemory += page.getActualMemorySize();
      if (page.isHot()) {
        hotPages++;
      }
    }

    return String.format("ShardedPageCache: pages=%d, hot=%d (%.1f%%), memory=%.2fMB", totalPages, hotPages,
        totalPages > 0
            ? (hotPages * 100.0 / totalPages)
            : 0,
        totalMemory / (1024.0 * 1024.0));
  }
}

