package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

/**
 * Simple page cache with direct eviction control.
 * <p>
 * Uses a single ConcurrentHashMap with clock-based eviction.
 * Simplified from multi-shard design for easier debugging and maintenance.
 * <p>
 * Provides:
 * - Direct control over eviction (revision watermark + guardCount checks)
 * - Clock-based second-chance eviction algorithm
 * - ConcurrentHashMap's built-in lock-free read optimization
 * <p>
 * Inspired by LeanStore/Umbra buffer management architectures.
 * <p>
 * <b>Locking Strategy:</b>
 * - Per-key atomicity via ConcurrentHashMap.compute()
 * - evictionLock prevents concurrent ClockSweeper sweeps and clear()
 * - No global lock - optimized for high-concurrency workloads
 * <p>
 * <b>Note on clear() race:</b>
 * There is a benign race between clear() and concurrent operations. Since clear()
 * is typically called only at shutdown and uses evictionLock for coordination with
 * ClockSweeper, this race is acceptable. Pages use volatile fields and synchronized
 * close() for safety.
 *
 * @author Johannes Lichtenberger
 */
public final class ShardedPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardedPageCache.class);

  private final ConcurrentHashMap<PageReference, KeyValueLeafPage> map = new ConcurrentHashMap<>();
  private final ReentrantLock evictionLock = new ReentrantLock();
  private final Shard shard; // Single shard instance (simplified design)

  /**
   * Create a new page cache.
   *
   * @param shardCount unused (kept for API compatibility)
   */
  public ShardedPageCache(int shardCount) {
    this.shard = new Shard(map, evictionLock);
    LOGGER.info("Created ShardedPageCache (simplified single-map design)");
  }

  /**
   * Get the single shard (for ClockSweeper compatibility).
   */
  public Shard getShard(PageReference ref) {
    return shard;
  }
  
  /**
   * Shard wrapper for ClockSweeper compatibility.
   * Note: clockHand should only be accessed while holding evictionLock.
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
  public KeyValueLeafPage get(PageReference key, BiFunction<? super PageReference, ? super KeyValueLeafPage, ? extends KeyValueLeafPage> mappingFunction) {
    // CRITICAL FIX: Wrap mappingFunction to only execute on cache MISS
    // The mappingFunction should only be called when value is null, not on every access!
    // This prevents guards from being acquired repeatedly (guard leaks)
    KeyValueLeafPage page = map.compute(key, (k, existingValue) -> {
      if (existingValue != null && !existingValue.isClosed()) {
        // Cache HIT - mark as accessed and return existing without calling mappingFunction
        existingValue.markAccessed(); // Set HOT bit atomically within compute()
        return existingValue;
      }
      // Cache MISS - call mappingFunction to load
      KeyValueLeafPage newPage = mappingFunction.apply(k, existingValue);
      if (newPage != null && !newPage.isClosed()) {
        newPage.markAccessed(); // Set HOT bit for newly loaded page
      }
      return newPage;
    });
    return page;
  }

  @Override
  public void put(PageReference key, KeyValueLeafPage value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }
    
    value.markAccessed(); // Set HOT bit BEFORE inserting to ensure it's marked
    map.put(key, value);
  }

  @Override
  public void putIfAbsent(PageReference key, KeyValueLeafPage value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }
    
    value.markAccessed(); // Set HOT bit BEFORE attempting insert
    map.putIfAbsent(key, value);
    // If a value already exists, our value wasn't inserted (another thread won the race)
    // but marking it hot is still harmless
  }

  @Override
  public void clear() {
    evictionLock.lock();
    try {
      // CRITICAL: Iterate over snapshot to avoid concurrent modification during iteration
      java.util.List<KeyValueLeafPage> snapshot = new java.util.ArrayList<>(map.values());
      
      // Try to close all pages
      // Note: close() is synchronized and checks guardCount internally
      // Guarded pages will NOT be closed (close() returns early if guardCount > 0)
      for (KeyValueLeafPage page : snapshot) {
        if (!page.isClosed()) {
          page.close(); // Skips if guarded
        }
      }
      
      // Clear the map
      // WARNING: This removes cache entries even for guarded pages that couldn't be closed
      // This is acceptable for shutdown scenarios (typical clear() use case)
      // If guards are leaked, those pages will be orphaned in memory
      map.clear();
      shard.clockHand = 0;
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public void remove(PageReference key) {
    KeyValueLeafPage page = map.remove(key);
    if (page != null && !page.isClosed()) {
      // close() is synchronized and idempotent
      page.close();
    }
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
   * Get a page and atomically acquire a guard on it.
   * This prevents the race where ClockSweeper evicts a page between
   * cache lookup and guard acquisition.
   *
   * @param key the page reference key
   * @return page with guard already acquired, or null if not in cache or closed
   */
  public KeyValueLeafPage getAndGuard(PageReference key) {
    return map.compute(key, (k, existingValue) -> {
      if (existingValue != null && !existingValue.isClosed()) {
        // ATOMIC: mark accessed AND acquire guard while holding map lock for this key
        existingValue.markAccessed();
        existingValue.acquireGuard();
        return existingValue;
      }
      // Not in cache or closed - return null (don't return closed page!)
      // CRITICAL: If page is closed, we must NOT acquire guard and must NOT return it
      // Returning null signals to caller that they need to load from disk
      return null;
    });
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

    return String.format("ShardedPageCache: pages=%d, hot=%d (%.1f%%), memory=%.2fMB",
        totalPages, hotPages,
        totalPages > 0 ? (hotPages * 100.0 / totalPages) : 0,
        totalMemory / (1024.0 * 1024.0));
  }
}

