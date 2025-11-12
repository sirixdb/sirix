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
 *
 * @author Johannes Lichtenberger
 */
public final class ShardedPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardedPageCache.class);

  private final ConcurrentHashMap<PageReference, KeyValueLeafPage> map = new ConcurrentHashMap<>();
  private final ReentrantLock evictionLock = new ReentrantLock();
  private int clockHand = 0;

  /**
   * Create a new page cache.
   *
   * @param shardCount unused (kept for API compatibility)
   */
  public ShardedPageCache(int shardCount) {
    LOGGER.info("Created ShardedPageCache (simplified single-map design)");
  }

  /**
   * Get the single shard (for ClockSweeper compatibility).
   */
  public Shard getShard(PageReference ref) {
    return new Shard(map, evictionLock, clockHand);
  }
  
  /**
   * Shard wrapper for ClockSweeper compatibility.
   */
  public static final class Shard {
    final ConcurrentHashMap<PageReference, KeyValueLeafPage> map;
    final ReentrantLock evictionLock;
    int clockHand;

    Shard(ConcurrentHashMap<PageReference, KeyValueLeafPage> map, ReentrantLock lock, int clockHand) {
      this.map = map;
      this.evictionLock = lock;
      this.clockHand = clockHand;
    }
  }

  @Override
  public KeyValueLeafPage get(PageReference key) {
    KeyValueLeafPage page = map.get(key);
    if (page != null && !page.isClosed()) {
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
        // Cache HIT - return existing without calling mappingFunction
        return existingValue;
      }
      // Cache MISS - call mappingFunction to load
      return mappingFunction.apply(k, existingValue);
    });
    if (page != null && !page.isClosed()) {
      page.markAccessed(); // Set HOT bit
    }
    return page;
  }

  @Override
  public void put(PageReference key, KeyValueLeafPage value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }
    
    map.put(key, value);
    value.markAccessed(); // Set HOT bit
  }

  @Override
  public void putIfAbsent(PageReference key, KeyValueLeafPage value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }
    
    KeyValueLeafPage existing = map.putIfAbsent(key, value);
    if (existing == null) {
      value.markAccessed(); // Set HOT bit for new entry
    }
  }

  @Override
  public void clear() {
    evictionLock.lock();
    try {
      // Close all pages before clearing
      for (KeyValueLeafPage page : map.values()) {
        if (!page.isClosed()) {
          page.close();
        }
      }
      map.clear();
      clockHand = 0;
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public void remove(PageReference key) {
    KeyValueLeafPage page = map.remove(key);
    if (page != null && !page.isClosed()) {
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

