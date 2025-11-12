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
 * Sharded page cache for multi-core scalability.
 * <p>
 * Replaces Caffeine with custom sharding to enable:
 * - Direct control over eviction (revision watermark + guardCount checks)
 * - Clock-based second-chance eviction algorithm
 * - Minimal lock contention across cores
 * <p>
 * Inspired by LeanStore/Umbra buffer management architectures.
 *
 * @author Johannes Lichtenberger
 */
public final class ShardedPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardedPageCache.class);

  private final Shard[] shards;
  private final int shardMask;

  /**
   * A single shard containing a subset of cached pages.
   */
  public static final class Shard {
    final ConcurrentHashMap<PageReference, KeyValueLeafPage> map = new ConcurrentHashMap<>();
    final ReentrantLock evictionLock = new ReentrantLock();
    int clockHand = 0;

    public Shard() {
    }
  }

  /**
   * Create a new sharded page cache.
   *
   * @param shardCount number of shards (must be power of 2, e.g., 64, 128)
   */
  public ShardedPageCache(int shardCount) {
    if (Integer.bitCount(shardCount) != 1) {
      throw new IllegalArgumentException("Shard count must be power of 2, got: " + shardCount);
    }
    
    this.shards = new Shard[shardCount];
    this.shardMask = shardCount - 1;
    
    for (int i = 0; i < shardCount; i++) {
      shards[i] = new Shard();
    }
    
    LOGGER.info("Created ShardedPageCache with {} shards", shardCount);
  }

  /**
   * Compute shard index for a page reference.
   */
  private int shardIndex(PageReference ref) {
    // Hash based on database ID, resource ID, and key for good distribution
    int hash = (int) (ref.getDatabaseId() ^ ref.getResourceId() ^ ref.getKey());
    // Mix bits to improve distribution
    hash ^= (hash >>> 16);
    return hash & shardMask;
  }

  /**
   * Get the shard for a page reference.
   */
  public Shard getShard(PageReference ref) {
    return shards[shardIndex(ref)];
  }

  @Override
  public KeyValueLeafPage get(PageReference key) {
    Shard shard = getShard(key);
    KeyValueLeafPage page = shard.map.get(key);
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
    Shard shard = getShard(key);
    KeyValueLeafPage page = shard.map.compute(key, (k, existingValue) -> {
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
    
    Shard shard = getShard(key);
    shard.map.put(key, value);
    value.markAccessed(); // Set HOT bit
  }

  @Override
  public void putIfAbsent(PageReference key, KeyValueLeafPage value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }
    
    Shard shard = getShard(key);
    KeyValueLeafPage existing = shard.map.putIfAbsent(key, value);
    if (existing == null) {
      value.markAccessed(); // Set HOT bit for new entry
    }
  }

  @Override
  public void clear() {
    for (Shard shard : shards) {
      shard.evictionLock.lock();
      try {
        // Close all pages before clearing
        for (KeyValueLeafPage page : shard.map.values()) {
          if (!page.isClosed()) {
            page.close();
          }
        }
        shard.map.clear();
        shard.clockHand = 0;
      } finally {
        shard.evictionLock.unlock();
      }
    }
  }

  @Override
  public void remove(PageReference key) {
    Shard shard = getShard(key);
    KeyValueLeafPage page = shard.map.remove(key);
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
    // Return a view that combines all shards
    // Note: This is less efficient than per-shard access, but needed for compatibility
    ConcurrentHashMap<PageReference, KeyValueLeafPage> combinedMap = new ConcurrentHashMap<>();
    for (Shard shard : shards) {
      combinedMap.putAll(shard.map);
    }
    return combinedMap;
  }

  @Override
  public void close() {
    clear();
  }

  /**
   * Get the number of shards.
   *
   * @return shard count
   */
  public int getShardCount() {
    return shards.length;
  }

  /**
   * Get total number of cached pages across all shards.
   *
   * @return total page count
   */
  public long size() {
    long total = 0;
    for (Shard shard : shards) {
      total += shard.map.size();
    }
    return total;
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

    for (Shard shard : shards) {
      for (KeyValueLeafPage page : shard.map.values()) {
        totalPages++;
        totalMemory += page.getActualMemorySize();
        if (page.isHot()) {
          hotPages++;
        }
      }
    }

    return String.format("ShardedPageCache: shards=%d, pages=%d, hot=%d (%.1f%%), memory=%.2fMB",
        shards.length, totalPages, hotPages,
        totalPages > 0 ? (hotPages * 100.0 / totalPages) : 0,
        totalMemory / (1024.0 * 1024.0));
  }
}

