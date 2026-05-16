package io.sirix.cache;

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
public final class ShardedPageCache<V extends CacheablePage> implements Cache<PageReference, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardedPageCache.class);

  static final boolean DEBUG_MEMORY_LEAKS =
      Boolean.getBoolean("sirix.debug.memoryLeaks");

  private final ConcurrentHashMap<PageReference, V> map = new ConcurrentHashMap<>();
  private final ReentrantLock evictionLock = new ReentrantLock();
  private final Shard<V> shard;
  private final long maxWeightBytes;
  private final AtomicLong currentWeightBytes = new AtomicLong(0L);

  // ===== CACHE HIT/MISS INSTRUMENTATION =====
  // Use LongAdder for high-contention counters (better scalability than AtomicLong)
  private static final LongAdder CACHE_HITS = new LongAdder();
  private static final LongAdder CACHE_MISSES = new LongAdder();
  private static final LongAdder CACHE_EVICTIONS = new LongAdder();

  /** Get cache hit count for diagnostics */
  public static long getCacheHits() {
    return CACHE_HITS.sum();
  }

  /** Get cache miss count for diagnostics */
  public static long getCacheMisses() {
    return CACHE_MISSES.sum();
  }

  /** Get cache eviction count for diagnostics */
  public static long getCacheEvictions() {
    return CACHE_EVICTIONS.sum();
  }

  /** Reset cache counters */
  public static void resetCacheCounters() {
    CACHE_HITS.reset();
    CACHE_MISSES.reset();
    CACHE_EVICTIONS.reset();
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
  public Shard<V> getShard(PageReference ref) {
    return shard;
  }

  /**
   * Current tracked weight of cached pages in bytes.
   */
  long getCurrentWeightBytes() {
    return currentWeightBytes.get();
  }

  /**
   * Callback for eviction: adjust the tracked weight and bump the eviction counter.
   */
  void onEvicted(CacheablePage page, long pageWeight) {
    if (pageWeight <= 0) {
      return;
    }
    currentWeightBytes.addAndGet(-pageWeight);
    CACHE_EVICTIONS.increment();
  }

  /**
   * Compute the weight (bytes) of a cached page.
   */
  long weightOf(CacheablePage page) {
    if (page == null) {
      return 0L;
    }
    return page.getActualMemorySize();
  }

  /**
   * Shard wrapper for ClockSweeper compatibility. Note: clockHand should only be accessed while
   * holding evictionLock.
   */
  public static final class Shard<V extends CacheablePage> {
    final ConcurrentHashMap<PageReference, V> map;
    final ReentrantLock evictionLock;
    int clockHand; // Only access while holding evictionLock

    Shard(ConcurrentHashMap<PageReference, V> map, ReentrantLock lock) {
      this.map = map;
      this.evictionLock = lock;
      this.clockHand = 0;
    }
  }

  @Override
  public V get(PageReference key) {
    V page = map.get(key);
    if (page != null) {
      page.markAccessed();
    }
    return page;
  }

  @Override
  public V get(PageReference key,
      BiFunction<? super PageReference, ? super V, ? extends V> mappingFunction) {
    V existing = map.get(key);
    if (existing != null && !existing.isClosed()) {
      existing.markAccessed();
      return existing;
    }

    V page = map.compute(key, (k, existingValue) -> {
      if (existingValue != null && !existingValue.isClosed()) {
        existingValue.markAccessed();
        return existingValue;
      }
      V newPage = mappingFunction.apply(k, existingValue);
      if (newPage != null && !newPage.isClosed()) {
        newPage.markAccessed();

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

        if (DEBUG_MEMORY_LEAKS && newPage.getPageKey() == 0) {
          LOGGER.debug("[CACHE-COMPUTE] Page 0 computed and caching: {} rev={} instance={} guardCount={}",
              newPage.getIndexType(), newPage.getRevision(), System.identityHashCode(newPage), newPage.getGuardCount());
        }
      }
      return newPage;
    });

    evictIfOverBudget();

    if (DEBUG_MEMORY_LEAKS && page != null && page.getPageKey() == 0) {
      V cached = map.get(key);
      boolean inCache = (cached == page);
      LOGGER.debug("[CACHE-VERIFY] Page 0 after compute: {} rev={} instance={} inCache={} cachedInstance={}",
          page.getIndexType(), page.getRevision(), System.identityHashCode(page), inCache, cached != null
              ? System.identityHashCode(cached)
              : "null");
    }

    return page;
  }

  @Override
  public V getAndGuard(PageReference key) {
    V existing = map.get(key);
    if (existing != null && existing.acquireGuard()) {
      existing.markAccessed();
      return existing;
    }

    return map.compute(key, (k, existingValue) -> {
      if (existingValue != null && !existingValue.isClosed()) {
        existingValue.markAccessed();
        existingValue.acquireGuard();
        return existingValue;
      }
      if (existingValue != null) {
        k.setPage(null);
      }
      return null;
    });
  }

  @Override
  public V getOrLoadAndGuard(PageReference key, Function<PageReference, V> loader) {
    V existing = map.get(key);
    if (existing != null && existing.acquireGuard()) {
      existing.markAccessed();
      CACHE_HITS.increment();
      return existing;
    }

    V page = map.compute(key, (k, existingInCompute) -> {
      if (existingInCompute != null && !existingInCompute.isClosed()) {
        CACHE_HITS.increment();
        existingInCompute.markAccessed();
        existingInCompute.acquireGuard();
        return existingInCompute;
      }
      CACHE_MISSES.increment();
      V loaded = loader.apply(k);
      if (loaded != null && !loaded.isClosed()) {
        loaded.markAccessed();
        loaded.acquireGuard();
        long newWeight = weightOf(loaded);
        if (existingInCompute != null) {
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
  public void put(PageReference key, V value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }

    value.markAccessed();
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
  public void putIfAbsent(PageReference key, V value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }

    value.markAccessed();
    V existing = map.putIfAbsent(key, value);

    if (DEBUG_MEMORY_LEAKS && value.getPageKey() == 0) {
      if (existing == null) {
        LOGGER.debug("[CACHE-ADD] Page 0 added to cache: {} rev={} instance={} guardCount={}", value.getIndexType(),
            value.getRevision(), System.identityHashCode(value), value.getGuardCount());
      } else {
        LOGGER.debug("[CACHE-SKIP] Page 0 NOT added (already exists): {} rev={} newInstance={} existingInstance={}",
            value.getIndexType(), value.getRevision(), System.identityHashCode(value),
            System.identityHashCode(existing));
      }
    }

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
      java.util.List<V> snapshot = new java.util.ArrayList<>(map.values());

      for (V page : snapshot) {
        if (page.isClosed()) {
          continue;
        }
        while (page.getGuardCount() > 0) {
          page.releaseGuard();
        }
        page.close();
      }

      for (final PageReference key : map.keySet()) {
        key.setPage(null);
      }
      map.clear();
      currentWeightBytes.set(0L);
      shard.clockHand = 0;
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public void remove(PageReference key) {
    V page = map.remove(key);

    if (page != null) {
      long weight = weightOf(page);
      if (weight > 0) {
        currentWeightBytes.addAndGet(-weight);
      }
      key.setPage(null);
    }
  }

  @Override
  public java.util.Map<PageReference, V> getAll(Iterable<? extends PageReference> keys) {
    java.util.Map<PageReference, V> result = new java.util.HashMap<>();
    for (PageReference key : keys) {
      V page = get(key);
      if (page != null) {
        result.put(key, page);
      }
    }
    return result;
  }

  @Override
  public void putAll(java.util.Map<? extends PageReference, ? extends V> map) {
    for (java.util.Map.Entry<? extends PageReference, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void toSecondCache() {
    // No-op: we don't have a two-level cache structure
  }

  @Override
  public ConcurrentMap<PageReference, V> asMap() {
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
   * Evict cold, unguarded pages until the cache is within the configured memory budget.
   *
   * <h2>Two modes of pressure</h2>
   *
   * <ul>
   *   <li><b>Normal over-budget (&lt; 110% of limit):</b> non-blocking {@code tryLock},
   *       two-pass HOT-bit algorithm. Lets concurrent writers through and delegates
   *       to the background {@link ClockSweeper}.</li>
   *   <li><b>Severe over-budget (&ge; 110% of limit):</b> blocking {@code lock},
   *       single-pass eviction — HOT bit is ignored since at high concurrency the
   *       two-pass approach never catches up (bit gets re-set between sweeps).</li>
   * </ul>
   *
   * <p>Why the split: at 20 parallel readers, tryLock succeeds only intermittently
   * and the two-pass HOT logic leaves every page permanently hot during a scan.
   * That starves the allocator budget and surfaces as {@code OutOfMemoryError} in
   * {@code MemorySegmentAllocator.allocate} 10+ seconds later. Forcing a blocking
   * one-pass eviction when we're significantly over budget makes eviction keep up
   * with allocation pressure.
   */
  private void evictIfOverBudget() {
    if (maxWeightBytes <= 0) {
      return;
    }

    final long currentWeight = currentWeightBytes.get();
    if (currentWeight <= maxWeightBytes) {
      return;
    }

    // Pressure level: normal = two-pass HOT-bit; severe = blocking one-pass.
    // 10% over-budget threshold matches the retry budget we allow the allocator
    // before it bubbles OOM — eviction must finish before that fires.
    final boolean severe = currentWeight > (maxWeightBytes + maxWeightBytes / 10);

    // Fast path: if we're not severely over budget, skip synchronous eviction
    // entirely on the put-path and let the background {@link ClockSweeper}
    // handle the trickle. Walking the full CHM entry set on every put was
    // ~9% of cold-cache on-CPU time at 100M scale (profiled). The sweeper
    // runs periodically, so a transient few-percent overshoot is fine —
    // the allocator only OOMs when >10% over, which is the severe branch.
    if (!severe) {
      return;
    }

    if (severe) {
      evictionLock.lock();
    } else if (!evictionLock.tryLock()) {
      return; // Let the ClockSweeper handle the transient case.
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

          // Two-pass HOT-bit only in the non-severe path; in severe mode we
          // evict cold-or-hot unguarded pages in a single pass.
          if (!severe && page.isHot()) {
            page.clearHot();
            return page;
          }

          // Skip guarded pages
          if (page.getGuardCount() > 0) {
            if (DEBUG_MEMORY_LEAKS && LOGGER.isDebugEnabled()) {
              LOGGER.debug("Eviction skipped: guarded page key={} type={} rev={} guards={} hot={}", page.getPageKey(),
                  page.getIndexType(), page.getRevision(), page.getGuardCount(), page.isHot());
            }
            return page;
          }

          long pageWeight = weightOf(page);
          try {
            // close() first — it's a no-op if guards are held. Only bump
            // version and break the back-reference AFTER we know the page
            // is dead. Previously these mutations happened before close(),
            // so a concurrent reader with a snapshotted version (PageGuard)
            // would see a drifted version on release and throw
            // FrameReusedException even though the eviction never happened.
            page.close();
            if (!page.isClosed()) {
              return page; // Guard acquired concurrently — no mutations applied.
            }

            page.incrementVersion();
            ref.setPage(null);

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
      if (DEBUG_MEMORY_LEAKS && currentWeightBytes.get() > maxWeightBytes && LOGGER.isWarnEnabled()) {
        logGuardedPagesSample();
      }
    } finally {
      evictionLock.unlock();
    }
  }

  /**
   * Force eviction under global allocator pressure. Evicts unguarded pages even if
   * this cache is within its own budget — the global allocator budget is shared across
   * all caches and the TIL, so this cache must shed pages when the allocator cannot
   * satisfy an allocation regardless of local budget state.
   */
  public void evictUnderPressure() {
    if (maxWeightBytes <= 0 || map.isEmpty()) {
      return;
    }
    final long target = maxWeightBytes * 3 / 4;
    evictionLock.lock();
    try {
      var iterator = map.entrySet().iterator();
      while (currentWeightBytes.get() > target && iterator.hasNext()) {
        var entry = iterator.next();
        final PageReference ref = entry.getKey();
        map.compute(ref, (k, page) -> {
          if (page == null || page.getGuardCount() > 0) {
            return page;
          }
          final long pageWeight = weightOf(page);
          try {
            page.close();
            if (!page.isClosed()) {
              return page;
            }
            page.incrementVersion();
            ref.setPage(null);
            if (pageWeight > 0) {
              currentWeightBytes.addAndGet(-pageWeight);
            }
            return null;
          } catch (Exception e) {
            LOGGER.debug("evictUnderPressure failed for page {}: {}", page.getPageKey(), e.getMessage());
            return page;
          }
        });
      }
    } finally {
      evictionLock.unlock();
    }
  }

  private void logGuardedPagesSample() {
    int logged = 0;
    for (V page : map.values()) {
      if (page.getGuardCount() > 0) {
        LOGGER.warn("Guarded page prevents eviction: key={} type={} rev={} guards={} hot={}", page.getPageKey(),
            page.getIndexType(), page.getRevision(), page.getGuardCount(), page.isHot());
        if (++logged >= 5) {
          break;
        }
      }
    }
    if (logged == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug("No guarded pages found, but cache over budget. CurrentWeight={} MaxWeight={}",
          currentWeightBytes.get(), maxWeightBytes);
    }
  }

  public String getDiagnostics() {
    long totalPages = 0;
    long totalMemory = 0;
    long hotPages = 0;

    for (V page : map.values()) {
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

