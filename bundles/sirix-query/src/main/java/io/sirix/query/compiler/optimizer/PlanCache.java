package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU cache for optimized query plans (ASTs).
 *
 * <p>Maps query template strings to their optimized ASTs. When the same query
 * pattern hits the optimizer again, the cached AST is returned immediately,
 * bypassing the full 10-stage optimization pipeline.</p>
 *
 * <p>Template key: the raw query string. For parameterized queries, callers
 * should normalize constants to produce a stable template (e.g., replace
 * literal values with placeholders). For now, exact query string matching
 * is used — this captures the common case of repeated identical queries
 * (dashboards, monitoring, etc.).</p>
 *
 * <p>Thread-safety: all access is synchronized. The cache is used during
 * query optimization which is inherently sequential per-query, so
 * contention is negligible.</p>
 *
 * <p>Eviction: LRU (least-recently-used) via {@link LinkedHashMap} with
 * access-order. Default capacity is 128 plans.</p>
 *
 * <p><b>Important:</b> AST nodes are mutable. Cached ASTs must be deep-copied
 * before use to prevent downstream stages from mutating the cache entry.
 * The {@link #get} method returns the cached AST directly — the caller
 * is responsible for copying if the AST will be modified.</p>
 */
public final class PlanCache {

  /** Default maximum number of cached plans. */
  public static final int DEFAULT_MAX_SIZE = 128;

  private final Map<String, AST> cache;
  private long hits;
  private long misses;

  public PlanCache() {
    this(DEFAULT_MAX_SIZE);
  }

  public PlanCache(int maxSize) {
    if (maxSize <= 0) {
      throw new IllegalArgumentException("maxSize must be positive: " + maxSize);
    }
    this.cache = new LinkedHashMap<>(maxSize, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<String, AST> eldest) {
        return size() > maxSize;
      }
    };
  }

  /**
   * Look up a cached optimized AST for the given query template.
   *
   * @param queryTemplate the query template string
   * @return the cached AST, or {@code null} if not found
   */
  public synchronized AST get(String queryTemplate) {
    final AST cached = cache.get(queryTemplate);
    if (cached != null) {
      hits++;
    } else {
      misses++;
    }
    return cached;
  }

  /**
   * Cache an optimized AST for the given query template.
   *
   * @param queryTemplate the query template string
   * @param optimizedAst  the optimized AST to cache
   */
  public synchronized void put(String queryTemplate, AST optimizedAst) {
    if (queryTemplate != null && optimizedAst != null) {
      cache.put(queryTemplate, optimizedAst);
    }
  }

  /**
   * Remove a cached plan.
   */
  public synchronized void invalidate(String queryTemplate) {
    cache.remove(queryTemplate);
  }

  /**
   * Clear all cached plans.
   */
  public synchronized void clear() {
    cache.clear();
    hits = 0;
    misses = 0;
  }

  /**
   * Get the number of cached plans.
   */
  public synchronized int size() {
    return cache.size();
  }

  /**
   * Get the cache hit count.
   */
  public synchronized long hits() {
    return hits;
  }

  /**
   * Get the cache miss count.
   */
  public synchronized long misses() {
    return misses;
  }

  /**
   * Get the cache hit ratio.
   *
   * @return hit ratio in [0, 1], or 0 if no queries yet
   */
  public synchronized double hitRatio() {
    final long total = hits + misses;
    return total > 0 ? (double) hits / total : 0.0;
  }
}
