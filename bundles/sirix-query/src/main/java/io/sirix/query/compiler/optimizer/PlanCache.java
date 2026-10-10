package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.sirix.query.compiler.AstSupport;
import io.brackit.query.module.StaticContext;

import java.util.LinkedHashMap;
import java.util.Map;
import io.sirix.index.IndexSchemaVersion;

/**
 * LRU cache for optimized query plans (ASTs).
 *
 * <p>
 * Maps query template strings to their optimized ASTs. When the same query pattern hits the
 * optimizer again, the cached AST is returned immediately, bypassing the full 10-stage optimization
 * pipeline.
 * </p>
 *
 * <p>
 * The optimizer supplies the complete query/prolog and target identity, plus the core schema
 * version. AST.toString() is only a node label and must never be used as a query identity. Each
 * Sirix compile chain owns one cache, so store bindings cannot leak between chains.
 * </p>
 *
 * <p>
 * Thread-safety: all access is synchronized. The cache is used during query optimization which is
 * inherently sequential per-query, so contention is negligible.
 * </p>
 *
 * <p>
 * Eviction: LRU (least-recently-used) via {@link LinkedHashMap} with access-order. Default capacity
 * is 128 plans.
 * </p>
 *
 * <p>
 * <b>Important:</b> AST nodes are mutable. The {@link #get} method returns a deep copy of the
 * cached AST to prevent downstream stages from mutating the cache entry.
 * </p>
 */
public final class PlanCache {

  /** Default maximum number of cached plans. */
  public static final int DEFAULT_MAX_SIZE = 128;

  /**
   * Signal that the index schema has changed (index created, dropped, or modified). All plan caches
   * will produce misses for plans cached before this call.
   *
   * <p>
   * Call this from index-modifying operations (jn:create-cas-index, etc.).
   * </p>
   */
  public static void signalIndexSchemaChange() {
    IndexSchemaVersion.changed();
  }

  /**
   * Get the current index schema version. Used to build version-aware cache keys.
   */
  public static long indexSchemaVersion() {
    return IndexSchemaVersion.current();
  }

  /** Reference identity fences targets whose caller cannot supply the owning prolog. */
  private record CacheKey(String namespace, String text, StaticContext context) {
    @Override
    public boolean equals(final Object other) {
      return other instanceof CacheKey key && namespace.equals(key.namespace) && text.equals(key.text)
          && context == key.context;
    }

    @Override
    public int hashCode() {
      return 31 * (31 * namespace.hashCode() + text.hashCode()) + System.identityHashCode(context);
    }
  }

  private final Map<CacheKey, AST> cache;
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
      protected boolean removeEldestEntry(Map.Entry<CacheKey, AST> eldest) {
        return size() > maxSize;
      }
    };
  }

  /**
   * Look up a cached optimized AST for the given query template.
   *
   * <p>
   * Returns a deep copy to prevent downstream stages from mutating the cache entry. The copy cost is
   * negligible compared to the 10-stage optimization pipeline it replaces on cache hits.
   * </p>
   *
   * @param queryTemplate the query template string
   * @return a deep copy of the cached AST, or {@code null} if not found
   */
  public synchronized AST get(String queryTemplate) {
    return get(queryTemplate, null);
  }

  /** Lookup scoped to one analysis when no full-query namespace is supplied by the caller. */
  synchronized AST get(final String queryTemplate, final StaticContext context) {
    return get("", queryTemplate, context);
  }

  /** Share the full-query fingerprint across target keys instead of copying it into every string. */
  synchronized AST get(final String namespace, final String queryTemplate, final StaticContext context) {
    final AST cached = queryTemplate == null
        ? null
        : cache.get(new CacheKey(namespace, queryTemplate, context));
    if (cached != null) {
      hits++;
      return AstSupport.copyTree(cached);
    }
    misses++;
    return null;
  }

  /**
   * Cache an optimized AST for the given query template.
   *
   * @param queryTemplate the query template string
   * @param optimizedAst the optimized AST to cache
   */
  public synchronized void put(String queryTemplate, AST optimizedAst) {
    put(queryTemplate, null, optimizedAst);
  }

  synchronized void put(final String queryTemplate, final StaticContext context, final AST optimizedAst) {
    put("", queryTemplate, context, optimizedAst);
  }

  synchronized void put(final String namespace, final String queryTemplate, final StaticContext context,
      final AST optimizedAst) {
    if (queryTemplate != null && optimizedAst != null) {
      cache.put(new CacheKey(namespace, queryTemplate, context), optimizedAst);
    }
  }

  /**
   * Remove a cached plan.
   */
  public synchronized void invalidate(String queryTemplate) {
    cache.keySet().removeIf(key -> key.text.equals(queryTemplate));
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
    return total > 0
        ? (double) hits / total
        : 0.0;
  }
}
