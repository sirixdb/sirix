package io.sirix.query.compiler.optimizer.stats;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Thread-safe in-memory catalog mapping (database, resource, path) → {@link Histogram}.
 *
 * <p>Survives across queries within a JVM lifetime, allowing histograms
 * built by {@link HistogramCollector} (ANALYZE) to inform selectivity
 * estimates in subsequent query optimizations.</p>
 *
 * <p>Uses a synchronized {@link LinkedHashMap} in access-order mode for
 * true LRU eviction. Reads and writes both update the access order, so
 * frequently-used histograms survive eviction. This is preferable to
 * ConcurrentHashMap's random-order eviction which can evict hot entries.</p>
 *
 * <p>Entries expire after a configurable TTL (default: 1 hour). Expired
 * entries are lazily evicted on access — no background thread needed.</p>
 *
 * <p>Thread-safety: all methods are synchronized. Reads are infrequent
 * (once per query optimization per field) so lock contention is negligible.</p>
 *
 * <p>Singleton: a single catalog serves all compile chains in the same JVM,
 * matching PostgreSQL's shared pg_statistic catalog.</p>
 */
public final class StatisticsCatalog {

  /** Maximum number of histogram entries. Prevents unbounded growth in long-running servers. */
  static final int MAX_ENTRIES = 4096;

  /** Default TTL in milliseconds (1 hour). After this, entries are lazily evicted. */
  static final long DEFAULT_TTL_MILLIS = 3_600_000L;

  private static final StatisticsCatalog INSTANCE = new StatisticsCatalog();

  /** Volatile: read in get() under entries lock, written in setTtlMillis() without it. */
  private volatile long ttlMillis = DEFAULT_TTL_MILLIS;

  /**
   * LRU cache: access-order LinkedHashMap with automatic eviction at MAX_ENTRIES.
   * Wrapped in Collections.synchronizedMap for thread safety.
   */
  private final Map<CatalogKey, CatalogEntry> entries;

  private StatisticsCatalog() {
    this.entries = Collections.synchronizedMap(
        new LinkedHashMap<CatalogKey, CatalogEntry>(256, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<CatalogKey, CatalogEntry> eldest) {
            return size() > MAX_ENTRIES;
          }
        });
  }

  /**
   * Get the singleton catalog instance.
   */
  public static StatisticsCatalog getInstance() {
    return INSTANCE;
  }

  /**
   * Set the TTL for catalog entries. Entries older than this are lazily evicted.
   *
   * @param ttlMillis TTL in milliseconds (must be positive)
   */
  public void setTtlMillis(long ttlMillis) {
    if (ttlMillis <= 0) {
      throw new IllegalArgumentException("TTL must be positive: " + ttlMillis);
    }
    this.ttlMillis = ttlMillis;
  }

  /**
   * Get the current TTL in milliseconds.
   */
  public long getTtlMillis() {
    return ttlMillis;
  }

  /**
   * Register a histogram for a specific (database, resource, path) triple.
   *
   * @param databaseName the database name
   * @param resourceName the resource name
   * @param pathString   the JSON path string (e.g., "price", "address.city")
   * @param histogram    the histogram to register
   */
  public void put(String databaseName, String resourceName, String pathString, Histogram histogram) {
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(resourceName, "resourceName");
    Objects.requireNonNull(pathString, "pathString");
    Objects.requireNonNull(histogram, "histogram");
    entries.put(new CatalogKey(databaseName, resourceName, pathString),
        new CatalogEntry(histogram, System.currentTimeMillis()));
  }

  /**
   * Look up a histogram for a specific (database, resource, path) triple.
   * Returns null if the entry is expired or not found.
   *
   * @return the histogram, or {@code null} if none registered or expired
   */
  public Histogram get(String databaseName, String resourceName, String pathString) {
    if (databaseName == null || resourceName == null || pathString == null) {
      return null;
    }
    final var key = new CatalogKey(databaseName, resourceName, pathString);
    // Synchronize to avoid TOCTOU between get and conditional remove:
    // without this, another thread could put() a fresh entry between our
    // get() and remove(), and we'd evict the fresh entry.
    synchronized (entries) {
      final CatalogEntry entry = entries.get(key);
      if (entry == null) {
        return null;
      }
      // Lazy TTL eviction
      if (System.currentTimeMillis() - entry.createdAtMillis > ttlMillis) {
        entries.remove(key);
        return null;
      }
      return entry.histogram;
    }
  }

  /**
   * Remove a histogram for a specific (database, resource, path) triple.
   *
   * @return the removed histogram, or {@code null} if none was registered
   */
  public Histogram remove(String databaseName, String resourceName, String pathString) {
    if (databaseName == null || resourceName == null || pathString == null) {
      return null;
    }
    final CatalogEntry removed = entries.remove(new CatalogKey(databaseName, resourceName, pathString));
    return removed != null ? removed.histogram : null;
  }

  /**
   * Remove all histograms for a given database and resource.
   * Useful when data changes invalidate statistics.
   */
  public void invalidate(String databaseName, String resourceName) {
    // Synchronize on the map for safe iteration
    synchronized (entries) {
      entries.keySet().removeIf(key ->
          key.databaseName.equals(databaseName) && key.resourceName.equals(resourceName));
    }
  }

  /**
   * Clear all registered histograms.
   */
  public void clear() {
    entries.clear();
  }

  /**
   * Get the number of registered histograms (including possibly expired ones).
   */
  public int size() {
    return entries.size();
  }

  /**
   * Composite key for the catalog. Uses record for correct equals/hashCode.
   */
  private record CatalogKey(String databaseName, String resourceName, String pathString) {}

  /**
   * Wraps a histogram with its creation timestamp for TTL tracking.
   */
  private record CatalogEntry(Histogram histogram, long createdAtMillis) {}
}
