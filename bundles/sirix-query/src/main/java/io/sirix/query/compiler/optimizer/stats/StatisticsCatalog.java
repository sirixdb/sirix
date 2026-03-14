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
 * <p>Thread-safety: all methods are synchronized. Reads are infrequent
 * (once per query optimization per field) so lock contention is negligible.</p>
 *
 * <p>Singleton: a single catalog serves all compile chains in the same JVM,
 * matching PostgreSQL's shared pg_statistic catalog.</p>
 */
public final class StatisticsCatalog {

  /** Maximum number of histogram entries. Prevents unbounded growth in long-running servers. */
  static final int MAX_ENTRIES = 4096;

  private static final StatisticsCatalog INSTANCE = new StatisticsCatalog();

  /**
   * LRU cache: access-order LinkedHashMap with automatic eviction at MAX_ENTRIES.
   * Wrapped in Collections.synchronizedMap for thread safety.
   */
  private final Map<CatalogKey, Histogram> histograms;

  private StatisticsCatalog() {
    this.histograms = Collections.synchronizedMap(
        new LinkedHashMap<CatalogKey, Histogram>(256, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<CatalogKey, Histogram> eldest) {
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
    histograms.put(new CatalogKey(databaseName, resourceName, pathString), histogram);
  }

  /**
   * Look up a histogram for a specific (database, resource, path) triple.
   *
   * @return the histogram, or {@code null} if none registered
   */
  public Histogram get(String databaseName, String resourceName, String pathString) {
    if (databaseName == null || resourceName == null || pathString == null) {
      return null;
    }
    return histograms.get(new CatalogKey(databaseName, resourceName, pathString));
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
    return histograms.remove(new CatalogKey(databaseName, resourceName, pathString));
  }

  /**
   * Remove all histograms for a given database and resource.
   * Useful when data changes invalidate statistics.
   */
  public void invalidate(String databaseName, String resourceName) {
    // Synchronize on the map for safe iteration
    synchronized (histograms) {
      histograms.keySet().removeIf(key ->
          key.databaseName.equals(databaseName) && key.resourceName.equals(resourceName));
    }
  }

  /**
   * Clear all registered histograms.
   */
  public void clear() {
    histograms.clear();
  }

  /**
   * Get the number of registered histograms.
   */
  public int size() {
    return histograms.size();
  }

  /**
   * Composite key for the catalog. Uses record for correct equals/hashCode.
   */
  private record CatalogKey(String databaseName, String resourceName, String pathString) {}
}
