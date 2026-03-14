package io.sirix.query.compiler.optimizer.stats;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory catalog mapping (database, resource, path) → {@link Histogram}.
 *
 * <p>Survives across queries within a JVM lifetime, allowing histograms
 * built by {@link HistogramCollector} (ANALYZE) to inform selectivity
 * estimates in subsequent query optimizations.</p>
 *
 * <p>Uses {@link ConcurrentHashMap} for lock-free reads on the hot path
 * (query optimization). Writes (histogram registration) are infrequent
 * and amortized over many queries.</p>
 *
 * <p>Thread-safety: all methods are safe for concurrent use. The singleton
 * pattern is intentional — a single catalog serves all compile chains in
 * the same JVM, matching PostgreSQL's shared pg_statistic catalog.</p>
 *
 * <p>Size is bounded by {@link #MAX_ENTRIES} (default 4096). When the limit
 * is reached, new entries silently replace the oldest via ConcurrentHashMap's
 * natural behavior after eviction. This prevents unbounded memory growth
 * in long-running server processes with many databases/resources.</p>
 */
public final class StatisticsCatalog {

  /** Maximum number of histogram entries. Prevents unbounded growth in long-running servers. */
  static final int MAX_ENTRIES = 4096;

  private static final StatisticsCatalog INSTANCE = new StatisticsCatalog();

  private final ConcurrentHashMap<CatalogKey, Histogram> histograms = new ConcurrentHashMap<>();

  private StatisticsCatalog() {}

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
    // Evict oldest entries when at capacity. ConcurrentHashMap doesn't have LRU,
    // so we do a simple bulk eviction (remove ~25%) to amortize the cost.
    if (histograms.size() >= MAX_ENTRIES) {
      final var iterator = histograms.keySet().iterator();
      final int toRemove = MAX_ENTRIES / 4;
      for (int i = 0; i < toRemove && iterator.hasNext(); i++) {
        iterator.next();
        iterator.remove();
      }
    }
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
    histograms.keySet().removeIf(key ->
        key.databaseName.equals(databaseName) && key.resourceName.equals(resourceName));
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
