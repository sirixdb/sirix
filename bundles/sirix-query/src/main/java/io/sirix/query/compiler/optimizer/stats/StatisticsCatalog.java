package io.sirix.query.compiler.optimizer.stats;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Thread-safe in-memory catalog mapping (database, resource, path, revision) → {@link Histogram}.
 *
 * <p>Revision-aware for bitemporal databases: historical revisions are immutable,
 * so their histograms never expire. Only the "latest" revision ({@code revision == -1})
 * entries are subject to TTL expiry and write-triggered invalidation.</p>
 *
 * <p>Uses a synchronized {@link LinkedHashMap} in access-order mode for
 * true LRU eviction. Entries expire after a configurable TTL (default: 1 hour)
 * for latest-revision entries. Historical revision entries only evict via LRU.</p>
 *
 * <p>Singleton: a single catalog serves all compile chains in the same JVM.</p>
 */
public final class StatisticsCatalog {

  /** Maximum number of histogram entries. Prevents unbounded growth in long-running servers. */
  static final int MAX_ENTRIES = 4096;

  /** Default TTL in milliseconds (1 hour). Only applies to latest-revision entries. */
  static final long DEFAULT_TTL_MILLIS = 3_600_000L;

  /** Revision value indicating "most recent" (mutable — subject to TTL and invalidation). */
  public static final int LATEST_REVISION = -1;

  private static final StatisticsCatalog INSTANCE = new StatisticsCatalog();

  private volatile long ttlMillis = DEFAULT_TTL_MILLIS;

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

  public static StatisticsCatalog getInstance() {
    return INSTANCE;
  }

  public void setTtlMillis(long ttlMillis) {
    if (ttlMillis <= 0) {
      throw new IllegalArgumentException("TTL must be positive: " + ttlMillis);
    }
    this.ttlMillis = ttlMillis;
  }

  public long getTtlMillis() {
    return ttlMillis;
  }

  // --- Revision-aware API (primary) ---

  /**
   * Register a histogram for a specific (database, resource, path, revision) tuple.
   *
   * @param databaseName the database name
   * @param resourceName the resource name
   * @param pathString   the JSON path string (e.g., "price")
   * @param revision     the revision number, or {@link #LATEST_REVISION} for most recent
   * @param histogram    the histogram to register
   */
  public void put(String databaseName, String resourceName, String pathString,
                  int revision, Histogram histogram) {
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(resourceName, "resourceName");
    Objects.requireNonNull(pathString, "pathString");
    Objects.requireNonNull(histogram, "histogram");
    entries.put(new CatalogKey(databaseName, resourceName, pathString, revision),
        new CatalogEntry(histogram, System.currentTimeMillis()));
  }

  /**
   * Look up a histogram for a specific (database, resource, path, revision) tuple.
   *
   * <p>For historical revisions (revision > 0), entries never expire via TTL
   * since the underlying data is immutable. Only {@link #LATEST_REVISION}
   * entries are subject to TTL expiry.</p>
   *
   * @return the histogram, or {@code null} if none registered or expired
   */
  public Histogram get(String databaseName, String resourceName, String pathString, int revision) {
    if (databaseName == null || resourceName == null || pathString == null) {
      return null;
    }
    final var key = new CatalogKey(databaseName, resourceName, pathString, revision);
    synchronized (entries) {
      final CatalogEntry entry = entries.get(key);
      if (entry == null) {
        return null;
      }
      // TTL only applies to latest-revision entries (mutable data)
      if (revision == LATEST_REVISION
          && System.currentTimeMillis() - entry.createdAtMillis > ttlMillis) {
        entries.remove(key);
        return null;
      }
      return entry.histogram;
    }
  }

  // --- Backward-compatible API (defaults to LATEST_REVISION) ---

  /**
   * Register a histogram for the latest revision.
   */
  public void put(String databaseName, String resourceName, String pathString, Histogram histogram) {
    put(databaseName, resourceName, pathString, LATEST_REVISION, histogram);
  }

  /**
   * Look up a histogram for the latest revision.
   */
  public Histogram get(String databaseName, String resourceName, String pathString) {
    return get(databaseName, resourceName, pathString, LATEST_REVISION);
  }

  /**
   * Remove a histogram for a specific (database, resource, path, revision) tuple.
   */
  public Histogram remove(String databaseName, String resourceName, String pathString, int revision) {
    if (databaseName == null || resourceName == null || pathString == null) {
      return null;
    }
    final CatalogEntry removed = entries.remove(
        new CatalogKey(databaseName, resourceName, pathString, revision));
    return removed != null ? removed.histogram : null;
  }

  /**
   * Remove a latest-revision histogram.
   */
  public Histogram remove(String databaseName, String resourceName, String pathString) {
    return remove(databaseName, resourceName, pathString, LATEST_REVISION);
  }

  /**
   * Remove all latest-revision histograms for a given database and resource.
   * Historical revision histograms are preserved (immutable data).
   */
  public void invalidate(String databaseName, String resourceName) {
    synchronized (entries) {
      entries.keySet().removeIf(key ->
          key.databaseName.equals(databaseName)
              && key.resourceName.equals(resourceName)
              && key.revision == LATEST_REVISION);
    }
  }

  /**
   * Remove ALL histograms (including historical) for a database and resource.
   */
  public void invalidateAll(String databaseName, String resourceName) {
    synchronized (entries) {
      entries.keySet().removeIf(key ->
          key.databaseName.equals(databaseName)
              && key.resourceName.equals(resourceName));
    }
  }

  public void clear() {
    entries.clear();
  }

  public int size() {
    return entries.size();
  }

  /**
   * Composite key for the catalog. Historical revisions (revision > 0) produce
   * distinct cache entries from latest-revision (revision == -1).
   */
  private record CatalogKey(String databaseName, String resourceName,
                             String pathString, int revision) {}

  private record CatalogEntry(Histogram histogram, long createdAtMillis) {}
}
