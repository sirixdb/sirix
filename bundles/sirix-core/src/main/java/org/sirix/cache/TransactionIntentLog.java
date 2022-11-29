package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.settings.Constants;

import java.util.*;

/**
 * The transaction intent log, used for logging everything a write transaction changes.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class TransactionIntentLog implements AutoCloseable {

  /**
   * The collection to hold the maps.
   */
  private final Map<PageReference, PageContainer> map;

  /**
   * The log key.
   */
  private int logKey;

  /**
   * Creates a new transaction intent log.
   *
   * @param maxInMemoryCapacity the maximum size of the in-memory map
   */
  public TransactionIntentLog(final int maxInMemoryCapacity) {
    logKey = 0;
    map = new HashMap<>(maxInMemoryCapacity);
  }

  /**
   * Retrieves an entry from the cache.<br>
   *
   * @param key the key whose associated value is to be returned.
   * @return the value associated to this key, or {@code null} if no value with this key exists in the
   * cache
   */
  public PageContainer get(final PageReference key) {
    return map.get(key);
  }

  /**
   * Adds an entry to this cache. If the cache is full, the LRU (least recently used) entry is
   * dropped.
   *
   * @param key   the key with which the specified value is to be associated
   * @param value a value to be associated with the specified key
   */
  public void put(final PageReference key, final PageContainer value) {
    map.remove(key);

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey++);
    map.put(key, value);
  }

  /**
   * Removes an entry from this cache.
   *
   * @param key the key with which the specified value is to be associated
   */
  public void remove(final PageReference key) {
    map.remove(key);
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    logKey = 0;
    map.clear();
  }

  /**
   * Returns the number of used entries in the cache.
   *
   * @return the number of entries currently in the cache.
   */
  public int usedEntries() {
    return map.size();
  }

  /**
   * Returns a {@code Collection} that contains a copy of all cache entries.
   *
   * @return a {@code Collection} with a copy of the cache content
   */
  public Collection<Map.Entry<? super PageReference, ? super PageContainer>> getAll() {
    return new ArrayList<>(map.entrySet());
  }

  /**
   * Get a view of the underlying map.
   *
   * @return an unmodifiable view of all entries in the cache
   */
  public Map<PageReference, PageContainer> getMap() {
    return Collections.unmodifiableMap(map);
  }

  /**
   * Truncate the log.
   *
   * @return this log instance
   */
  public TransactionIntentLog truncate() {
    map.clear();
    return this;
  }

  @Override
  public void close() {
    map.clear();
  }
}
