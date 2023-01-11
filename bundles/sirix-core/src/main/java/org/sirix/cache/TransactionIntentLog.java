package org.sirix.cache;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.sirix.page.PageReference;
import org.sirix.settings.Constants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * The transaction intent log, used for logging everything a write transaction changes.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class TransactionIntentLog implements AutoCloseable {

  /**
   * The collection to hold the maps.
   */
  private final Long2ObjectMap<PageContainer> map;

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
    map = new Long2ObjectOpenHashMap<>(maxInMemoryCapacity); // TODO: right size
  }

  /**
   * Retrieves an entry from the cache.<br>
   *
   * @param key the key whose associated value is to be returned.
   * @return the value associated to this key, or {@code null} if no value with this key exists in the
   * cache
   */
  public PageContainer get(final PageReference key) {
    return map.get(key.getLogKey());
  }

  /**
   * Adds an entry to this cache. If the cache is full, the LRU (least recently used) entry is
   * dropped.
   *
   * @param key   the key with which the specified value is to be associated
   * @param value a value to be associated with the specified key
   */
  public void put(final PageReference key, final PageContainer value) {
    map.remove(key.getLogKey());

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey);

    map.put(logKey, value);
    logKey++;
  }

  /**
   * Removes an entry from this cache.
   *
   * @param key the key with which the specified value is to be associated
   */
  public void remove(final PageReference key) {
    map.remove(key.getLogKey());
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    logKey = 0;
    map.clear();
  }

  /**
   * Returns a {@code Collection} that contains a copy of all cache entries.
   *
   * @return a {@code Collection} with a copy of the cache content
   */
  public Collection<Map.Entry<? super Long, ? super PageContainer>> getAll() {
    return new ArrayList<>(map.long2ObjectEntrySet());
  }

  /**
   * Get a view of the underlying map.
   *
   * @return an unmodifiable view of all entries in the cache
   */
  public Long2ObjectMap<PageContainer> getMap() {
    return map;
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
