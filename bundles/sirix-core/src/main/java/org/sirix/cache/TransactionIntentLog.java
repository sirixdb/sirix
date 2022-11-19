package org.sirix.cache;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.index.IndexType;
import org.sirix.page.*;
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
   * Maps in-memory key to persistent key and vice versa.
   */
  private final Map<Integer, Long> mapToPersistentLogKey;

  /**
   * The reference to the second cache.
   */
  private final PersistentFileCache secondCache;

  /**
   * The log key.
   */
  private int logKey;

  private PageReadOnlyTrx pageReadOnlyTrx;

  /**
   * Creates a new transaction intent log.
   *
   * @param secondCache         the reference to the second {@link Cache} where the data is stored when it
   *                            gets removed from the first one.
   * @param maxInMemoryCapacity the maximum size of the in-memory map
   */
  public TransactionIntentLog(final PersistentFileCache secondCache,
      final int maxInMemoryCapacity) {
    // Assertion instead of checkNotNull(...).
    assert secondCache != null;
    logKey = 0;
    this.secondCache = secondCache;
    mapToPersistentLogKey = new HashMap<>(maxInMemoryCapacity >> 1);
    map = new LinkedHashMap<>(maxInMemoryCapacity >> 1) {
      private static final long serialVersionUID = 1;

      @Override
      protected boolean removeEldestEntry(final Map.Entry<PageReference, PageContainer> eldest) {
        if (size() > maxInMemoryCapacity) {
          int i = 0;
          final var iter = map.entrySet().iterator();
          final int size = size();
          while (iter.hasNext() && i < (size / 2)) {
            final Map.Entry<PageReference, PageContainer> entry = iter.next();

            if (isImportant(entry))
              continue;

            i++;
            final PageReference key = entry.getKey();
            assert key.getLogKey() != Constants.NULL_ID_INT;
            PageContainer value = entry.getValue();

            if (value != null) {
              iter.remove();
              TransactionIntentLog.this.secondCache.put(pageReadOnlyTrx, key, value);
              //noinspection UnusedAssignment
              value = null;
              mapToPersistentLogKey.put(key.getLogKey(), key.getPersistentLogKey());
            }
          }
        }
        return false;
      }

      private boolean isImportant(Map.Entry<PageReference, PageContainer> eldest) {
        final var page = eldest.getValue().getComplete();
        if (page instanceof RevisionRootPage || page instanceof NamePage || page instanceof CASPage
            || page instanceof PathPage || page instanceof PathSummaryPage || page instanceof UberPage) {
          return true;
        } else if (page instanceof UnorderedKeyValuePage dataPage) {
          return dataPage.getIndexType() != IndexType.DOCUMENT;
        }
        return false;
      }
    };
  }

  public TransactionIntentLog setPageReadOnlyTrx(PageReadOnlyTrx pageReadOnlyTrx) {
    this.pageReadOnlyTrx = pageReadOnlyTrx;
    return this;
  }

  /**
   * Retrieves an entry from the cache.<br>
   *
   * @param key the key whose associated value is to be returned.
   * @return the value associated to this key, or {@code null} if no value with this key exists in the
   * cache
   */
  public PageContainer get(final PageReference key, final PageReadOnlyTrx pageRtx) {
    PageContainer value = map.get(key);
    if (value == null) {
      if (key.getLogKey() != Constants.NULL_ID_INT) {
        final Long persistentKey = mapToPersistentLogKey.get(key.getLogKey());
        if (persistentKey != null)
          key.setPersistentLogKey(persistentKey);
      }
      value = secondCache.get(pageRtx, key);
      if (value != null && !PageContainer.emptyInstance().equals(value)) {
        mapToPersistentLogKey.remove(key.getLogKey());
        key.setPersistentLogKey(Constants.NULL_ID_LONG);
        //key.setPage(value.getModified());
        put(key, value);
      }
    }
    return value;
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
    key.setPersistentLogKey(Constants.NULL_ID_LONG);
    map.put(key, value);
  }

  /**
   * Removes an entry from this cache.
   *
   * @param key the key with which the specified value is to be associated
   */
  public void remove(final PageReference key) {
    map.remove(key);
    mapToPersistentLogKey.remove(key.getLogKey());
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
    secondCache.close();
    mapToPersistentLogKey.clear();
    map.clear();
    return this;
  }

  @Override
  public void close() {
    map.clear();
    secondCache.close();
  }
}
