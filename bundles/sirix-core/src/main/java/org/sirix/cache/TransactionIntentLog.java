package org.sirix.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.Key;
import org.sirix.page.CASPage;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.PathPage;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * The transaction intent log, used for logging everything a write transaction changes.
 *
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 */
public final class TransactionIntentLog implements AutoCloseable {

  private boolean mEvict;

  /**
   * The collection to hold the maps.
   */
  private final Map<PageReference, PageContainer> mMap;

  /**
   * Maps in-memory key to persistent key and vice versa.
   */
  private final BiMap<Integer, Long> mMapToPersistentLogKey;

  /**
   * The reference to the second cache.
   */
  private final PersistentFileCache mSecondCache;

  /**
   * The log key.
   */
  private int mLogKey;

  /**
   * Creates a new transaction intent log.
   *
   * @param secondCache         the reference to the second {@link Cache} where the data is stored when it
   *                            gets removed from the first one.
   * @param maxInMemoryCapacity the maximum size of the in-memory map
   */
  public TransactionIntentLog(final PersistentFileCache secondCache, final int maxInMemoryCapacity) {
    // Assertion instead of checkNotNull(...).
    assert secondCache != null;
    mEvict = true;
    mLogKey = 0;
    mSecondCache = secondCache;
    mMapToPersistentLogKey = HashBiMap.create(maxInMemoryCapacity >> 1);
    mMap = new LinkedHashMap<>(maxInMemoryCapacity >> 1) {
      private static final long serialVersionUID = 1;

      @Override
      protected boolean removeEldestEntry(final @Nullable Map.Entry<PageReference, PageContainer> eldest) {
        if (size() > maxInMemoryCapacity && mEvict) {
          int i = 0;
          final var iter = mMap.entrySet().iterator();
          final int size = size();
          while (iter.hasNext() && i < (size / 2)) {
            final Map.Entry<PageReference, PageContainer> entry = iter.next();

            if (isImportant(entry))
              continue;

            i++;
            final PageReference key = entry.getKey();
            assert key.getLogKey() != Constants.NULL_ID_INT;
            PageContainer value = entry.getValue();

            if (key != null && value != null) {
              iter.remove();
              mSecondCache.put(key, value);
              value = null;
              mMapToPersistentLogKey.put(key.getLogKey(), key.getPersistentLogKey());
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
        } else if (page instanceof UnorderedKeyValuePage) {
          var dataPage = (UnorderedKeyValuePage) page;
          if (dataPage.getPageKind() == PageKind.RECORDPAGE)
            return false;
          else
            return true;
        }
        return false;
      }
    };
  }

  public TransactionIntentLog setEvict(boolean evict) {
    mEvict = evict;
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
    PageContainer value = mMap.get(key);
    if (value == null) {
      if (key.getLogKey() != Constants.NULL_ID_INT) {
        final Long persistentKey = mMapToPersistentLogKey.get(key.getLogKey());
        if (persistentKey != null)
          key.setPersistentLogKey(persistentKey);
      }
      value = mSecondCache.get(key, pageRtx);
      if (value != null && !PageContainer.emptyInstance().equals(value)) {
        mMapToPersistentLogKey.remove(key.getPersistentLogKey());
        key.setPersistentLogKey(Constants.NULL_ID_LONG);
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
    if (mMap.containsKey(key)) {
      mMap.remove(key);
    }

    key.setKey(Constants.NULL_ID_LONG);
    key.setLogKey(mLogKey++);
    key.setPersistentLogKey(Constants.NULL_ID_LONG);
    mMap.put(key, value);
  }

  /**
   * Removes an entry from this cache.
   *
   * @param key the key with which the specified value is to be associated
   */
  public void remove(final PageReference key) {
    mMap.remove(key);
    mMapToPersistentLogKey.remove(key);
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    mLogKey = 0;
    mMap.clear();
  }

  /**
   * Returns the number of used entries in the cache.
   *
   * @return the number of entries currently in the cache.
   */
  public int usedEntries() {
    return mMap.size();
  }

  /**
   * Returns a {@code Collection} that contains a copy of all cache entries.
   *
   * @return a {@code Collection} with a copy of the cache content
   */
  public Collection<Map.Entry<? super PageReference, ? super PageContainer>> getAll() {
    return new ArrayList<Map.Entry<? super PageReference, ? super PageContainer>>(mMap.entrySet());
  }

  //  @Override
  //  public String toString() {
  //    return MoreObjects.toStringHelper(this).add("First Cache", mMap).add("Second Cache", mSecondCache).toString();
  //  }

  /**
   * Get a view of the underlying map.
   *
   * @return an unmodifiable view of all entries in the cache
   */
  public Map<PageReference, PageContainer> getMap() {
    return Collections.unmodifiableMap(mMap);
  }

  /**
   * Truncate the log.
   *
   * @return this log instance
   */
  public TransactionIntentLog truncate() {
    mSecondCache.close();
    mMapToPersistentLogKey.clear();
    mMap.clear();
    return this;
  }

  @Override
  public void close() {
    mMap.clear();
    mSecondCache.close();
  }
}
