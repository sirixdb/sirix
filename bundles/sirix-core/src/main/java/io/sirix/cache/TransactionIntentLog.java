package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * The transaction intent log, used for caching everything the read/write-transaction changes.
 *
 * @author Johannes Lichtenberger
 */
public final class TransactionIntentLog implements AutoCloseable {

  /**
   * The collection to hold the maps.
   */
  private final List<PageContainer> list;

  /**
   * The buffer manager.
   */
  private final BufferManager bufferManager;

  /**
   * The log key.
   */
  private int logKey;

  /**
   * Creates a new transaction intent log.
   *
   * @param maxInMemoryCapacity the maximum size of the in-memory map
   */
  public TransactionIntentLog(final BufferManager bufferManager, final int maxInMemoryCapacity) {
    this.bufferManager = bufferManager;
    logKey = 0;
    list = new ArrayList<>(maxInMemoryCapacity);
  }

  /**
   * Retrieves an entry from the cache.<br>
   *
   * @param key the key whose associated value is to be returned.
   * @return the value associated to this key, or {@code null} if no value with this key exists in the
   * cache
   */
  public PageContainer get(final PageReference key) {
    var logKey = key.getLogKey();
    if ((logKey >= this.logKey) || logKey < 0) {
      return null;
    }
    return list.get(logKey);
  }

  /**
   * Adds an entry to this cache. If the cache is full, the LRU (least recently used) entry is
   * dropped.
   *
   * @param key   the key with which the specified value is to be associated
   * @param value a value to be associated with the specified key
   */
  public void put(final PageReference key, final PageContainer value) {
    bufferManager.getRecordPageCache().remove(key);
    bufferManager.getPageCache().remove(key);

    if (value.getComplete() instanceof KeyValueLeafPage) {
      assert value.getCompleteAsUnorderedKeyValuePage().getPinCount() == 0;
      assert value.getModifiedAsUnorderedKeyValuePage().getPinCount() == 0;
    }

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey);

    list.add(value);
    logKey++;
  }

  /**
   * Clears the cache.
   */
  public void clear() {
    logKey = 0;
    for (final PageContainer pageContainer : list) {
      pageContainer.getComplete().clear();
      pageContainer.getModified().clear();
    }
    list.clear();
  }

  /**
   * Get a view of the underlying map.
   *
   * @return an unmodifiable view of all entries in the cache
   */
  public List<PageContainer> getList() {
    return list;
  }

  @Override
  public void close() {
    logKey = 0;
    list.clear();
  }
}
