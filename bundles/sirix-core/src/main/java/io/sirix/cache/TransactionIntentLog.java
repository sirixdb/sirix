package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.PageFragmentKey;
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

    // CRITICAL FIX: Also remove all page fragments from cache!
    List<PageFragmentKey> pageFragments = key.getPageFragments();
    if (pageFragments != null && !pageFragments.isEmpty()) {
      for (PageFragmentKey fragmentKey : pageFragments) {
        PageReference fragmentRef = new PageReference().setKey(fragmentKey.key());
        bufferManager.getPageCache().remove(fragmentRef);
      }
    }

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey);

    list.add(value);
    logKey++;
  }

  /**
   * Clears the cache.
   * Optimized version without diagnostic logging overhead (5% CPU savings).
   */
  public void clear() {
    logKey = 0;
    
    // Fast path - no diagnostic logging or complex type tracking
    for (final PageContainer pageContainer : list) {
      Page complete = pageContainer.getComplete();
      Page modified = pageContainer.getModified();
      
      if (complete != null) {
        if (complete instanceof KeyValueLeafPage completePage) {
          KeyValueLeafPagePool.getInstance().returnPage(completePage);
        }
        complete.clear();
      }
      
      if (modified != null) {
        if (modified instanceof KeyValueLeafPage modifiedPage) {
          KeyValueLeafPagePool.getInstance().returnPage(modifiedPage);
        }
        modified.clear();
      }
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
    // Clear pages thoroughly first, then return KeyValueLeafPages to pool
    for (final PageContainer pageContainer : list) {
      Page completePage = pageContainer.getComplete();
      Page modifiedPage = pageContainer.getModified();
      
      // CRITICAL: Clear pages completely before returning to pool
      // This ensures no data leakage between transactions
      if (completePage != null) {
        completePage.clear();
      }
      if (modifiedPage != null) {
        modifiedPage.clear();
      }
      
      // Return to pool after clearing (pinCount is now 0)
      if (completePage instanceof KeyValueLeafPage kvCompletePage) {
        KeyValueLeafPagePool.getInstance().returnPage(kvCompletePage);
      }
      
      if (modifiedPage instanceof KeyValueLeafPage kvModifiedPage) {
        KeyValueLeafPagePool.getInstance().returnPage(kvModifiedPage);
      }
    }
    logKey = 0;
    list.clear();
  }
}
