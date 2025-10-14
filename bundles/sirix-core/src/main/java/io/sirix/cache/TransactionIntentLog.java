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
   */
  public void clear() {
    DiagnosticLogger.log("TransactionIntentLog.clear() called with " + list.size() + " page containers");
    logKey = 0;
    int kvPageCount = 0;
    int otherPageCount = 0;
    int nullPageCount = 0;
    int indirectPageCount = 0;
    int pathPageCount = 0;
    int namePageCount = 0;
    
    for (final PageContainer pageContainer : list) {
      // Track page types
      Page complete = pageContainer.getComplete();
      Page modified = pageContainer.getModified();
      
      if (complete != null) {
        String className = complete.getClass().getSimpleName();
        if (complete instanceof KeyValueLeafPage completePage) {
          kvPageCount++;
          DiagnosticLogger.log("  Returning complete KeyValueLeafPage: " + completePage.getPageKey());
          KeyValueLeafPagePool.getInstance().returnPage(completePage);
        } else if (className.contains("Indirect")) {
          indirectPageCount++;
        } else if (className.contains("Path")) {
          pathPageCount++;
        } else if (className.contains("Name")) {
          namePageCount++;
        } else {
          otherPageCount++;
          DiagnosticLogger.log("  Other complete page type: " + className);
        }
        complete.clear();
      } else {
        nullPageCount++;
      }
      
      if (modified != null) {
        String className = modified.getClass().getSimpleName();
        if (modified instanceof KeyValueLeafPage modifiedPage) {
          kvPageCount++;
          DiagnosticLogger.log("  Returning modified KeyValueLeafPage: " + modifiedPage.getPageKey());
          KeyValueLeafPagePool.getInstance().returnPage(modifiedPage);
        } else if (className.contains("Indirect")) {
          indirectPageCount++;
        } else if (className.contains("Path")) {
          pathPageCount++;
        } else if (className.contains("Name")) {
          namePageCount++;
        } else {
          otherPageCount++;
          DiagnosticLogger.log("  Other modified page type: " + className);
        }
        modified.clear();
      } else {
        nullPageCount++;
      }
    }
    list.clear();
    DiagnosticLogger.log("TransactionIntentLog.clear() completed: " + kvPageCount + " KeyValueLeafPages, " + 
                        indirectPageCount + " IndirectPages, " + pathPageCount + " PathPages, " + 
                        namePageCount + " NamePages, " + otherPageCount + " others, " + nullPageCount + " nulls");
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
