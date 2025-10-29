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
    // DIAGNOSTIC: Track PATH_SUMMARY pages being added to TIL
    boolean isPathSummary = false;
    if (Boolean.getBoolean("sirix.debug.path.summary")) {
      Page page = value.getComplete() != null ? value.getComplete() : value.getModified();
      if (page instanceof KeyValueLeafPage kvp && kvp.getIndexType() == io.sirix.index.IndexType.PATH_SUMMARY) {
        isPathSummary = true;
        System.err.println("[TIL-PUT] Adding PATH_SUMMARY page to TIL: " +
                           "pageKey=" + kvp.getPageKey() +
                           ", revision=" + kvp.getRevision() +
                           ", logKey=" + logKey);
      }
    }
    
    // Remove from cache (pages must stay OPEN in TIL for serialization)
    // Removal listener will skip closing if pinned (handled in cache listener logic)
    bufferManager.getRecordPageCache().remove(key);
    bufferManager.getPageCache().remove(key);

    // CRITICAL FIX: Also remove all page fragments from cache!
    List<PageFragmentKey> pageFragments = key.getPageFragments();
    if (pageFragments != null && !pageFragments.isEmpty()) {
      for (PageFragmentKey fragmentKey : pageFragments) {
        PageReference fragmentRef = new PageReference()
            .setKey(fragmentKey.key())
            .setDatabaseId(fragmentKey.databaseId())
            .setResourceId(fragmentKey.resourceId());
        bufferManager.getRecordPageFragmentCache().remove(fragmentRef);
      }
    }

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey);

    list.add(value);
    logKey++;
    
    if (isPathSummary && Boolean.getBoolean("sirix.debug.path.summary")) {
      System.err.println("[TIL-PUT]   -> PATH_SUMMARY page added at logKey=" + (logKey - 1));
    }
  }

  /**
   * Force-unpin all transactions from a page.
   * Called before closing TIL pages to ensure pin counts don't prevent segment release.
   */
  private void forceUnpinAll(KeyValueLeafPage page) {
    var pinsByTrx = new java.util.HashMap<>(page.getPinCountByTransaction());
    for (var entry : pinsByTrx.entrySet()) {
      int trxId = entry.getKey();
      int pinCount = entry.getValue();
      for (int i = 0; i < pinCount; i++) {
        page.decrementPinCount(trxId);
      }
    }
  }

  /**
   * Clears the cache.
   * Force-unpins and closes all pages in TIL.
   */
  public void clear() {
    logKey = 0;
    
    // Close all pages in TIL
    // CRITICAL: Must force-unpin before closing to release memory segments
    for (final PageContainer pageContainer : list) {
      Page complete = pageContainer.getComplete();
      Page modified = pageContainer.getModified();
      
      if (complete instanceof KeyValueLeafPage completePage) {
        if (!completePage.isClosed()) {
          forceUnpinAll(completePage);
          completePage.close();
        }
      }
      
      // Check if modified is a different instance before closing
      if (modified instanceof KeyValueLeafPage modifiedPage && modified != complete) {
        if (!modifiedPage.isClosed()) {
          forceUnpinAll(modifiedPage);
          modifiedPage.close();
        }
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
    // Close pages to release segments
    // CRITICAL: Must force-unpin before closing to release memory segments
    for (final PageContainer pageContainer : list) {
      Page completePage = pageContainer.getComplete();
      Page modifiedPage = pageContainer.getModified();
      
      if (completePage instanceof KeyValueLeafPage kvCompletePage) {
        if (!kvCompletePage.isClosed()) {
          forceUnpinAll(kvCompletePage);
          kvCompletePage.close();
        }
      }
      
      // Check if modified is a different instance before closing
      if (modifiedPage instanceof KeyValueLeafPage kvModifiedPage && modifiedPage != completePage) {
        if (!kvModifiedPage.isClosed()) {
          forceUnpinAll(kvModifiedPage);
          kvModifiedPage.close();
        }
      }
    }
    logKey = 0;
    list.clear();
  }
}


