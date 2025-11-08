package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The transaction intent log, used for caching everything the read/write-transaction changes.
 *
 * @author Johannes Lichtenberger
 */
public final class TransactionIntentLog implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionIntentLog.class);

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
    
    // DIAGNOSTIC: Track TIL creation
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("TIL CREATED: instance={} (maxCapacity={})", System.identityHashCode(this), maxInMemoryCapacity);
    }
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
    // CRITICAL: Remove from ALL caches BEFORE mutating the PageReference
    // This prevents double-close: cache eviction → close(), then TIL.close() → close()
    // Pages in TIL must NOT be in any cache - TIL owns them exclusively
    
    // Remove from RecordPageCache (full pages)
    bufferManager.getRecordPageCache().remove(key);
    
    // Remove from RecordPageFragmentCache (fragments)
    bufferManager.getRecordPageFragmentCache().remove(key);

    // Remove all page fragments from cache
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
    
    // Remove from PageCache (other page types)
    bufferManager.getPageCache().remove(key);

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey);

    list.add(value);
    logKey++;
    
    // Diagnostic logging for leak detection
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      if (value.getComplete() instanceof KeyValueLeafPage completePage) {
        LOGGER.debug("TIL.put: complete page {} type={} rev={}", 
            completePage.getPageKey(), completePage.getIndexType(), completePage.getRevision());
      }
      if (value.getModified() instanceof KeyValueLeafPage modifiedPage && modifiedPage != value.getComplete()) {
        LOGGER.debug("TIL.put: modified page {} type={} rev={}", 
            modifiedPage.getPageKey(), modifiedPage.getIndexType(), modifiedPage.getRevision());
      }
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
    int closedComplete = 0;
    int closedModified = 0;
    int skippedAlreadyClosed = 0;
    
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("TIL.clear() starting with {} containers", list.size());
    }
    
    logKey = 0;
    
    // CRITICAL: Force completion of ALL pending async removal listeners
    // Pages were removed from caches in put() - we must wait for async listeners
    // to finish before we close pages in TIL, otherwise double-close
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();
    
    // Close all pages in TIL
    // CRITICAL: Must force-unpin before closing to release memory segments
    int totalContainers = list.size();
    int kvLeafContainers = 0;
    
    for (final PageContainer pageContainer : list) {
      Page complete = pageContainer.getComplete();
      Page modified = pageContainer.getModified();
      
      if (complete instanceof KeyValueLeafPage completePage) {
        kvLeafContainers++;
        
        if (!completePage.isClosed()) {
          // CRITICAL: Pages in TIL were already removed from cache in TIL.put()
          // We MUST close them here since they won't be closed by cache RemovalListener
          forceUnpinAll(completePage);
          completePage.close();
          closedComplete++;
        } else {
          skippedAlreadyClosed++;
        }
      }
      
      // Check if modified is a different instance before closing
      if (modified instanceof KeyValueLeafPage modifiedPage && modified != complete) {
        if (!modifiedPage.isClosed()) {
          // Same logic - pages in TIL must be closed here
          forceUnpinAll(modifiedPage);
          modifiedPage.close();
          closedModified++;
        } else {
          skippedAlreadyClosed++;
        }
      }
    }
    list.clear();
    
    LOGGER.debug("TIL.clear() processed {} containers ({} KeyValueLeafPages), closed {} complete + {} modified pages, skipped {} already closed",
        totalContainers, kvLeafContainers, closedComplete, closedModified, skippedAlreadyClosed);
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
    int initialSize = list.size();
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("TIL.close() starting with {} containers", initialSize);
    }
    
    // CRITICAL: Force completion of ALL pending async removal listeners
    // Pages were removed from caches in put() - we must wait for async listeners
    // to finish before we close pages in TIL, otherwise double-close
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();
    
    // Close pages to release segments
    // CRITICAL: Must force-unpin before closing to release memory segments
    int closedComplete = 0;
    int closedModified = 0;
    
    for (final PageContainer pageContainer : list) {
      Page completePage = pageContainer.getComplete();
      Page modifiedPage = pageContainer.getModified();
      
      if (completePage instanceof KeyValueLeafPage kvCompletePage) {
        if (!kvCompletePage.isClosed()) {
          forceUnpinAll(kvCompletePage);
          kvCompletePage.close();
          closedComplete++;
        }
      }
      
      // Check if modified is a different instance before closing
      if (modifiedPage instanceof KeyValueLeafPage kvModifiedPage && modifiedPage != completePage) {
        if (!kvModifiedPage.isClosed()) {
          forceUnpinAll(kvModifiedPage);
          kvModifiedPage.close();
          closedModified++;
        }
      }
    }
    
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("TIL.close() closed {} complete + {} modified pages (from {} containers)", 
          closedComplete, closedModified, initialSize);
    }
    
    logKey = 0;
    list.clear();
  }
  
  @Override
  @SuppressWarnings("deprecation")
  protected void finalize() {
    // DIAGNOSTIC: Detect if TIL is GC'd without being closed
    if (!list.isEmpty() && KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.warn("⚠️  TIL FINALIZED WITHOUT CLEAR: instance={} with {} containers still in list!", 
          System.identityHashCode(this), list.size());
    }
  }
}




