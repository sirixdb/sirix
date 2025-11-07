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
    
    // DIAGNOSTIC: Track TIL creation
    if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      io.sirix.cache.DiagnosticLogger.log("TIL CREATED: instance=" + System.identityHashCode(this) + " (maxCapacity=" + maxInMemoryCapacity + ")");
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
    // CRITICAL: Only remove if the page in cache is actually the one we're putting in TIL!
    // Otherwise we'd orphan a different page instance that happens to have the same PageReference
    Page pageInCache = bufferManager.getRecordPageCache().get(key);
    if (pageInCache == value.getComplete() || pageInCache == value.getModified()) {
      bufferManager.getRecordPageCache().remove(key);
    }
    bufferManager.getPageCache().remove(key);
    
    // CRITICAL: Remove the key itself from RecordPageFragmentCache BEFORE mutating it  
    bufferManager.getRecordPageFragmentCache().remove(key);

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

    // CRITICAL FIX: Don't null the page!
    // Some PageReferences (e.g., from RevisionRootPage) are shared and still needed
    // Only null the storage key to prevent reload attempts
    key.setKey(Constants.NULL_ID_LONG);
    // DON'T null page - it might still be accessed via this PageReference!
    // key.setPage(null);
    key.setLogKey(logKey);

    list.add(value);
    logKey++;
    
    // DIAGNOSTIC: Track Page 0 additions to TIL
    if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      if (value.getComplete() instanceof io.sirix.page.KeyValueLeafPage completePage && completePage.getPageKey() == 0) {
        io.sirix.cache.DiagnosticLogger.log("TIL.put Page 0: complete=" + System.identityHashCode(completePage) +
            " type=" + completePage.getIndexType() + " rev=" + completePage.getRevision());
      }
      if (value.getModified() instanceof io.sirix.page.KeyValueLeafPage modifiedPage && 
          modifiedPage != value.getComplete() && modifiedPage.getPageKey() == 0) {
        io.sirix.cache.DiagnosticLogger.log("TIL.put Page 0: modified=" + System.identityHashCode(modifiedPage) +
            " type=" + modifiedPage.getIndexType() + " rev=" + modifiedPage.getRevision());
      }
    }
    
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
    int closedComplete = 0;
    int closedModified = 0;
    int skippedAlreadyClosed = 0;
    int page0Complete = 0;
    int page0Modified = 0;
    int page0SkippedClosed = 0;
    int page0ActuallyClosed = 0;
    
    if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      io.sirix.cache.DiagnosticLogger.log("TIL.clear() starting with " + list.size() + " containers");
    }
    
    logKey = 0;
    
    // Close all pages in TIL
    // CRITICAL: Must force-unpin before closing to release memory segments
    int totalContainers = list.size();
    int kvLeafContainers = 0;
    
    for (final PageContainer pageContainer : list) {
      Page complete = pageContainer.getComplete();
      Page modified = pageContainer.getModified();
      
      if (complete instanceof KeyValueLeafPage completePage) {
        kvLeafContainers++;
        boolean isPage0 = completePage.getPageKey() == 0;
        if (isPage0) {
          page0Complete++;
          if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
            io.sirix.cache.DiagnosticLogger.log("TIL.clear checking complete Page 0: instance=" + 
                System.identityHashCode(completePage) + " isClosed=" + completePage.isClosed());
          }
        }
        
        if (!completePage.isClosed()) {
          // CRITICAL: Pages in TIL were already removed from cache in TIL.put()
          // We MUST close them here since they won't be closed by cache RemovalListener
          forceUnpinAll(completePage);
          completePage.close();
          closedComplete++;
          if (isPage0) page0ActuallyClosed++;
        } else {
          skippedAlreadyClosed++;
          if (isPage0) page0SkippedClosed++;
        }
      }
      
      // Check if modified is a different instance before closing
      if (modified instanceof KeyValueLeafPage modifiedPage && modified != complete) {
        boolean isPage0 = modifiedPage.getPageKey() == 0;
        if (isPage0) page0Modified++;
        
        if (!modifiedPage.isClosed()) {
          // Same logic - pages in TIL must be closed here
          forceUnpinAll(modifiedPage);
          modifiedPage.close();
          closedModified++;
          if (isPage0) page0ActuallyClosed++;
        } else {
          skippedAlreadyClosed++;
          if (isPage0) page0SkippedClosed++;
        }
      }
    }
    list.clear();
    
    if (page0Complete > 0 || page0Modified > 0 || closedComplete + closedModified > 10) {
      System.err.println("TIL.clear() " + totalContainers + " containers (" + kvLeafContainers + " KVLeaf), Page0: " + 
          page0Complete + " complete + " + page0Modified + 
          " modified, closed " + page0ActuallyClosed + ", skipped " + page0SkippedClosed + " already closed" +
          " (total closed: " + closedComplete + "+" + closedModified + ", skipped: " + skippedAlreadyClosed + ")");
    }
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
    // DIAGNOSTIC: Track TIL.close() calls (always log, even if empty)
    int initialSize = list.size();
    if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      io.sirix.cache.DiagnosticLogger.log("TIL.close() starting with " + initialSize + " containers");
    }
    
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
    
    if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      io.sirix.cache.DiagnosticLogger.log("TIL.close() closed " + closedComplete + " complete + " + closedModified + " modified pages (from " + initialSize + " containers)");
    }
    
    logKey = 0;
    list.clear();
  }
  
  @Override
  @SuppressWarnings("deprecation")
  protected void finalize() {
    // DIAGNOSTIC: Detect if TIL is GC'd without being closed
    if (!list.isEmpty() && io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      io.sirix.cache.DiagnosticLogger.log("⚠️  TIL FINALIZED WITHOUT CLEAR: instance=" + System.identityHashCode(this) + 
          " with " + list.size() + " containers still in list!");
    }
  }
}




