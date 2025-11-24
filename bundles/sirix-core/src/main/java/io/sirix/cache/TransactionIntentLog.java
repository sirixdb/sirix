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

  // DEBUG FLAG: Enable with -Dsirix.debug.memory.leaks=true
  private static final boolean DEBUG_MEMORY_LEAKS = 
    Boolean.getBoolean("sirix.debug.memory.leaks");

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
    if (DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("[TIL-CREATE] instance={} (maxCapacity={})", System.identityHashCode(this), maxInMemoryCapacity);
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
    
    // CRITICAL FIX: Clear cached hash before cache operations
    // The hash depends on key/logKey which we'll modify below
    key.clearCachedHash();
    
    // Remove from RecordPageCache (full pages only)
    // TIL is taking ownership of the complete page in PageContainer
    bufferManager.getRecordPageCache().remove(key);
    
    // NOTE: We do NOT remove from RecordPageFragmentCache
    // Fragments are SHARED across multiple transactions and revisions
    // Even after combining into a complete page, fragments may still be:
    // 1. In use by other concurrent transactions
    // 2. Needed for other revisions
    // 3. Being accessed by ongoing operations
    // ClockSweeper will evict them when safe (guardCount == 0 && not hot)
    
    // Remove from PageCache (other page types)
    bufferManager.getPageCache().remove(key);

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey);

    list.add(value);
    logKey++;
    
    // CRITICAL: Release guards on pages added to TIL
    // TIL pages are transaction-private and not visible to other transactions
    // so they don't need guard protection anymore
    if (value.getComplete() instanceof KeyValueLeafPage completePage) {
      int guardCount = completePage.getGuardCount();
      if (guardCount > 0) {
        completePage.releaseGuard();
        assert completePage.getGuardCount() == 0 : 
            "Page had guardCount=" + guardCount + ", after release should be 0 but is " + completePage.getGuardCount();
      }
    }
    if (value.getModified() instanceof KeyValueLeafPage modifiedPage && modifiedPage != value.getComplete()) {
      int guardCount = modifiedPage.getGuardCount();
      if (guardCount > 0) {
        modifiedPage.releaseGuard();
        assert modifiedPage.getGuardCount() == 0 : 
            "Page had guardCount=" + guardCount + ", after release should be 0 but is " + modifiedPage.getGuardCount();
      }
    }
    
    // Diagnostic logging for leak detection
    if (DEBUG_MEMORY_LEAKS) {
      if (value.getComplete() instanceof KeyValueLeafPage completePage) {
        LOGGER.debug("[TIL-PUT] complete page: pageKey={}, indexType={}, rev={}, instance={}, guardCount={}", 
            completePage.getPageKey(), completePage.getIndexType(), completePage.getRevision(),
            System.identityHashCode(completePage), completePage.getGuardCount());
      }
      if (value.getModified() instanceof KeyValueLeafPage modifiedPage && modifiedPage != value.getComplete()) {
        LOGGER.debug("[TIL-PUT] modified page: pageKey={}, indexType={}, rev={}, instance={}, guardCount={}", 
            modifiedPage.getPageKey(), modifiedPage.getIndexType(), modifiedPage.getRevision(),
            System.identityHashCode(modifiedPage), modifiedPage.getGuardCount());
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
    
    if (DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("[TIL-CLEAR] Starting with {} containers", list.size());
    }
    
    logKey = 0;
    
    // CRITICAL: Force completion of ALL pending async removal listeners
    // Pages were removed from caches in put() - we must wait for async listeners
    // to finish before we close pages in TIL, otherwise double-close
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();
    
    // Close all pages in TIL
    // CRITICAL: Must release guards before closing to allow pages to close
    int totalContainers = list.size();
    int kvLeafContainers = 0;
    int page0Count = 0;
    
    for (final PageContainer pageContainer : list) {
      Page complete = pageContainer.getComplete();
      Page modified = pageContainer.getModified();
      
      if (complete instanceof KeyValueLeafPage completePage) {
        kvLeafContainers++;
        if (completePage.getPageKey() == 0) {
          page0Count++;
        }
        
        // CRITICAL FIX: Release all guards before closing
        // Pages in TIL may have guards from when they were in cache
        while (completePage.getGuardCount() > 0) {
          completePage.releaseGuard();
        }
        
        // TIL owns these pages exclusively - must close them
        completePage.close();
        if (!completePage.isClosed()) {
          LOGGER.error("TIL.clear(): FAILED to close complete page {} ({}) rev={} guardCount={}",
              completePage.getPageKey(), completePage.getIndexType(), completePage.getRevision(), completePage.getGuardCount());
          skippedAlreadyClosed++;
        } else {
          closedComplete++;
        }
      }
      
      // Check if modified is a different instance before closing
      if (modified instanceof KeyValueLeafPage modifiedPage && modified != complete) {
        if (modifiedPage.getPageKey() == 0) {
          page0Count++;
        }
        
        // CRITICAL FIX: Release all guards before closing
        while (modifiedPage.getGuardCount() > 0) {
          modifiedPage.releaseGuard();
        }
        
        // TIL owns these pages exclusively - must close them
        modifiedPage.close();
        if (!modifiedPage.isClosed()) {
          LOGGER.error("TIL.clear(): FAILED to close modified page {} ({}) rev={} guardCount={}",
              modifiedPage.getPageKey(), modifiedPage.getIndexType(), modifiedPage.getRevision(), modifiedPage.getGuardCount());
          skippedAlreadyClosed++;
        } else {
          closedModified++;
        }
      }
    }
    list.clear();
    
    // Log TIL cleanup for diagnostics
    if (DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("[TIL-CLEAR] Processed {} containers ({} KeyValueLeafPages, {} Page0s), closed {} complete + {} modified, failed {}",
          totalContainers, kvLeafContainers, page0Count, closedComplete, closedModified, skippedAlreadyClosed);
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
    int initialSize = list.size();
    if (DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("[TIL-CLOSE] Starting with {} containers", initialSize);
    }
    
    // CRITICAL: Force completion of ALL pending async removal listeners
    // Pages were removed from caches in put() - we must wait for async listeners
    // to finish before we close pages in TIL, otherwise double-close
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();
    
    // Close pages to release segments
    // CRITICAL: Must release guards before closing to allow pages to close
    int closedComplete = 0;
    int closedModified = 0;
    int page0Count = 0;
    int failedCloses = 0;
    
    for (final PageContainer pageContainer : list) {
      Page completePage = pageContainer.getComplete();
      Page modifiedPage = pageContainer.getModified();
      
      if (completePage instanceof KeyValueLeafPage kvCompletePage) {
        if (kvCompletePage.getPageKey() == 0) {
          page0Count++;
        }
        
        // CRITICAL FIX: Release all guards before closing
        // Pages in TIL may have guards from when they were in cache
        while (kvCompletePage.getGuardCount() > 0) {
          kvCompletePage.releaseGuard();
        }
        
        // TIL owns these pages exclusively - must close them regardless of pin count
        kvCompletePage.close();
        if (!kvCompletePage.isClosed()) {
          LOGGER.error("TIL.close(): FAILED to close complete page {} ({}) rev={} guardCount={}",
              kvCompletePage.getPageKey(), kvCompletePage.getIndexType(), kvCompletePage.getRevision(), kvCompletePage.getGuardCount());
          failedCloses++;
        } else {
          closedComplete++;
        }
      }
      
      // Check if modified is a different instance before closing
      if (modifiedPage instanceof KeyValueLeafPage kvModifiedPage && modifiedPage != completePage) {
        if (kvModifiedPage.getPageKey() == 0) {
          page0Count++;
        }
        
        // CRITICAL FIX: Release all guards before closing
        while (kvModifiedPage.getGuardCount() > 0) {
          kvModifiedPage.releaseGuard();
        }
        
        kvModifiedPage.close();
        if (!kvModifiedPage.isClosed()) {
          LOGGER.error("TIL.close(): FAILED to close modified page {} ({}) rev={} guardCount={}",
              kvModifiedPage.getPageKey(), kvModifiedPage.getIndexType(), kvModifiedPage.getRevision(), kvModifiedPage.getGuardCount());
          failedCloses++;
        } else {
          closedModified++;
        }
      }
    }
    
    // Log TIL cleanup for diagnostics
    if (DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("[TIL-CLOSE] Processed {} containers ({} Page0s), closed {} complete + {} modified, failed {}",
          initialSize, page0Count, closedComplete, closedModified, failedCloses);
    }
    
    logKey = 0;
    list.clear();
  }
  
  /**
   * Get the number of containers in the TIL.
   */
  public int size() {
    return list.size();
  }
  
  /**
   * Get the log key (for diagnostics).
   */
  public int getLogKey() {
    return logKey;
  }
  
  @Override
  @SuppressWarnings("deprecation")
  protected void finalize() {
    // DIAGNOSTIC: Detect if TIL is GC'd without being closed
    if (!list.isEmpty() && DEBUG_MEMORY_LEAKS) {
      LOGGER.warn("⚠️  TIL FINALIZED WITHOUT CLEAR: instance={} with {} containers still in list!", 
          System.identityHashCode(this), list.size());
    }
  }
}




