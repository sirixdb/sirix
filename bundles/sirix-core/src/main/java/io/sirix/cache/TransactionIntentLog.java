package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * Transaction intent log (TIL) for caching all changes made by a read/write transaction.
 * <p>
 * The TIL stores modified pages during a transaction. When the transaction commits, pages are
 * written to storage. On rollback, the TIL is simply cleared.
 * <p>
 * Pages added to the TIL are removed from global caches since they represent uncommitted changes
 * that should not be visible to other transactions.
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
   * @param bufferManager the buffer manager for cache operations
   * @param maxInMemoryCapacity the maximum expected number of modified pages
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
   *         cache
   */
  public PageContainer get(final PageReference key) {
    var logKey = key.getLogKey();
    if ((logKey >= this.logKey) || logKey < 0) {
      return null;
    }
    return list.get(logKey);
  }

  /**
   * Adds an entry to the transaction intent log.
   * <p>
   * The page is removed from global caches as the TIL now owns it exclusively. Guards are released
   * since TIL pages are transaction-private.
   * <p>
   * CRITICAL: Pages removed from caches during this operation are closed if they differ from the
   * pages in the new PageContainer. This prevents memory leaks when a cached combined-page is
   * replaced by newly created pages from combineRecordPagesForModification().
   *
   * @param key the page reference key
   * @param value the page container with complete and modified versions
   */
  public void put(final PageReference key, final PageContainer value) {
    // Clear cached hash before modifying key properties
    key.clearCachedHash();

    // CRITICAL FIX: Close old cached pages that differ from new pages being added to TIL
    // This fixes memory leak where:
    // 1. Read path creates combined page X and caches it
    // 2. Write path creates NEW pages (complete, modified) via combineRecordPagesForModification()
    // 3. TIL.put() removes page X from cache but TIL owns the new pages, not X
    // 4. Page X becomes orphaned (not in cache, not in TIL)
    KeyValueLeafPage oldCachedPage = bufferManager.getRecordPageCache().get(key);

    // Remove from caches - TIL takes exclusive ownership of NEW pages
    bufferManager.getRecordPageCache().remove(key);
    bufferManager.getPageCache().remove(key);
    // Note: RecordPageFragmentCache entries are shared and managed by ClockSweeper

    // Close the old cached page if it's different from the pages going into TIL
    // This prevents orphaned pages from leaking memory
    if (oldCachedPage != null && !oldCachedPage.isClosed()) {
      boolean isInNewContainer = (oldCachedPage == value.getComplete() || oldCachedPage == value.getModified());
      if (!isInNewContainer) {
        // Old page is NOT one of the new pages - mark as orphaned first
        // This prevents new guards from being acquired (tryAcquireGuard checks isOrphaned)
        oldCachedPage.markOrphaned();
        // close() internally checks guardCount > 0 and isClosed
        // If guards are active, close() returns without closing
        // The last releaseGuard() will close the page (since isOrphaned is true)
        oldCachedPage.close();
      }
    }

    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(logKey);

    list.add(value);
    logKey++;

    // Release guards - TIL pages are transaction-private
    if (value.getComplete() instanceof KeyValueLeafPage completePage && completePage.getGuardCount() > 0) {
      completePage.releaseGuard();
    }
    if (value.getModified() instanceof KeyValueLeafPage modifiedPage && modifiedPage != value.getComplete()
        && modifiedPage.getGuardCount() > 0) {
      modifiedPage.releaseGuard();
    }
  }


  /**
   * Clears the transaction intent log, closing all owned pages.
   * <p>
   * This is typically called on transaction rollback. All pages in the TIL are closed and their
   * memory is released.
   */
  public void clear() {
    logKey = 0;

    // Ensure pending cache operations are complete before closing pages
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();

    // Close all pages owned by TIL
    for (final PageContainer pageContainer : list) {
      closePage(pageContainer.getComplete());
      if (pageContainer.getModified() != pageContainer.getComplete()) {
        closePage(pageContainer.getModified());
      }
    }
    list.clear();
  }

  /**
   * Helper method to release guards and close a page.
   */
  private void closePage(Page page) {
    if (page instanceof KeyValueLeafPage kvPage) {
      while (kvPage.getGuardCount() > 0) {
        kvPage.releaseGuard();
      }
      kvPage.close();
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

  /**
   * Closes the transaction intent log and releases all owned pages.
   */
  @Override
  public void close() {
    // Ensure pending cache operations are complete
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();

    // Close all pages owned by TIL
    for (final PageContainer pageContainer : list) {
      closePage(pageContainer.getComplete());
      if (pageContainer.getModified() != pageContainer.getComplete()) {
        closePage(pageContainer.getModified());
      }
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
   * Get the current log key.
   *
   * @return the current log key value
   */
  public int getLogKey() {
    return logKey;
  }
}


