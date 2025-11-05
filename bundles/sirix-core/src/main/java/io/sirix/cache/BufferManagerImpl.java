package io.sirix.cache;

import io.sirix.node.interfaces.Node;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {

  private final PageCache pageCache;

  private final RecordPageCache recordPageCache;

  private final RecordPageFragmentCache recordPageFragmentCache;

  private final RevisionRootPageCache revisionRootPageCache;

  private final RedBlackTreeNodeCache redBlackTreeNodeCache;

  private final NamesCache namesCache;

  private final PathSummaryCache pathSummaryCache;

  public BufferManagerImpl(int maxPageCachWeight, int maxRecordPageCacheWeight,
      int maxRecordPageFragmentCacheWeight, int maxRevisionRootPageCache, int maxRBTreeNodeCache, 
      int maxNamesCacheSize, int maxPathSummaryCacheSize) {
    pageCache = new PageCache(maxPageCachWeight);
    recordPageCache = new RecordPageCache(maxRecordPageCacheWeight);
    recordPageFragmentCache = new RecordPageFragmentCache(maxRecordPageFragmentCacheWeight);
    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
    redBlackTreeNodeCache = new RedBlackTreeNodeCache(maxRBTreeNodeCache);
    namesCache = new NamesCache(maxNamesCacheSize);
    pathSummaryCache = new PathSummaryCache(maxPathSummaryCacheSize);
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return pageCache;
  }

  @Override
  public Cache<PageReference, KeyValueLeafPage> getRecordPageCache() {
    return recordPageCache;
  }

  @Override
  public Cache<PageReference, KeyValueLeafPage> getRecordPageFragmentCache() {
    return recordPageFragmentCache;
  }

  @Override
  public Cache<RevisionRootPageCacheKey, RevisionRootPage> getRevisionRootPageCache() {
    return revisionRootPageCache;
  }

  @Override
  public Cache<RBIndexKey, Node> getIndexCache() {
    return redBlackTreeNodeCache;
  }

  @Override
  public NamesCache getNamesCache() {
    return namesCache;
  }

  @Override
  public Cache<PathSummaryCacheKey, PathSummaryData> getPathSummaryCache() {
    return pathSummaryCache;
  }

  @Override
  public void close() {
  }

  @Override
  public void clearAllCaches() {
    // Force-unpin all pages before clearing (pin count leak fix)
    // At this point all transactions should be closed, so any remaining pins are leaks
    
    // DIAGNOSTIC: Count pages in caches
    int recordCacheSize = recordPageCache.asMap().size();
    int fragmentCacheSize = recordPageFragmentCache.asMap().size();
    int pageCacheSize = pageCache.asMap().size();
    int page0InCaches = 0;
    
    // First pass: force-unpin all pinned pages
    for (var entry : recordPageCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (page.getPageKey() == 0) page0InCaches++;
      if (page.getPinCount() > 0) {
        forceUnpinAll(page);
      }
    }
    
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (page.getPageKey() == 0) page0InCaches++;
      if (page.getPinCount() > 0) {
        forceUnpinAll(page);
      }
    }
    
    for (var entry : pageCache.asMap().entrySet()) {
      if (entry.getValue() instanceof KeyValueLeafPage kvPage) {
        if (kvPage.getPageKey() == 0) page0InCaches++;
        if (kvPage.getPinCount() > 0) {
          forceUnpinAll(kvPage);
        }
      }
    }
    
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      int totalPage0s = KeyValueLeafPage.ALL_PAGE_0_INSTANCES.size();
      System.err.println("🧹 clearAllCaches(): RecordCache=" + recordCacheSize + ", FragmentCache=" + fragmentCacheSize + 
                        ", PageCache=" + pageCacheSize + ", Page0InCaches=" + page0InCaches + 
                        ", TotalPage0s=" + totalPage0s);
    }
    
    // DON'T explicitly close pages - let the cache removal listener do it
    // Explicitly closing leaves closed pages in cache which causes "assert !isClosed()" failures
    // Just clear the caches - removal listener will close pages
    pageCache.clear();
    recordPageCache.clear();
    recordPageFragmentCache.clear();
    revisionRootPageCache.clear();
    redBlackTreeNodeCache.clear();
    namesCache.clear();
    pathSummaryCache.clear();
  }
  
  /**
   * Force-unpin all transactions from a page.
   * Used when clearing caches - at this point all transactions should be closed,
   * so any remaining pins are leaks that need to be cleaned up.
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
    
    // CRITICAL: Verify the page is actually unpinned
    if (page.getPinCount() > 0) {
      throw new IllegalStateException("Page " + page.getPageKey() + " still has pinCount=" + 
          page.getPinCount() + " after force-unpin! Pins by trx: " + page.getPinCountByTransaction());
    }
  }
}
