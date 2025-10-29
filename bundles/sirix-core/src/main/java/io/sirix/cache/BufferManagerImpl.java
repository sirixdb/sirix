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
      int maxRevisionRootPageCache, int maxRBTreeNodeCache, int maxNamesCacheSize, int maxPathSummaryCacheSize) {
    pageCache = new PageCache(maxPageCachWeight);
    recordPageCache = new RecordPageCache(maxRecordPageCacheWeight);
    recordPageFragmentCache = new RecordPageFragmentCache(maxRecordPageCacheWeight / 2); // Half the size of recordPageCache
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
    int unpinnedPages = 0;
    int closedPages = 0;
    
    // First pass: force-unpin all pinned pages
    for (var entry : recordPageCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (page.getPinCount() > 0) {
        forceUnpinAll(page);
        unpinnedPages++;
      }
    }
    
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (page.getPinCount() > 0) {
        forceUnpinAll(page);
        unpinnedPages++;
      }
    }
    
    for (var entry : pageCache.asMap().entrySet()) {
      if (entry.getValue() instanceof KeyValueLeafPage kvPage && kvPage.getPinCount() > 0) {
        forceUnpinAll(kvPage);
        unpinnedPages++;
      }
    }
    
    if (unpinnedPages > 0) {
      System.err.println("✓ clearAllCaches() force-unpinned " + unpinnedPages + " leaked pages");
    }
    
    // Second pass: explicitly close all pages to release segments
    // Cache removal listeners will also close, but let's be explicit
    for (var entry : recordPageCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (!page.isClosed()) {
        page.close();
        closedPages++;
      }
    }
    
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (!page.isClosed()) {
        page.close();
        closedPages++;
      }
    }
    
    for (var entry : pageCache.asMap().entrySet()) {
      if (entry.getValue() instanceof KeyValueLeafPage kvPage && !kvPage.isClosed()) {
        kvPage.close();
        closedPages++;
      }
    }
    
    if (closedPages > 0) {
      System.err.println("✓ clearAllCaches() closed " + closedPages + " pages to release segments");
    }
    
    // Report page leak statistics (if DEBUG_MEMORY_LEAKS enabled)
    long finalized = KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();
    long created = KeyValueLeafPage.PAGES_CREATED.get();
    long closed = KeyValueLeafPage.PAGES_CLOSED.get();
    if (finalized > 0 || (created > 0 && created != closed)) {
      System.err.println("📊 Page Leak Stats: Created=" + created + ", Closed=" + closed + 
                         ", Finalized=" + finalized + ", Live=" + KeyValueLeafPage.ALL_LIVE_PAGES.size());
    }
    
    // Now clear the caches (pages already closed)
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
  }
}
