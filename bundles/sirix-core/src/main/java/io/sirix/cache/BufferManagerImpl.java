package io.sirix.cache;

import io.sirix.node.interfaces.Node;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {

  private final PageCache pageCache;

  private final RecordPageCache recordPageCache;

  private final RevisionRootPageCache revisionRootPageCache;

  private final RedBlackTreeNodeCache redBlackTreeNodeCache;

  private final NamesCache namesCache;

  private final PathSummaryCache pathSummaryCache;

  public BufferManagerImpl(int maxPageCachWeight, int maxRecordPageCacheWeight,
      int maxRevisionRootPageCache, int maxRBTreeNodeCache, int maxNamesCacheSize, int maxPathSummaryCacheSize) {
    pageCache = new PageCache(maxPageCachWeight);
    recordPageCache = new RecordPageCache(maxRecordPageCacheWeight);
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
  public Cache<Integer, RevisionRootPage> getRevisionRootPageCache() {
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
  public PathSummaryCache getPathSummaryCache() {
    return pathSummaryCache;
  }

  @Override
  public void close() {
  }

  @Override
  public void clearAllCaches() {
    pageCache.clear();
    recordPageCache.clear();
    revisionRootPageCache.clear();
    redBlackTreeNodeCache.clear();
    namesCache.clear();
    pathSummaryCache.clear();
  }
}
