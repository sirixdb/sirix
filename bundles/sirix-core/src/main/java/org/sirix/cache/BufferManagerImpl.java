package org.sirix.cache;

import org.sirix.index.avltree.AVLNode;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {
  private final PageCache pageCache;

  private final RecordPageCache recordPageCache;

  private final RevisionRootPageCache revisionRootPageCache;

  private final AVLNodeCache avlNodeCache;

  public BufferManagerImpl(final int maxPageCacheSize, final int maxRecordPageCacheSize, final int maxRevisionRootPageCache, final int maxAVLNodeCache) {
    pageCache = new PageCache(maxPageCacheSize);
    recordPageCache = new RecordPageCache(maxRecordPageCacheSize);
    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
    avlNodeCache = new AVLNodeCache(maxAVLNodeCache);
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return pageCache;
  }

  @Override
  public Cache<PageReference, Page> getRecordPageCache() {
    return recordPageCache;
  }

  @Override
  public Cache<Integer, RevisionRootPage> getRevisionRootPageCache() {
    return revisionRootPageCache;
  }

  @Override
  public Cache<AVLIndexKey, AVLNode<?, ?>> getIndexCache() {
    return avlNodeCache;
  }

  @Override
  public void close() {
    pageCache.clear();
    recordPageCache.clear();
    revisionRootPageCache.clear();
    avlNodeCache.clear();
  }
}
