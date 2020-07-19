package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {
  private final PageCache pageCache;

  private final RecordPageCache recordPageCache;

  private final RevisionRootPageCache revisionRootPageCache;

  public BufferManagerImpl(final int maxPageCacheSize, final int maxRecordPageCacheSize, final int maxRevisionRootPageCache) {
    pageCache = new PageCache(maxPageCacheSize);
    recordPageCache = new RecordPageCache(maxRecordPageCacheSize);
    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
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
  public void close() {
    pageCache.clear();
    recordPageCache.clear();
    revisionRootPageCache.clear();
  }
}
