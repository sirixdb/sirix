package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {
  private final PageCache mPageCache;

  private final RecordPageCache mRecordPageCache;

  private final RevisionRootPageCache mRevisionRootPageCache;

  public BufferManagerImpl() {
    mPageCache = new PageCache();
    mRecordPageCache = new RecordPageCache();
    mRevisionRootPageCache = new RevisionRootPageCache();
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return mPageCache;
  }

  @Override
  public Cache<PageReference, Page> getRecordPageContainerCache() {
    return mRecordPageCache;
  }

  @Override
  public Cache<Integer, RevisionRootPage> getRevisionRootPageCache() {
    return mRevisionRootPageCache;
  }
}
