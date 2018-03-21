package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {
  private final PageCache mPageCache;

  private final RecordPageCache mRecordPageCache;

  public BufferManagerImpl() {
    mPageCache = new PageCache();
    mRecordPageCache = new RecordPageCache();
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return mPageCache;
  }

  @Override
  public Cache<PageReference, PageContainer> getRecordPageCache() {
    return mRecordPageCache;
  }
}
