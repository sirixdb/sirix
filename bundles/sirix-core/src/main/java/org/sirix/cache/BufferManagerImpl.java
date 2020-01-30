package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {
  private final PageCache mPageCache;

  private final RecordPageCache mRecordPageCache;

  private final UnorderedKeyValuePageCache mUnorderedKeyValuePageCache;

  private final RevisionRootPageCache mRevisionRootPageCache;

  public BufferManagerImpl() {
    mPageCache = new PageCache();
    mRecordPageCache = new RecordPageCache();
    mUnorderedKeyValuePageCache = new UnorderedKeyValuePageCache();
    mRevisionRootPageCache = new RevisionRootPageCache();
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return mPageCache;
  }

  @Override
  public Cache<PageReference, Page> getRecordPageCache() {
    return mRecordPageCache;
  }

  @Override
  public Cache<IndexLogKey, Page> getUnorderedKeyValuePageCache() {
    return mUnorderedKeyValuePageCache;
  }

  @Override
  public Cache<Integer, RevisionRootPage> getRevisionRootPageCache() {
    return mRevisionRootPageCache;
  }
}
