package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public final class BufferManagerImpl implements BufferManager {
  private final PageCache pageCache;

  private final RecordPageCache recordPageCache;

  private final UnorderedKeyValuePageCache unorderedKeyValuePageCache;

  private final RevisionRootPageCache revisionRootPageCache;

  public BufferManagerImpl() {
    pageCache = new PageCache();
    recordPageCache = new RecordPageCache();
    unorderedKeyValuePageCache = new UnorderedKeyValuePageCache();
    revisionRootPageCache = new RevisionRootPageCache();
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
  public Cache<IndexLogKey, Page> getUnorderedKeyValuePageCache() {
    return unorderedKeyValuePageCache;
  }

  @Override
  public Cache<Integer, RevisionRootPage> getRevisionRootPageCache() {
    return revisionRootPageCache;
  }
}
