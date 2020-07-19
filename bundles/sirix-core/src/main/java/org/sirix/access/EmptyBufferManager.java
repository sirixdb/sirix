package org.sirix.access;

import org.sirix.cache.Cache;
import org.sirix.cache.EmptyCache;
import org.sirix.cache.IndexLogKey;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public final class EmptyBufferManager implements org.sirix.cache.BufferManager {

  private static final EmptyCache<PageReference, Page> RECORD_PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<PageReference, Page> PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<Integer, RevisionRootPage> REVISION_ROOT_PAGE_CACHE = new EmptyCache<>();

  EmptyBufferManager() {
  }

  @Override
  public Cache<PageReference, Page> getRecordPageCache() {
    return RECORD_PAGE_CACHE;
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return PAGE_CACHE;
  }

  @Override
  public Cache<Integer, RevisionRootPage> getRevisionRootPageCache() {
    return REVISION_ROOT_PAGE_CACHE;
  }

  @Override
  public void close() {
  }
}
