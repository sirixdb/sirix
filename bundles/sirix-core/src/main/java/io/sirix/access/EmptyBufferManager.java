package io.sirix.access;

import io.sirix.cache.*;
import io.sirix.index.name.Names;
import io.sirix.node.interfaces.Node;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;

public final class EmptyBufferManager implements BufferManager {

  private static final EmptyCache<PageReference, Page> RECORD_PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<PageReference, Page> PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<Integer, RevisionRootPage> REVISION_ROOT_PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<RBIndexKey, Node> INDEX_CACHE = new EmptyCache<>();

  private static final EmptyCache<NamesCacheKey, Names> NAMES_CACHE = new EmptyCache<>();

  private static final EmptyCache<Integer, PathSummaryData> PATH_SUMMARY_CACHE = new EmptyCache<>();

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
  public Cache<RBIndexKey, Node> getIndexCache() {
    return INDEX_CACHE;
  }

  @Override
  public Cache<NamesCacheKey, Names> getNamesCache() {
    return NAMES_CACHE;
  }

  @Override
  public Cache<Integer, PathSummaryData> getPathSummaryCache() {
    return PATH_SUMMARY_CACHE;
  }

  @Override
  public void close() {
  }

  @Override
  public void clearAllCaches() {
  }
}
