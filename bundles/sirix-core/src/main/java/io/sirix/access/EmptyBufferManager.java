package io.sirix.access;

import io.sirix.cache.*;
import io.sirix.index.name.Names;
import io.sirix.node.interfaces.Node;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;

public final class EmptyBufferManager implements BufferManager {

  private static final EmptyCache<PageReference, KeyValueLeafPage> RECORD_PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<PageReference, KeyValueLeafPage> RECORD_PAGE_FRAGMENT_CACHE = new EmptyCache<>();

  private static final EmptyCache<PageReference, Page> PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<RevisionRootPageCacheKey, RevisionRootPage> REVISION_ROOT_PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<RBIndexKey, Node> INDEX_CACHE = new EmptyCache<>();

  private static final EmptyCache<NamesCacheKey, Names> NAMES_CACHE = new EmptyCache<>();

  private static final EmptyCache<PathSummaryCacheKey, PathSummaryData> PATH_SUMMARY_CACHE = new EmptyCache<>();

  EmptyBufferManager() {
  }

  @Override
  public Cache<PageReference, KeyValueLeafPage> getRecordPageCache() {
    return RECORD_PAGE_CACHE;
  }

  @Override
  public Cache<PageReference, KeyValueLeafPage> getRecordPageFragmentCache() {
    return RECORD_PAGE_FRAGMENT_CACHE;
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return PAGE_CACHE;
  }

  @Override
  public Cache<RevisionRootPageCacheKey, RevisionRootPage> getRevisionRootPageCache() {
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
  public Cache<PathSummaryCacheKey, PathSummaryData> getPathSummaryCache() {
    return PATH_SUMMARY_CACHE;
  }

  @Override
  public void close() {
  }

  @Override
  public void clearAllCaches() {
  }
  
  @Override
  public void clearCachesForDatabase(long databaseId) {
    // No-op for empty buffer manager
  }
  
  @Override
  public void clearCachesForResource(long databaseId, long resourceId) {
    // No-op for empty buffer manager
  }
}
