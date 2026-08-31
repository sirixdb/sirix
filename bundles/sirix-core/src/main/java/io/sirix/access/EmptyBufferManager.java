package io.sirix.access;

import io.sirix.cache.BufferManager;
import io.sirix.cache.Cache;
import io.sirix.cache.EmptyCache;
import io.sirix.cache.HOTLookupCache;
import io.sirix.cache.GlobalDictionaryRecordCacheKey;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.cache.GlobalVerdictCacheKey;
import io.sirix.cache.NamesCacheKey;
import io.sirix.cache.PathSummaryCacheKey;
import io.sirix.cache.PathSummaryData;
import io.sirix.cache.RevisionRootPageCacheKey;
import io.sirix.index.name.Names;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;

public final class EmptyBufferManager implements BufferManager {

  private static final EmptyCache<PageReference, KeyValueLeafPage> RECORD_PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<PageReference, KeyValueLeafPage> RECORD_PAGE_FRAGMENT_CACHE = new EmptyCache<>();

  private static final EmptyCache<PageReference, Page> PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<PageReference, HOTLeafPage> HOT_LEAF_PAGE_CACHE = new EmptyCache<>();

  private static final EmptyCache<RevisionRootPageCacheKey, RevisionRootPage> REVISION_ROOT_PAGE_CACHE =
      new EmptyCache<>();

  private static final HOTLookupCache HOT_LOOKUP_CACHE = HOTLookupCache.disabled();

  private static final EmptyCache<NamesCacheKey, Names> NAMES_CACHE = new EmptyCache<>();

  private static final EmptyCache<GlobalVerdictCacheKey, long[]> GLOBAL_VERDICT_CACHE = new EmptyCache<>();

  private static final EmptyCache<GlobalDictionaryRecordCacheKey, DataRecord> GLOBAL_DICT_RECORD_CACHE =
      new EmptyCache<>();

  private static final EmptyCache<PathSummaryCacheKey, PathSummaryData> PATH_SUMMARY_CACHE = new EmptyCache<>();

  EmptyBufferManager() {}

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
  public Cache<PageReference, HOTLeafPage> getHOTLeafPageCache() {
    return HOT_LEAF_PAGE_CACHE;
  }

  @Override
  public Cache<PageReference, HOTLeafPage> getHOTLeafFragmentCache() {
    return HOT_LEAF_PAGE_CACHE;
  }

  @Override
  public Cache<RevisionRootPageCacheKey, RevisionRootPage> getRevisionRootPageCache() {
    return REVISION_ROOT_PAGE_CACHE;
  }

  @Override
  public HOTLookupCache getHOTLookupCache() {
    return HOT_LOOKUP_CACHE;
  }

  @Override
  public Cache<NamesCacheKey, Names> getNamesCache() {
    return NAMES_CACHE;
  }

  @Override
  public Cache<GlobalVerdictCacheKey, long[]> getGlobalVerdictCache() {
    return GLOBAL_VERDICT_CACHE;
  }

  @Override
  public Cache<GlobalDictionaryRecordCacheKey, DataRecord> getGlobalDictionaryRecordCache() {
    return GLOBAL_DICT_RECORD_CACHE;
  }

  @Override
  public Cache<PathSummaryCacheKey, PathSummaryData> getPathSummaryCache() {
    return PATH_SUMMARY_CACHE;
  }

  @Override
  public void close() {}

  @Override
  public void clearAllCaches() {}

  @Override
  public void clearCachesForDatabase(long databaseId) {
    // No-op for empty buffer manager
  }

  @Override
  public void clearCachesForResource(long databaseId, long resourceId) {
    // No-op for empty buffer manager
  }
}
