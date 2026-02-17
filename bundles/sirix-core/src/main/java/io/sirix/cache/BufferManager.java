package io.sirix.cache;

import io.sirix.index.name.Names;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.node.interfaces.Node;
import io.sirix.page.interfaces.Page;

public interface BufferManager extends AutoCloseable {
  Cache<PageReference, KeyValueLeafPage> getRecordPageCache();

  Cache<PageReference, KeyValueLeafPage> getRecordPageFragmentCache();

  Cache<PageReference, Page> getPageCache();

  Cache<RevisionRootPageCacheKey, RevisionRootPage> getRevisionRootPageCache();

  Cache<RBIndexKey, Node> getIndexCache();

  Cache<NamesCacheKey, Names> getNamesCache();

  Cache<PathSummaryCacheKey, PathSummaryData> getPathSummaryCache();

  void clearAllCaches();

  /**
   * Clear all cached pages for a specific database. CRITICAL: Must be called when a database is
   * closed to prevent cache pollution.
   * 
   * @param databaseId the database ID to clear pages for
   */
  void clearCachesForDatabase(long databaseId);

  /**
   * Clear all cached pages for a specific resource within a database. CRITICAL: Must be called when a
   * resource is closed to prevent cache pollution.
   * 
   * @param databaseId the database ID
   * @param resourceId the resource ID to clear pages for
   */
  void clearCachesForResource(long databaseId, long resourceId);
}
