package io.sirix.cache;

import io.sirix.node.interfaces.Node;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BufferManagerImpl implements BufferManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferManagerImpl.class);

  // Use ShardedPageCache instead of Caffeine for direct eviction control
  private final ShardedPageCache recordPageCache;
  private final ShardedPageCache recordPageFragmentCache;
  private final ShardedPageCache pageCache;

  private final RevisionRootPageCache revisionRootPageCache;
  private final RedBlackTreeNodeCache redBlackTreeNodeCache;
  private final NamesCache namesCache;
  private final PathSummaryCache pathSummaryCache;

  public BufferManagerImpl(int maxPageCachWeight, int maxRecordPageCacheWeight,
      int maxRecordPageFragmentCacheWeight, int maxRevisionRootPageCache, int maxRBTreeNodeCache, 
      int maxNamesCacheSize, int maxPathSummaryCacheSize) {
    // Use ShardedPageCache with 64 shards for multi-core scalability
    // TODO: Eviction based on memory limits will be handled by ClockSweeper
    int shardCount = 64;
    recordPageCache = new ShardedPageCache(shardCount);
    recordPageFragmentCache = new ShardedPageCache(shardCount);
    pageCache = new ShardedPageCache(shardCount);
    
    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
    redBlackTreeNodeCache = new RedBlackTreeNodeCache(maxRBTreeNodeCache);
    namesCache = new NamesCache(maxNamesCacheSize);
    pathSummaryCache = new PathSummaryCache(maxPathSummaryCacheSize);
    
    LOGGER.info("BufferManagerImpl initialized with ShardedPageCache (shards={})", shardCount);
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    // Cast is safe - ShardedPageCache stores KeyValueLeafPages which are Pages
    @SuppressWarnings("unchecked")
    Cache<PageReference, Page> cache = (Cache<PageReference, Page>) (Cache<?, ?>) pageCache;
    return cache;
  }

  @Override
  public Cache<PageReference, KeyValueLeafPage> getRecordPageCache() {
    return recordPageCache;
  }

  @Override
  public Cache<PageReference, KeyValueLeafPage> getRecordPageFragmentCache() {
    return recordPageFragmentCache;
  }

  @Override
  public Cache<RevisionRootPageCacheKey, RevisionRootPage> getRevisionRootPageCache() {
    return revisionRootPageCache;
  }

  @Override
  public Cache<RBIndexKey, Node> getIndexCache() {
    return redBlackTreeNodeCache;
  }

  @Override
  public NamesCache getNamesCache() {
    return namesCache;
  }

  @Override
  public Cache<PathSummaryCacheKey, PathSummaryData> getPathSummaryCache() {
    return pathSummaryCache;
  }

  @Override
  public void close() {
  }

  @Override
  public void clearAllCaches() {
    // TODO: Update after implementing guard-based system
    // Clear all caches - removal listeners will handle page cleanup
    
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("clearAllCaches(): RecordCache={}, FragmentCache={}, PageCache={}", 
          recordPageCache.asMap().size(), recordPageFragmentCache.asMap().size(), 
          pageCache.asMap().size());
    }
    
    // Just clear the caches - removal listener will close pages
    pageCache.clear();
    recordPageCache.clear();
    recordPageFragmentCache.clear();
    revisionRootPageCache.clear();
    redBlackTreeNodeCache.clear();
    namesCache.clear();
    pathSummaryCache.clear();
  }
  
  @Override
  public void clearCachesForDatabase(long databaseId) {
    // CRITICAL FIX: Remove all pages belonging to this database from global caches
    // This prevents cache pollution when database is removed and recreated with same ID
    // THREAD-SAFE: Collect keys first, then remove atomically to avoid concurrent modification
    
    int removedFromRecordCache = 0;
    int removedFromFragmentCache = 0;
    int removedFromPageCache = 0;
    int removedFromRevisionCache = 0;
    
    // Clear RecordPageCache - collect keys then remove atomically
    var recordKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        recordKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : recordKeysToRemove) {
      recordPageCache.remove(key);
      removedFromRecordCache++;
    }
    
    // Clear RecordPageFragmentCache
    var fragmentKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        fragmentKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : fragmentKeysToRemove) {
      recordPageFragmentCache.remove(key);
      removedFromFragmentCache++;
    }
    
    // Clear PageCache
    var pageKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : pageCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        pageKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : pageKeysToRemove) {
      pageCache.remove(key);  // Cache.remove() is thread-safe
      removedFromPageCache++;
    }
    
    // Clear RevisionRootPageCache
    var revisionKeysToRemove = new java.util.ArrayList<RevisionRootPageCacheKey>();
    for (var entry : revisionRootPageCache.asMap().entrySet()) {
      if (entry.getKey().databaseId() == databaseId) {  // Record field access
        revisionKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : revisionKeysToRemove) {
      revisionRootPageCache.remove(key);  // Thread-safe
      removedFromRevisionCache++;
    }
    
    if (removedFromRecordCache + removedFromFragmentCache + removedFromPageCache + removedFromRevisionCache > 0) {
      LOGGER.debug("Cleared caches for database {}: RecordCache={}, FragmentCache={}, PageCache={}, RevisionCache={}",
          databaseId, removedFromRecordCache, removedFromFragmentCache, removedFromPageCache, removedFromRevisionCache);
    }
  }
  
  @Override
  public void clearCachesForResource(long databaseId, long resourceId) {
    // CRITICAL FIX: Remove all pages belonging to this resource from global caches
    // This prevents cache pollution when resource is closed and recreated with same IDs
    // THREAD-SAFE: Collect keys first, then remove atomically to avoid concurrent modification
    
    int removedFromRecordCache = 0;
    int removedFromFragmentCache = 0;
    int removedFromPageCache = 0;
    int removedFromRevisionCache = 0;
    
    // Clear RecordPageCache - collect keys then remove atomically
    var recordKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        recordKeysToRemove.add(key);
      }
    }
    for (var key : recordKeysToRemove) {
      recordPageCache.remove(key);
      removedFromRecordCache++;
    }
    
    // Clear RecordPageFragmentCache
    var fragmentKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        fragmentKeysToRemove.add(key);
      }
    }
    for (var key : fragmentKeysToRemove) {
      recordPageFragmentCache.remove(key);
      removedFromFragmentCache++;
    }
    
    // Clear PageCache
    var pageKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : pageCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        pageKeysToRemove.add(key);
      }
    }
    for (var key : pageKeysToRemove) {
      pageCache.remove(key);  // Cache.remove() is thread-safe
      removedFromPageCache++;
    }
    
    // Clear RevisionRootPageCache
    var revisionKeysToRemove = new java.util.ArrayList<RevisionRootPageCacheKey>();
    for (var entry : revisionRootPageCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.databaseId() == databaseId && key.resourceId() == resourceId) {
        revisionKeysToRemove.add(key);
      }
    }
    for (var key : revisionKeysToRemove) {
      revisionRootPageCache.remove(key);  // Thread-safe
      removedFromRevisionCache++;
    }
    
    if (removedFromRecordCache + removedFromFragmentCache + removedFromPageCache + removedFromRevisionCache > 0) {
      LOGGER.debug("Cleared caches for resource (db={}, res={}): RecordCache={}, FragmentCache={}, PageCache={}, RevisionCache={}",
          databaseId, resourceId, removedFromRecordCache, removedFromFragmentCache, removedFromPageCache, removedFromRevisionCache);
    }
  }
}
