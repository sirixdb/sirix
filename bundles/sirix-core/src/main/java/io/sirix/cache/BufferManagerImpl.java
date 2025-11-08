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

  private final PageCache pageCache;

  private final RecordPageCache recordPageCache;

  private final RecordPageFragmentCache recordPageFragmentCache;

  private final RevisionRootPageCache revisionRootPageCache;

  private final RedBlackTreeNodeCache redBlackTreeNodeCache;

  private final NamesCache namesCache;

  private final PathSummaryCache pathSummaryCache;

  public BufferManagerImpl(int maxPageCachWeight, int maxRecordPageCacheWeight,
      int maxRecordPageFragmentCacheWeight, int maxRevisionRootPageCache, int maxRBTreeNodeCache, 
      int maxNamesCacheSize, int maxPathSummaryCacheSize) {
    pageCache = new PageCache(maxPageCachWeight);
    recordPageCache = new RecordPageCache(maxRecordPageCacheWeight);
    recordPageFragmentCache = new RecordPageFragmentCache(maxRecordPageFragmentCacheWeight);
    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
    redBlackTreeNodeCache = new RedBlackTreeNodeCache(maxRBTreeNodeCache);
    namesCache = new NamesCache(maxNamesCacheSize);
    pathSummaryCache = new PathSummaryCache(maxPathSummaryCacheSize);
  }

  @Override
  public Cache<PageReference, Page> getPageCache() {
    return pageCache;
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
    // Force-unpin all pages before clearing (pin count leak fix)
    // At this point all transactions should be closed, so any remaining pins are leaks
    
    // DIAGNOSTIC: Count pages in caches
    int recordCacheSize = recordPageCache.asMap().size();
    int fragmentCacheSize = recordPageFragmentCache.asMap().size();
    int pageCacheSize = pageCache.asMap().size();
    
    // First pass: force-unpin all pinned pages
    for (var entry : recordPageCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (page.getPinCount() > 0) {
        forceUnpinAll(page);
      }
    }
    
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      var page = entry.getValue();
      if (page.getPinCount() > 0) {
        forceUnpinAll(page);
      }
    }
    
    for (var entry : pageCache.asMap().entrySet()) {
      if (entry.getValue() instanceof KeyValueLeafPage kvPage) {
        if (kvPage.getPinCount() > 0) {
          forceUnpinAll(kvPage);
        }
      }
    }
    
    if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("clearAllCaches(): RecordCache={}, FragmentCache={}, PageCache={}", 
          recordCacheSize, fragmentCacheSize, pageCacheSize);
    }
    
    // DON'T explicitly close pages - let the cache removal listener do it
    // Explicitly closing leaves closed pages in cache which causes "assert !isClosed()" failures
    // Just clear the caches - removal listener will close pages
    pageCache.clear();
    recordPageCache.clear();
    recordPageFragmentCache.clear();
    revisionRootPageCache.clear();
    redBlackTreeNodeCache.clear();
    namesCache.clear();
    pathSummaryCache.clear();
  }
  
  /**
   * Force-unpin all transactions from a page.
   * Used when clearing caches - at this point all transactions should be closed,
   * so any remaining pins are leaks that need to be cleaned up.
   */
  private void forceUnpinAll(KeyValueLeafPage page) {
    // CRITICAL FIX: Skip if page is already closed
    if (page.isClosed()) {
      return;  // Page is closed, no need to unpin
    }
    
    var pinsByTrx = new java.util.HashMap<>(page.getPinCountByTransaction());
    for (var entry : pinsByTrx.entrySet()) {
      int trxId = entry.getKey();
      int pinCount = entry.getValue();
      for (int i = 0; i < pinCount; i++) {
        page.decrementPinCount(trxId);
      }
    }
    
    // CRITICAL: Verify the page is actually unpinned
    if (page.getPinCount() > 0) {
      throw new IllegalStateException("Page " + page.getPageKey() + " still has pinCount=" + 
          page.getPinCount() + " after force-unpin! Pins by trx: " + page.getPinCountByTransaction());
    }
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
      // Use computeIfPresent for atomic force-unpin and removal
      recordPageCache.asMap().computeIfPresent(key, (k, page) -> {
        if (page.getPinCount() > 0) {
          forceUnpinAll(page);
        }
        return null;  // Returning null removes the entry atomically
      });
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
      recordPageFragmentCache.asMap().computeIfPresent(key, (k, page) -> {
        if (page.getPinCount() > 0) {
          forceUnpinAll(page);
        }
        return null;  // Atomic removal
      });
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
      // Use computeIfPresent for atomic force-unpin and removal
      recordPageCache.asMap().computeIfPresent(key, (k, page) -> {
        if (page.getPinCount() > 0) {
          forceUnpinAll(page);
        }
        return null;  // Returning null removes the entry atomically
      });
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
      recordPageFragmentCache.asMap().computeIfPresent(key, (k, page) -> {
        if (page.getPinCount() > 0) {
          forceUnpinAll(page);
        }
        return null;  // Atomic removal
      });
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
