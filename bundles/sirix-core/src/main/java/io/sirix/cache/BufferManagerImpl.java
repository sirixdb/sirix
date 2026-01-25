package io.sirix.cache;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.node.interfaces.Node;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global buffer manager for SirixDB page caching.
 * <p>
 * This component manages all page caches for a database, providing:
 * <ul>
 *   <li><b>Record page cache:</b> Full KeyValueLeafPages for data access</li>
 *   <li><b>Fragment cache:</b> Page fragments for versioning reconstruction</li>
 *   <li><b>Page cache:</b> Other page types (NamePage, RevisionRootPage, etc.)</li>
 *   <li><b>Specialized caches:</b> RevisionRootPages, RBTree nodes, Names, PathSummary</li>
 * </ul>
 * <p>
 * The buffer manager coordinates with background ClockSweeper threads for eviction,
 * following the PostgreSQL bgwriter pattern. Eviction uses a second-chance clock
 * algorithm with revision watermark protection for MVCC safety.
 * <p>
 * <b>Cache Architecture:</b>
 * <ul>
 *   <li>ShardedPageCache for KeyValueLeafPages (direct eviction control)</li>
 *   <li>Caffeine-based caches for other page types</li>
 *   <li>Global ClockSweeper threads for background eviction</li>
 * </ul>
 * <p>
 * <b>Thread Safety:</b> All caches are thread-safe and support concurrent access
 * from multiple transactions and ClockSweeper threads.
 *
 * @author Johannes Lichtenberger
 * @see ShardedPageCache
 * @see ClockSweeper
 */
public final class BufferManagerImpl implements BufferManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferManagerImpl.class);

  // Use ShardedPageCache for KeyValueLeafPage caches (direct eviction control)
  private final ShardedPageCache recordPageCache;
  private final ShardedPageCache recordPageFragmentCache;
  
  // Keep Caffeine PageCache for mixed page types (NamePage, RevisionRootPage, etc.)
  private final PageCache pageCache;

  private final RevisionRootPageCache revisionRootPageCache;
  private final RedBlackTreeNodeCache redBlackTreeNodeCache;
  private final NamesCache namesCache;
  private final PathSummaryCache pathSummaryCache;
  
  // GLOBAL ClockSweeper threads (PostgreSQL bgwriter pattern)
  // Started when BufferManager is initialized, run until shutdown
  private final java.util.List<Thread> clockSweeperThreads;
  private final java.util.List<ClockSweeper> clockSweepers;
  private volatile boolean isShutdown = false;

  /**
   * Create a BufferManagerImpl with specified cache sizes.
   * <p>
   * All cache sizes are in bytes. For large caches (> 2GB), use long values.
   *
   * @param maxPageCacheWeight maximum weight in bytes for the metadata page cache
   * @param maxRecordPageCacheWeight maximum weight in bytes for the record page cache
   * @param maxRecordPageFragmentCacheWeight maximum weight in bytes for the record page fragment cache
   * @param maxRevisionRootPageCache maximum number of revision root pages to cache
   * @param maxRBTreeNodeCache maximum number of RB-tree nodes to cache
   * @param maxNamesCacheSize maximum number of name entries to cache
   * @param maxPathSummaryCacheSize maximum number of path summary entries to cache
   */
  public BufferManagerImpl(long maxPageCacheWeight, long maxRecordPageCacheWeight,
      long maxRecordPageFragmentCacheWeight, int maxRevisionRootPageCache, int maxRBTreeNodeCache,
      int maxNamesCacheSize, int maxPathSummaryCacheSize) {
    // Use simplified ShardedPageCache (single HashMap) for KeyValueLeafPage caches
    // ShardedPageCache uses long for maxWeightBytes - supports > 2GB caches
    recordPageCache = new ShardedPageCache(maxRecordPageCacheWeight);
    recordPageFragmentCache = new ShardedPageCache(maxRecordPageFragmentCacheWeight);

    // PageCache uses Caffeine which internally uses long for weights
    pageCache = new PageCache(maxPageCacheWeight);

    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
    redBlackTreeNodeCache = new RedBlackTreeNodeCache(maxRBTreeNodeCache);
    namesCache = new NamesCache(maxNamesCacheSize);
    pathSummaryCache = new PathSummaryCache(maxPathSummaryCacheSize);

    // Initialize ClockSweeper threads (GLOBAL, like PostgreSQL bgwriter)
    this.clockSweeperThreads = new java.util.ArrayList<>();
    this.clockSweepers = new java.util.ArrayList<>();

    LOGGER.info("BufferManagerImpl initialized with large cache support:");
    LOGGER.info("  - RecordPageCache: {} MB", maxRecordPageCacheWeight / (1024 * 1024));
    LOGGER.info("  - RecordPageFragmentCache: {} MB", maxRecordPageFragmentCacheWeight / (1024 * 1024));
    LOGGER.info("  - PageCache: {} MB", maxPageCacheWeight / (1024 * 1024));
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

  /**
   * Start global ClockSweeper threads for this BufferManager.
   * Called once when first database opens. ClockSweepers run until BufferManager shutdown.
   * This follows PostgreSQL bgwriter pattern - background threads run independently of sessions.
   *
   * @param globalEpochTracker the global epoch tracker for MVCC-aware eviction
   */
  public synchronized void startClockSweepers(RevisionEpochTracker globalEpochTracker) {
    if (!clockSweepers.isEmpty()) {
      // Already started
      return;
    }
    
    int sweepIntervalMs = 100; // Sweep every 100ms
    
    // Start ClockSweeper for RecordPageCache (GLOBAL - handles all databases/resources)
    if (recordPageCache instanceof ShardedPageCache recordCache) {
      // Get shard 0 (we only have 1 shard in simplified design)
      ShardedPageCache.Shard shard = recordCache.getShard(new PageReference());
      
      ClockSweeper sweeper = new ClockSweeper(
          shard, recordCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);  // databaseId=0, resourceId=0 means "all"
      
      Thread thread = new Thread(sweeper, "ClockSweeper-RecordPage-GLOBAL");
      thread.setDaemon(true);
      thread.start();
      
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      
      LOGGER.info("Started GLOBAL ClockSweeper thread for RecordPageCache");
    }
    
    // Start ClockSweeper for RecordPageFragmentCache (GLOBAL)
    if (recordPageFragmentCache instanceof ShardedPageCache fragmentCache) {
      ShardedPageCache.Shard shard = fragmentCache.getShard(new PageReference());
      
      ClockSweeper sweeper = new ClockSweeper(
          shard, fragmentCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);  // databaseId=0, resourceId=0 means "all"
      
      Thread thread = new Thread(sweeper, "ClockSweeper-FragmentPage-GLOBAL");
      thread.setDaemon(true);
      thread.start();
      
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      
      LOGGER.info("Started GLOBAL ClockSweeper thread for RecordPageFragmentCache");
    }
  }
  
  /**
   * Stop all global ClockSweeper threads.
   * Called when BufferManager is shut down (last database closes) or when clearing all caches.
   */
  public synchronized void stopClockSweepers() {
    for (ClockSweeper sweeper : clockSweepers) {
      sweeper.stop();
    }
    
    for (Thread thread : clockSweeperThreads) {
      thread.interrupt();
    }
    
    // Wait for threads to finish (with timeout)
    for (Thread thread : clockSweeperThreads) {
      try {
        thread.join(1000); // Wait max 1 second
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    
    LOGGER.info("Stopped {} GLOBAL ClockSweeper threads", clockSweeperThreads.size());
    
    clockSweepers.clear();
    clockSweeperThreads.clear();
  }

  @Override
  public void close() {
    if (!isShutdown) {
      stopClockSweepers();
      isShutdown = true;
    }
  }

  /**
   * Clears all caches, closing all cached pages.
   * <p>
   * This is typically called during database shutdown. ClockSweeper threads
   * continue running (they handle future evictions as new pages are loaded).
   */
  @Override
  public void clearAllCaches() {
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
    
    // Clear RecordPageCache - close pages BEFORE removing from cache
    var recordKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        recordKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : recordKeysToRemove) {
      KeyValueLeafPage page = recordPageCache.get(key);
      if (page != null && !page.isClosed()) {
        // CRITICAL: Force-release all guards to ensure page can be closed and memory returned
        while (page.getGuardCount() > 0) {
          page.releaseGuard();
        }
        page.close();  // Close page to release memory segments to allocator
      }
      recordPageCache.remove(key);
      removedFromRecordCache++;
    }
    
    // Clear RecordPageFragmentCache - close fragments BEFORE removing from cache
    var fragmentKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        fragmentKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : fragmentKeysToRemove) {
      KeyValueLeafPage page = recordPageFragmentCache.get(key);
      if (page != null && !page.isClosed()) {
        // CRITICAL: Force-release all guards to ensure fragment can be closed and memory returned
        while (page.getGuardCount() > 0) {
          page.releaseGuard();
        }
        page.close();  // Close fragment to release memory segments to allocator
      }
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
    
    // Clear RecordPageCache - close pages BEFORE removing from cache
    var recordKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        recordKeysToRemove.add(key);
      }
    }
    for (var key : recordKeysToRemove) {
      KeyValueLeafPage page = recordPageCache.get(key);
      if (page != null && !page.isClosed()) {
        // CRITICAL: Force-release all guards to ensure page can be closed and memory returned
        while (page.getGuardCount() > 0) {
          page.releaseGuard();
        }
        page.close();  // Close page to release memory segments to allocator
      }
      recordPageCache.remove(key);
      removedFromRecordCache++;
    }
    
    // Clear RecordPageFragmentCache - close fragments BEFORE removing from cache
    var fragmentKeysToRemove = new java.util.ArrayList<PageReference>();
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        fragmentKeysToRemove.add(key);
      }
    }
    for (var key : fragmentKeysToRemove) {
      KeyValueLeafPage page = recordPageFragmentCache.get(key);
      if (page != null && !page.isClosed()) {
        // CRITICAL: Force-release all guards to ensure fragment can be closed and memory returned
        while (page.getGuardCount() > 0) {
          page.releaseGuard();
        }
        page.close();  // Close fragment to release memory segments to allocator
      }
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
