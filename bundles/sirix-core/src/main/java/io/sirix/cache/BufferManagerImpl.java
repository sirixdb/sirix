package io.sirix.cache;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.node.interfaces.Node;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

/**
 * Global buffer manager for SirixDB page caching.
 * <p>
 * This component manages all page caches for a database, providing:
 * <ul>
 * <li><b>Record page cache:</b> Full KeyValueLeafPages for data access</li>
 * <li><b>Fragment cache:</b> Page fragments for versioning reconstruction</li>
 * <li><b>Page cache:</b> Other page types (NamePage, RevisionRootPage, etc.)</li>
 * <li><b>Specialized caches:</b> RevisionRootPages, RBTree nodes, Names, PathSummary</li>
 * </ul>
 * <p>
 * The buffer manager coordinates with background ClockSweeper threads for eviction, following the
 * PostgreSQL bgwriter pattern. Eviction uses a second-chance clock algorithm with revision
 * watermark protection for MVCC safety.
 * <p>
 * <b>Cache Architecture:</b>
 * <ul>
 * <li>ShardedPageCache for KeyValueLeafPages (direct eviction control)</li>
 * <li>Caffeine-based caches for other page types</li>
 * <li>Global ClockSweeper threads for background eviction</li>
 * </ul>
 * <p>
 * <b>Thread Safety:</b> All caches are thread-safe and support concurrent access from multiple
 * transactions and ClockSweeper threads.
 *
 * @author Johannes Lichtenberger
 * @see ShardedPageCache
 * @see ClockSweeper
 */
public final class BufferManagerImpl implements BufferManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferManagerImpl.class);

  /**
   * Pages that were still guarded when an invalidation sweep reached them.
   *
   * <p>A destructive sweep is supposed to run when nothing of the swept scope is in use, so a
   * non-zero count means some caller invalidates while it is itself holding pages — the shape of bug
   * that made {@code createPageTransaction} truncate after constructing its writer. The sweep stays
   * correct either way (teardown defers to the holder's last release), so this is an anomaly signal
   * rather than an error, and it costs one already-loaded field read per swept page on a cold path.
   */
  private static final LongAdder GUARDED_PAGES_SWEPT = new LongAdder();

  /**
   * Smallest useful HOT fragment budget: 32 fragments, i.e. about 16 concurrently written leaves at
   * the default chain cap. Below this the cache thrashes instead of serving anything.
   */
  private static final long MIN_HOT_FRAGMENT_BUDGET_BYTES = 32L * HOTLeafPage.DEFAULT_SIZE;

  // Use ShardedPageCache for KeyValueLeafPage caches (direct eviction control)
  private final ShardedPageCache<KeyValueLeafPage> recordPageCache;
  private final ShardedPageCache<KeyValueLeafPage> recordPageFragmentCache;

  // Budget-aware cache for HOTLeafPages with clock-sweep eviction.
  private final ShardedPageCache<HOTLeafPage> hotLeafPageCache;

  // Individual HOT leaf fragments, keyed by their own durable offset (see the interface javadoc for
  // why this cannot share hotLeafPageCache).
  private final ShardedPageCache<HOTLeafPage> hotLeafFragmentCache;

  // Keep Caffeine PageCache for mixed page types (NamePage, RevisionRootPage, etc.)
  private final PageCache pageCache;

  private final RevisionRootPageCache revisionRootPageCache;
  private final RedBlackTreeNodeCache redBlackTreeNodeCache;
  private final NamesCache namesCache;
  private final PathSummaryCache pathSummaryCache;

  // GLOBAL ClockSweeper threads (PostgreSQL bgwriter pattern)
  // Started when BufferManager is initialized, run until shutdown
  private final List<Thread> clockSweeperThreads;
  private final List<ClockSweeper> clockSweepers;
  private volatile boolean isShutdown = false;

  /**
   * Create a BufferManagerImpl with specified cache sizes.
   * <p>
   * All cache sizes are in bytes. For large caches (> 2GB), use long values.
   *
   * @param maxPageCacheWeight maximum weight in bytes for the metadata page cache
   * @param maxRecordPageCacheWeight maximum weight in bytes for the record page cache
   * @param maxRecordPageFragmentCacheWeight maximum weight in bytes for the record page fragment
   *        cache
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
    recordPageCache = new ShardedPageCache<>(maxRecordPageCacheWeight);
    recordPageFragmentCache = new ShardedPageCache<>(maxRecordPageFragmentCacheWeight);

    // Budget-aware HOT caches, together capped at a quarter of the record-page budget.
    // That ceiling is SPLIT between combined leaves and raw fragments, not granted to each: giving
    // the new fragment cache its own quarter would silently double the HOT off-heap ceiling from
    // 25% to 50% of the record-page budget.
    //
    // The split is 3:1 in favour of the combined leaves, not even. Combined leaves back every HOT
    // read, and their working set is the whole live index; fragments back only the copy-on-write
    // carry-forward window, whose working set is bounded by the chain cap (revsToRestore - 1, so two
    // fragments per recently written leaf) and confined to leaves being written right now. An even
    // split would have halved read capacity to buy far more fragment capacity than the window can use.
    //
    // A quarter is floored at MIN_HOT_FRAGMENT_BUDGET_BYTES because HOTLeafPage weighs a flat
    // DEFAULT_SIZE regardless of fill: below roughly two fragments per concurrently written leaf the
    // cache cannot hold a single window, so every load misses AND takes the blocking eviction path,
    // which is strictly worse than the uncached read it replaced. The floor is itself capped at half
    // the HOT budget so a small configured budget cannot starve the combined-leaf read cache, and it
    // is skipped entirely when the record-page cache is disabled (budget 0), where ShardedPageCache
    // reads a non-positive maximum as "unbounded" and a floor would create an uncapped cache.
    final long hotLeafBudget = maxRecordPageCacheWeight / 4;
    final long hotFragmentBudget = hotLeafBudget <= 0
        ? hotLeafBudget
        : Math.min(hotLeafBudget / 2, Math.max(hotLeafBudget / 4, MIN_HOT_FRAGMENT_BUDGET_BYTES));
    hotLeafPageCache = new ShardedPageCache<>(hotLeafBudget - hotFragmentBudget);
    hotLeafFragmentCache = new ShardedPageCache<>(hotFragmentBudget);

    // PageCache uses Caffeine which internally uses long for weights
    pageCache = new PageCache(maxPageCacheWeight);

    // Register a pressure listener that evicts the page caches on allocator
    // exhaustion. Synchronous eviction avoids the 500 ms ClockSweeper cadence
    // under hot-scan pressure that otherwise surfaces as OutOfMemoryError
    // in MemorySegmentAllocator.allocate.
    final FrameSlotAllocator.PressureListener pressureListener = () -> {
      recordPageCache.evictUnderPressure();
      recordPageFragmentCache.evictUnderPressure();
      hotLeafPageCache.evictUnderPressure();
      hotLeafFragmentCache.evictUnderPressure();
      pageCache.clear();
    };
    FrameSlotAllocator.setPressureListener(pressureListener);
    LinuxMemorySegmentAllocator.setPressureListener(pressureListener);

    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
    redBlackTreeNodeCache = new RedBlackTreeNodeCache(maxRBTreeNodeCache);
    namesCache = new NamesCache(maxNamesCacheSize);
    pathSummaryCache = new PathSummaryCache(maxPathSummaryCacheSize);

    // Initialize ClockSweeper threads (GLOBAL, like PostgreSQL bgwriter)
    this.clockSweeperThreads = new ArrayList<>();
    this.clockSweepers = new ArrayList<>();

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
   * Start global ClockSweeper threads for this BufferManager. Called once when first database opens.
   * ClockSweepers run until BufferManager shutdown. This follows PostgreSQL bgwriter pattern -
   * background threads run independently of sessions.
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
    {
      ShardedPageCache.Shard<KeyValueLeafPage> shard = recordPageCache.getShard(new PageReference());
      ClockSweeper sweeper = new ClockSweeper(shard, recordPageCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);
      Thread thread = new Thread(sweeper, "ClockSweeper-RecordPage-GLOBAL");
      thread.setDaemon(true);
      thread.start();
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      LOGGER.info("Started GLOBAL ClockSweeper thread for RecordPageCache");
    }

    // Start ClockSweeper for RecordPageFragmentCache (GLOBAL)
    {
      ShardedPageCache.Shard<KeyValueLeafPage> shard = recordPageFragmentCache.getShard(new PageReference());
      ClockSweeper sweeper = new ClockSweeper(shard, recordPageFragmentCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);
      Thread thread = new Thread(sweeper, "ClockSweeper-FragmentPage-GLOBAL");
      thread.setDaemon(true);
      thread.start();
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      LOGGER.info("Started GLOBAL ClockSweeper thread for RecordPageFragmentCache");
    }

    // Start ClockSweeper for HOTLeafPageCache (GLOBAL)
    {
      ShardedPageCache.Shard<HOTLeafPage> shard = hotLeafPageCache.getShard(new PageReference());
      ClockSweeper sweeper = new ClockSweeper(shard, hotLeafPageCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);
      Thread thread = new Thread(sweeper, "ClockSweeper-HOTLeafPage-GLOBAL");
      thread.setDaemon(true);
      thread.start();
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      LOGGER.info("Started GLOBAL ClockSweeper thread for HOTLeafPageCache");
    }

    // Start ClockSweeper for HOTLeafFragmentCache (GLOBAL)
    {
      ShardedPageCache.Shard<HOTLeafPage> shard = hotLeafFragmentCache.getShard(new PageReference());
      ClockSweeper sweeper = new ClockSweeper(shard, hotLeafFragmentCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);
      Thread thread = new Thread(sweeper, "ClockSweeper-HOTLeafFragment-GLOBAL");
      thread.setDaemon(true);
      thread.start();
      clockSweepers.add(sweeper);
      clockSweeperThreads.add(thread);
      LOGGER.info("Started GLOBAL ClockSweeper thread for HOTLeafFragmentCache");
    }
  }

  /**
   * Stop all global ClockSweeper threads. Called when BufferManager is shut down (last database
   * closes) or when clearing all caches.
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
      // Release the buffers. Without this, closing a manager only stopped its sweeper threads and
      // left every cached page alive: the caches went unreachable along with the manager, and their
      // off-heap frames were never freed — no cache to evict them from and no owner to close them.
      // It is why a run that replaces the global manager (reinitializeBufferManagerForTesting) ended
      // with dozens of unguarded, uncached, unclosed pages in the -Dsirix.debug.memory.leaks census.
      // Sweepers are stopped first so none is mid-eviction while the maps are torn down.
      clearAllCaches();
      isShutdown = true;
    }
  }

  /**
   * Clears all caches, closing all cached pages.
   * <p>
   * This is typically called during database shutdown. ClockSweeper threads continue running (they
   * handle future evictions as new pages are loaded).
   */
  @Override
  public void clearAllCaches() {
    pageCache.clear();
    recordPageCache.clear();
    recordPageFragmentCache.clear();
    hotLeafPageCache.clear();
    hotLeafFragmentCache.clear();
    revisionRootPageCache.clear();
    redBlackTreeNodeCache.clear();
    namesCache.clear();
    pathSummaryCache.clear();
  }

  @Override
  public Cache<PageReference, HOTLeafPage> getHOTLeafPageCache() {
    return hotLeafPageCache;
  }

  @Override
  public Cache<PageReference, HOTLeafPage> getHOTLeafFragmentCache() {
    return hotLeafFragmentCache;
  }

  // ===== Metrics accessors =====
  // Exposed so SirixMetricsRegistry can publish per-cache size gauges without
  // pulling Micrometer into sirix-core. Read-only views; safe to poll at scrape
  // cadence from any thread.

  /** Pages found still guarded by an invalidation sweep; see {@link #GUARDED_PAGES_SWEPT}. */
  public static long getGuardedPagesSweptCount() {
    return GUARDED_PAGES_SWEPT.sum();
  }

  /** Current weight (bytes) held by the record-page cache. */
  public long getRecordPageCacheCurrentWeightBytes() {
    return recordPageCache.getCurrentWeightBytes();
  }

  /** Configured max weight (bytes) of the record-page cache. */
  public long getRecordPageCacheMaxWeightBytes() {
    return recordPageCache.getMaxWeightBytes();
  }

  /** Current weight (bytes) held by the record-page-fragment cache. */
  public long getRecordPageFragmentCacheCurrentWeightBytes() {
    return recordPageFragmentCache.getCurrentWeightBytes();
  }

  /** Configured max weight (bytes) of the record-page-fragment cache. */
  public long getRecordPageFragmentCacheMaxWeightBytes() {
    return recordPageFragmentCache.getMaxWeightBytes();
  }

  /** Current weight (bytes) held by the HOT-leaf cache. */
  public long getHOTLeafPageCacheCurrentWeightBytes() {
    return hotLeafPageCache.getCurrentWeightBytes();
  }

  /** Configured max weight (bytes) of the HOT-leaf cache. */
  public long getHOTLeafPageCacheMaxWeightBytes() {
    return hotLeafPageCache.getMaxWeightBytes();
  }

  /** Current weight (bytes) held by the HOT-leaf-fragment cache. */
  public long getHOTLeafFragmentCacheCurrentWeightBytes() {
    return hotLeafFragmentCache.getCurrentWeightBytes();
  }

  /** Configured max weight (bytes) of the HOT-leaf-fragment cache. */
  public long getHOTLeafFragmentCacheMaxWeightBytes() {
    return hotLeafFragmentCache.getMaxWeightBytes();
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
    var recordKeysToRemove = new ArrayList<PageReference>();
    for (var entry : recordPageCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        recordKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : recordKeysToRemove) {
      KeyValueLeafPage page = recordPageCache.get(key);
      if (page != null && !page.isClosed()) {
        // Teardown/destructive-admin path. NEVER force-release guards this thread does not own:
        // the sweep is database-scoped while the truncation that triggers it is resource-scoped, so
        // a live transaction on a sibling resource can be holding a guard on a page whose bytes were
        // never truncated — draining frees the frame under it. markOrphaned() + close() is the
        // guard-aware protocol TransactionIntentLog.closePage already uses: close() alone SKIPS a
        // guarded page without arranging any teardown, while the orphan bit makes the holder's last
        // releaseGuard() free the slot.
        if (page.getGuardCount() > 0) {
          GUARDED_PAGES_SWEPT.increment();
        }
        page.retire();
      }
      recordPageCache.remove(key);
      removedFromRecordCache++;
    }

    // Clear RecordPageFragmentCache - close fragments BEFORE removing from cache
    var fragmentKeysToRemove = new ArrayList<PageReference>();
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        fragmentKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : fragmentKeysToRemove) {
      KeyValueLeafPage page = recordPageFragmentCache.get(key);
      if (page != null && !page.isClosed()) {
        // See above: unmap, never drain a guard this thread does not own.
        if (page.getGuardCount() > 0) {
          GUARDED_PAGES_SWEPT.increment();
        }
        page.retire();
      }
      recordPageFragmentCache.remove(key);
      removedFromFragmentCache++;
    }

    // Clear PageCache
    var pageKeysToRemove = new ArrayList<PageReference>();
    for (var entry : pageCache.asMap().entrySet()) {
      if (entry.getKey().getDatabaseId() == databaseId) {
        pageKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : pageKeysToRemove) {
      pageCache.remove(key); // Cache.remove() is thread-safe
      removedFromPageCache++;
    }

    // Clear RevisionRootPageCache
    var revisionKeysToRemove = new ArrayList<RevisionRootPageCacheKey>();
    for (var entry : revisionRootPageCache.asMap().entrySet()) {
      if (entry.getKey().databaseId() == databaseId) { // Record field access
        revisionKeysToRemove.add(entry.getKey());
      }
    }
    for (var key : revisionKeysToRemove) {
      revisionRootPageCache.remove(key); // Thread-safe
      removedFromRevisionCache++;
    }

    if (removedFromRecordCache + removedFromFragmentCache + removedFromPageCache + removedFromRevisionCache > 0) {
      LOGGER.debug("Cleared caches for database {}: RecordCache={}, FragmentCache={}, PageCache={}, RevisionCache={}",
          databaseId, removedFromRecordCache, removedFromFragmentCache, removedFromPageCache, removedFromRevisionCache);
    }

    // HOT leaf + fragment caches: truncation reuses the freed offsets, so a stale HOT fragment would
    // be merged into a live leaf. Deliberately OUTSIDE the log guard above — gating this on the
    // record/page/revision counters would skip HOT invalidation whenever those caches happened to
    // hold nothing for this database (a HOT-only resource, or a second clear right after a first),
    // which is precisely the stale-fragment path.
    clearHotPageCaches(key -> key.getDatabaseId() == databaseId);
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
    var recordKeysToRemove = new ArrayList<PageReference>();
    for (var entry : recordPageCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        recordKeysToRemove.add(key);
      }
    }
    for (var key : recordKeysToRemove) {
      KeyValueLeafPage page = recordPageCache.get(key);
      if (page != null && !page.isClosed()) {
        // Teardown/destructive-admin path. NEVER force-release guards this thread does not own:
        // the sweep is database-scoped while the truncation that triggers it is resource-scoped, so
        // a live transaction on a sibling resource can be holding a guard on a page whose bytes were
        // never truncated — draining frees the frame under it. markOrphaned() + close() is the
        // guard-aware protocol TransactionIntentLog.closePage already uses: close() alone SKIPS a
        // guarded page without arranging any teardown, while the orphan bit makes the holder's last
        // releaseGuard() free the slot.
        if (page.getGuardCount() > 0) {
          GUARDED_PAGES_SWEPT.increment();
        }
        page.retire();
      }
      recordPageCache.remove(key);
      removedFromRecordCache++;
    }

    // Clear RecordPageFragmentCache - close fragments BEFORE removing from cache
    var fragmentKeysToRemove = new ArrayList<PageReference>();
    for (var entry : recordPageFragmentCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        fragmentKeysToRemove.add(key);
      }
    }
    for (var key : fragmentKeysToRemove) {
      KeyValueLeafPage page = recordPageFragmentCache.get(key);
      if (page != null && !page.isClosed()) {
        // See above: unmap, never drain a guard this thread does not own.
        if (page.getGuardCount() > 0) {
          GUARDED_PAGES_SWEPT.increment();
        }
        page.retire();
      }
      recordPageFragmentCache.remove(key);
      removedFromFragmentCache++;
    }

    // Clear PageCache
    var pageKeysToRemove = new ArrayList<PageReference>();
    for (var entry : pageCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.getDatabaseId() == databaseId && key.getResourceId() == resourceId) {
        pageKeysToRemove.add(key);
      }
    }
    for (var key : pageKeysToRemove) {
      pageCache.remove(key); // Cache.remove() is thread-safe
      removedFromPageCache++;
    }

    // Clear RevisionRootPageCache
    var revisionKeysToRemove = new ArrayList<RevisionRootPageCacheKey>();
    for (var entry : revisionRootPageCache.asMap().entrySet()) {
      var key = entry.getKey();
      if (key.databaseId() == databaseId && key.resourceId() == resourceId) {
        revisionKeysToRemove.add(key);
      }
    }
    for (var key : revisionKeysToRemove) {
      revisionRootPageCache.remove(key); // Thread-safe
      removedFromRevisionCache++;
    }

    if (removedFromRecordCache + removedFromFragmentCache + removedFromPageCache + removedFromRevisionCache > 0) {
      LOGGER.debug(
          "Cleared caches for resource (db={}, res={}): RecordCache={}, FragmentCache={}, PageCache={}, RevisionCache={}",
          databaseId, resourceId, removedFromRecordCache, removedFromFragmentCache, removedFromPageCache,
          removedFromRevisionCache);
    }

    // See clearCachesForDatabase: reused offsets after truncation make stale HOT fragments a
    // silent-wrong-data hazard.
    clearHotPageCaches(key -> key.getDatabaseId() == databaseId && key.getResourceId() == resourceId);
  }

  /**
   * Sweep both HOT caches, guaranteeing the fragment cache is swept even if the leaf sweep throws.
   *
   * <p>Sequential statements would not: the fragment cache is the one whose stale entries are
   * silently merged into a live leaf after truncation reuses their offsets, so letting an exception
   * from the leaf sweep skip it reaches the same wrong-data outcome as not calling it at all.</p>
   */
  private void clearHotPageCaches(final Predicate<PageReference> matches) {
    try {
      clearHotPageCache(hotLeafPageCache, matches);
    } finally {
      clearHotPageCache(hotLeafFragmentCache, matches);
    }
  }

  /**
   * Drop every entry of a HOT page cache whose key matches.
   *
   * <p>Both truncate/rollback paths depend on this: {@code truncateTo} shortens the data file and the
   * NEXT commit REUSES those offsets, so a HOT fragment cached under a reused offset would serve
   * pre-truncation bytes into a live merge — silent wrong data, no exception. Fragments are immutable
   * only while the file is append-only, which truncation ends.</p>
   *
   * <p><b>Guards are NOT drained here</b>, unlike the record-page blocks above. A HOT chain fragment
   * is a SHARED cache entry held under a guard for the length of a merge, and draining that guard
   * would free the off-heap slot under it (torn read, or a {@code releaseGuard} underflow when the
   * merge's {@code finally} runs). Dropping the mapping is what the stale-offset hazard actually
   * requires; {@link CacheablePage#retire()} is guard-aware and defers the teardown to the last
   * release, so the slot is still reclaimed, just not out from under a live reader.</p>
   *
   * <p><b>That deferral only exists for the FRAGMENT cache.</b> Entries of the HOT leaf-page cache
   * are handed out unguarded, so {@code retire()} on one frees its frame there and then — exactly
   * what ordinary eviction does to an unguarded page, and safe for the same reason: this sweep runs
   * only from truncate/rollback, whose documented precondition is that nothing is reading the
   * resource's file. What makes that precondition sufficient is that the sweep is RESOURCE-scoped
   * (see {@code Databases.clearCachesForResource}); a database-wide sweep would reach sibling
   * resources the precondition never covered, and free pages under their live readers.</p>
   */
  private static int clearHotPageCache(final ShardedPageCache<HOTLeafPage> cache,
      final Predicate<PageReference> matches) {
    final List<PageReference> keysToRemove = new ArrayList<>();
    for (var entry : cache.asMap().entrySet()) {
      if (matches.test(entry.getKey())) {
        keysToRemove.add(entry.getKey());
      }
    }
    int removed = 0;
    for (final PageReference key : keysToRemove) {
      // removeAndGet, NOT get-then-remove: the two-step version retires whatever the GET saw while
      // the REMOVE unmaps whatever is there now. A page cached between the two is then dropped from
      // every cache unclosed (a pinned frame for the process's life), and — worse — a page the
      // TransactionIntentLog claimed in between (see removeHOTLeavesFromCache, which exists because
      // one instance can be both a container page and a cache entry) is freed while the writer still
      // owns it for commit. One atomic step means we only ever retire the page we actually unmapped.
      final HOTLeafPage page = cache.removeAndGet(key);
      if (page != null && !page.isClosed()) {
        page.retire();
      }
      removed++;
    }
    return removed;
  }

}
