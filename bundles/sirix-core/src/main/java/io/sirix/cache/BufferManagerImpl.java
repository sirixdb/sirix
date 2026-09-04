package io.sirix.cache;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

/**
 * Global buffer manager for SirixDB page caching.
 * <p>
 * This component manages all page caches for a database, providing:
 * <ul>
 * <li><b>Record page cache:</b> Full KeyValueLeafPages for data access</li>
 * <li><b>Fragment cache:</b> Page fragments for versioning reconstruction</li>
 * <li><b>Page cache:</b> Other page types (NamePage, RevisionRootPage, etc.)</li>
 * <li><b>Specialized caches:</b> RevisionRootPages, HOT lookups, Names, PathSummary</li>
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
   * <p>
   * A destructive sweep is supposed to run when nothing of the swept scope is in use, so a non-zero
   * count means some caller invalidates while it is itself holding pages — the shape of bug that made
   * {@code createPageTransaction} truncate after constructing its writer. The sweep stays correct
   * either way (teardown defers to the holder's last release), so this is an anomaly signal rather
   * than an error, and it costs one already-loaded field read per swept page on a cold path.
   */
  private static final LongAdder GUARDED_PAGES_SWEPT = new LongAdder();

  /**
   * Smallest useful HOT fragment budget: 32 fragments, i.e. about 16 concurrently written leaves at
   * the default chain cap. Below this the cache thrashes instead of serving anything.
   */
  private static final long MIN_HOT_FRAGMENT_BUDGET_BYTES = 32L * HOTLeafPage.DEFAULT_SIZE;

  /**
   * Memoized HOT point lookups to retain.
   *
   * <p>
   * The CEILING when no budget is available to derive one from — see
   * {@link #defaultHotLookupCacheEntries(long)}, which is what the constructor actually uses. A FULL
   * entry is the {@code long[]} ({@code MAX_CACHED_NODE_KEYS} x 8 = ~2 KB) plus the key's owned byte
   * copy plus two small objects, so 64K entries is ~150 MB if every slot holds a maximum-length
   * posting list. That is the worst case, not the expectation — posting lists near the bound are
   * rare, absent-key entries cost a shared empty array, and the table only fills where keys are
   * actually re-asked — but it is the number a bound has to be built from.
   * </p>
   */
  private static final int DEFAULT_HOT_LOOKUP_CACHE_ENTRIES = 1 << 16;

  /**
   * Worst-case retained bytes per admitted entry, for turning a byte budget into an entry count.
   *
   * <p>
   * {@code long[MAX_CACHED_NODE_KEYS]} (2064) + the {@code Entry} record (~24) + the
   * {@link HOTLookupKey} (~56) + its owned key copy (~256 for a maxed CAS key). Deliberately the
   * worst case: an average would size the table so that a workload of large posting lists overshoots
   * the budget, which is exactly the workload that most needs the bound to hold.
   * </p>
   */
  private static final long HOT_LOOKUP_WORST_CASE_ENTRY_BYTES = 2400L;

  /**
   * Share of the record-page budget the lookup cache may retain on the heap.
   *
   * <p>
   * A thirty-second, which is deliberately small: every other cache here is budgeted in BYTES from
   * the same pool, and this one is the only one that is ON-heap in a process that keeps its page
   * frames off it. Sizing it in entries alone — which is what shipped first — meant the default was a
   * flat count that no configured budget could restrain, so a heap tuned around the page caches
   * silently acquired up to ~150 MB it never accounted for.
   * </p>
   */
  private static final long HOT_LOOKUP_BUDGET_DIVISOR = 32L;

  /** Floor, so a tiny budget yields a cache that is still worth consulting rather than one slot. */
  private static final int MIN_HOT_LOOKUP_CACHE_ENTRIES = 1 << 10;

  /**
   * Entry count for {@code recordPageBudgetBytes}, bounded by the same pool as its siblings.
   *
   * @param recordPageBudgetBytes the record-page cache budget this instance was constructed with
   * @return the entry count to build the lookup cache with
   */
  private static int defaultHotLookupCacheEntries(final long recordPageBudgetBytes) {
    if (recordPageBudgetBytes <= 0) {
      return DEFAULT_HOT_LOOKUP_CACHE_ENTRIES;
    }
    final long affordable = recordPageBudgetBytes / HOT_LOOKUP_BUDGET_DIVISOR / HOT_LOOKUP_WORST_CASE_ENTRY_BYTES;
    final long bounded = Math.min(affordable, DEFAULT_HOT_LOOKUP_CACHE_ENTRIES);
    return (int) Math.max(bounded, MIN_HOT_LOOKUP_CACHE_ENTRIES);
  }

  /**
   * System property overriding {@link #DEFAULT_HOT_LOOKUP_CACHE_ENTRIES}.
   *
   * <p>
   * Operator-facing, because the right size depends on the read pattern rather than on the data: the
   * cache pays off only for keys asked about more than once within a revision, so a workload with no
   * key reuse wants it small and one serving repeated lookups wants it large. {@code 0} disables the
   * cache outright, which is also how a measurement isolates the MISS path — sizing it below the
   * working set is the only way to see what the memoization costs when it never hits.
   * </p>
   */
  public static final String HOT_LOOKUP_CACHE_ENTRIES_PROPERTY = "sirix.hotLookupCache.maxEntries";

  /**
   * @param recordPageBudgetBytes the record-page cache budget this instance was constructed with
   * @param configured the raw property value, read ONCE by the caller and passed in — reading it a
   *        second time for the startup log let a value that appeared or vanished between the two
   *        reads make the log report the wrong provenance for the size actually built
   * @return the entry count to build the lookup cache with
   */
  private static HotLookupSize hotLookupCacheEntries(final long recordPageBudgetBytes,
      final @Nullable String configured) {
    if (configured == null) {
      return new HotLookupSize(defaultHotLookupCacheEntries(recordPageBudgetBytes), false);
    }
    // Not final: javac treats a blank final assigned in the `try` as possibly-assigned on entry to
    // the `catch`, so the catch's assignment does not compile.
    int parsed;
    try {
      parsed = Integer.parseInt(configured.trim());
    } catch (final NumberFormatException e) {
      parsed = -1; // unusable, same as a negative value — one rejection rule, one message
    }
    // Negatives are NOT clamped silently to 0: 0 is the documented "disable" sentinel, so a typo'd
    // minus sign would turn the cache off and cost every lookup full price with nothing in the log.
    if (parsed < 0) {
      // The SAME default the no-property path takes, which is the whole point: falling back to the
      // raw DEFAULT_HOT_LOOKUP_CACHE_ENTRIES ceiling instead meant a single typo'd or negative value
      // built a 65536-slot table where setting nothing at all builds 4096 — 16x the on-heap
      // retention, past the very budget HOT_LOOKUP_BUDGET_DIVISOR exists to enforce — and announced
      // it as "the default", which it was not. An invalid override must land exactly where no
      // override lands.
      final int fallback = defaultHotLookupCacheEntries(recordPageBudgetBytes);
      LOGGER.warn("Ignoring invalid {}={} (expected an integer >= 0), using the default of {} entries",
          HOT_LOOKUP_CACHE_ENTRIES_PROPERTY, configured, fallback);
      return new HotLookupSize(fallback, false);
    }
    // A huge POSITIVE value is clamped too, and loudly. The rejection rule above only caught
    // negatives, so an extra zero on an otherwise sensible value sailed through: the constructor
    // clamps to MAX_SLOTS internally and silently, so the operator got a 16.7M-slot table — hundreds
    // of megabytes of references alone — while the startup line below dutifully reported the enormous
    // figure as if it had been honoured. Clamping HERE is what makes the discrepancy visible; the
    // ceiling itself is the constructor's, restated so the two cannot drift apart.
    if (parsed > HOTLookupCache.MAX_SLOTS) {
      LOGGER.warn("Clamping {}={} to the maximum of {} entries (~{} MB retained)", HOT_LOOKUP_CACHE_ENTRIES_PROPERTY,
          parsed, HOTLookupCache.MAX_SLOTS,
          (long) HOTLookupCache.MAX_SLOTS * HOT_LOOKUP_WORST_CASE_ENTRY_BYTES / (1024 * 1024));
      // fromProperty = false: the property did not decide the size, the ceiling did, and the log
      // line must not attribute the built table to a number that was overridden.
      return new HotLookupSize(HOTLookupCache.MAX_SLOTS, false);
    }
    return new HotLookupSize(parsed, true);
  }

  /**
   * The entry count to build the lookup cache with, and whether the property is what decided it.
   *
   * <p>
   * One value carrying both, rather than a second predicate re-deriving provenance from the raw
   * string, because the two drifted the moment they were separate: a rejected value logged "Ignoring
   * invalid …" and was then reported as "set by …" four lines later, and an over-large value was
   * clamped and still reported verbatim. Both times the startup line pointed an operator triaging a
   * mis-set override at the one number that was NOT in effect.
   * </p>
   *
   * @param entries the size the cache is built with
   * @param fromProperty whether the property both was set and survived validation unchanged, so the
   *        startup line may attribute the size to it
   */
  private record HotLookupSize(int entries, boolean fromProperty) {
  }

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
  // Memoized HOT point-lookup answers. Sized independently of the page caches: entries are a key
  // plus a bounded long[], so this is kilobytes where the page caches are megabytes.
  /**
   * Verdict-cache budget. A verdict is one bit per dictionary id, so this is tens of kilobytes at a
   * million distinct values and a few megabytes at a hundred million; the bound is in BYTES so the
   * second scale cannot quietly cost hundreds of megabytes the way a count bound would.
   */
  private static final long GLOBAL_VERDICT_CACHE_BYTES =
      Long.getLong("sirix.projection.globalDict.verdictCacheBytes", 64L << 20);

  /**
   * Decoded dictionary bytes retained across transactions. Sized to hold a mid-cardinality column's
   * blocks outright; above that it degrades to the hit rate its weight supports, which is the point
   * of metering it rather than sizing it from the dictionary.
   */
  private static final long GLOBAL_DICTIONARY_RECORD_CACHE_BYTES =
      Long.getLong("sirix.projection.globalDict.recordCacheBytes", 256L << 20);

  private final HOTLookupCache hotLookupCache;
  private final NamesCache namesCache;
  private final PathSummaryCache pathSummaryCache;
  private final GlobalVerdictCache globalVerdictCache;
  private final GlobalDictionaryRecordCache globalDictionaryRecordCache;
  private final GlobalDictionaryWarmMarkerCache globalDictionaryWarmMarkers;

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
   * @param maxNamesCacheSize maximum number of name entries to cache
   * @param maxPathSummaryCacheSize maximum number of path summary entries to cache
   */
  public BufferManagerImpl(long maxPageCacheWeight, long maxRecordPageCacheWeight,
      long maxRecordPageFragmentCacheWeight, int maxRevisionRootPageCache, int maxNamesCacheSize,
      int maxPathSummaryCacheSize) {
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
      // Held-vs-budget for every cache backed by the exhausted arena. Without it, an arena OOM is
      // uninterpretable: the caches can be nowhere near their own budgets and still be pinning every
      // frame, and nothing else reports the two numbers side by side.
      LOGGER.debug(
          "Allocator pressure: recordPage {}/{} B ({} entries), fragment {}/{} B, hotLeaf {}/{} B, "
              + "hotFragment {}/{} B",
          recordPageCache.getCurrentWeightBytes(), recordPageCache.getMaxWeightBytes(), recordPageCache.size(),
          recordPageFragmentCache.getCurrentWeightBytes(), recordPageFragmentCache.getMaxWeightBytes(),
          hotLeafPageCache.getCurrentWeightBytes(), hotLeafPageCache.getMaxWeightBytes(),
          hotLeafFragmentCache.getCurrentWeightBytes(), hotLeafFragmentCache.getMaxWeightBytes());
      recordPageCache.evictUnderPressure();
      recordPageFragmentCache.evictUnderPressure();
      hotLeafPageCache.evictUnderPressure();
      hotLeafFragmentCache.evictUnderPressure();
      pageCache.clear();
    };
    FrameSlotAllocator.setPressureListener(pressureListener);
    LinuxMemorySegmentAllocator.setPressureListener(pressureListener);

    revisionRootPageCache = new RevisionRootPageCache(maxRevisionRootPageCache);
    final String configuredHotLookupEntries = System.getProperty(HOT_LOOKUP_CACHE_ENTRIES_PROPERTY);
    final HotLookupSize hotLookupSize = hotLookupCacheEntries(maxRecordPageCacheWeight, configuredHotLookupEntries);
    final int hotLookupEntries = hotLookupSize.entries();
    hotLookupCache = hotLookupEntries == 0
        ? HOTLookupCache.disabled()
        : new HOTLookupCache(hotLookupEntries);
    namesCache = new NamesCache(maxNamesCacheSize);
    pathSummaryCache = new PathSummaryCache(maxPathSummaryCacheSize);
    globalVerdictCache = new GlobalVerdictCache(GLOBAL_VERDICT_CACHE_BYTES);
    globalDictionaryRecordCache = new GlobalDictionaryRecordCache(GLOBAL_DICTIONARY_RECORD_CACHE_BYTES);
    globalDictionaryWarmMarkers = new GlobalDictionaryWarmMarkerCache();

    // Initialize ClockSweeper threads (GLOBAL, like PostgreSQL bgwriter)
    this.clockSweeperThreads = new ArrayList<>();
    this.clockSweepers = new ArrayList<>();

    LOGGER.info("BufferManagerImpl initialized with large cache support:");
    LOGGER.info("  - RecordPageCache: {} MB", maxRecordPageCacheWeight / (1024 * 1024));
    LOGGER.info("  - RecordPageFragmentCache: {} MB", maxRecordPageFragmentCacheWeight / (1024 * 1024));
    LOGGER.info("  - PageCache: {} MB", maxPageCacheWeight / (1024 * 1024));
    // Reported alongside its siblings, in the same units they are budgeted in. Without this the one
    // cache an operator can size by system property was also the only one whose effective size never
    // appeared anywhere, so a mis-set (or ignored) override was invisible during triage.
    //
    // The figure is capacity(), the table that was BUILT, not the entry count that was requested:
    // the constructor rounds down to a power-of-two set count, so with the shipped default the two
    // differ by nearly half (6990 requested, 4096 built) — and a log line whose whole purpose is to
    // show the effective size must not print the number the cache ignored.
    final int hotLookupSlots = hotLookupCache.capacity();
    LOGGER.info("  - HOTLookupCache: {} entries, up to ~{} MB retained{}", hotLookupSlots,
        hotLookupSlots * HOT_LOOKUP_WORST_CASE_ENTRY_BYTES / (1024 * 1024), hotLookupSize.fromProperty()
            ? " (set by " + HOT_LOOKUP_CACHE_ENTRIES_PROPERTY + "=" + configuredHotLookupEntries + ')'
            : " (derived from the record-page budget)");
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
  public HOTLookupCache getHOTLookupCache() {
    return hotLookupCache;
  }

  @Override
  public NamesCache getNamesCache() {
    return namesCache;
  }

  @Override
  public GlobalVerdictCache getGlobalVerdictCache() {
    return globalVerdictCache;
  }

  @Override
  public GlobalDictionaryRecordCache getGlobalDictionaryRecordCache() {
    return globalDictionaryRecordCache;
  }

  @Override
  public GlobalDictionaryWarmMarkerCache getGlobalDictionaryWarmMarkers() {
    return globalDictionaryWarmMarkers;
  }

  /**
   * Drops this resource's dictionary-derived state: verdicts, decoded records and warm markers.
   *
   * <p>
   * All three are keyed by {@code (databaseId, resourceId, ...)}, so a resource recreated with the
   * same ids would otherwise be served its predecessor's answers -- the pollution
   * {@code clearCachesForResource}'s own comment exists to prevent. The marker is the worst of the
   * three: surviving alone it reports "already warm" over caches that were just swept, and the warmer
   * never runs again for that resource.
   * </p>
   */
  private void clearDictionaryCachesForResource(final long databaseId, final long resourceId) {
    globalVerdictCache.asMap()
                      .keySet()
                      .removeIf(key -> key.databaseId() == databaseId && key.resourceId() == resourceId);
    globalDictionaryRecordCache.asMap()
                               .keySet()
                               .removeIf(key -> key.databaseId() == databaseId && key.resourceId() == resourceId);
    globalDictionaryWarmMarkers.asMap()
                               .keySet()
                               .removeIf(key -> key.databaseId() == databaseId && key.resourceId() == resourceId);
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
      final ShardedPageCache.Shard<KeyValueLeafPage> shard = recordPageFragmentCache.getShard(new PageReference());
      final ClockSweeper sweeper =
          new ClockSweeper(shard, recordPageFragmentCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);
      final Thread thread = new Thread(sweeper, "ClockSweeper-FragmentPage-GLOBAL");
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
      final ShardedPageCache.Shard<HOTLeafPage> shard = hotLeafFragmentCache.getShard(new PageReference());
      final ClockSweeper sweeper =
          new ClockSweeper(shard, hotLeafFragmentCache, globalEpochTracker, sweepIntervalMs, 0, 0, 0);
      final Thread thread = new Thread(sweeper, "ClockSweeper-HOTLeafFragment-GLOBAL");
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
    // Memoized index answers are derived from the pages cleared below, so they go with them: this is
    // the "cold process" contract Databases.clearGlobalCaches() promises its corruption tests. In a
    // finally because an exception from any page cache above would otherwise leave the derived
    // answers behind — a cache that outlives the pages it was derived from is precisely what the
    // "cold process" contract rules out.
    try {
      pageCache.clear();
      recordPageCache.clear();
      recordPageFragmentCache.clear();
      hotLeafPageCache.clear();
      hotLeafFragmentCache.clear();
      revisionRootPageCache.clear();
      namesCache.clear();
      globalVerdictCache.clear();
      globalDictionaryRecordCache.clear();
      globalDictionaryWarmMarkers.clear();
      pathSummaryCache.clear();
    } finally {
      hotLookupCache.clear();
    }
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
    // try/catch/finally around the WHOLE body, not just around the HOT page sweep. The body's
    // exception is CAPTURED rather than left to propagate on its own, because a bare finally that
    // itself throws discards it outright — not even as a suppressed cause — and the body's failure is
    // the one an operator needs. It is rethrown below with the sweep's failure attached. The memoized
    // answers
    // are derived from these pages, and the loops above retire live off-heap frames and build
    // unbounded key lists — any of which can throw. When one did, this method unwound before the
    // memoized answers were ever swept, and truncateTo went on to re-issue the very revision numbers
    // those answers are keyed by: every later lookup at those revisions was served from discarded
    // history, permanently. The page sweep failing is bad; the page sweep failing SILENTLY while the
    // answers derived from it survive is unrecoverable, so the second sweep must not depend on the
    // first one finishing. clearAllCaches has had this guarantee all along.
    // CRITICAL FIX: Remove all pages belonging to this database from global caches
    // This prevents cache pollution when database is removed and recreated with same ID
    // THREAD-SAFE: Collect keys first, then remove atomically to avoid concurrent modification
    // Declared OUTSIDE the try so the finally below can report what was actually removed before a
    // failure, rather than losing the tally with the frame.
    int removedFromRecordCache = 0;
    int removedFromFragmentCache = 0;
    int removedFromPageCache = 0;
    int removedFromRevisionCache = 0;

    // Holds the body's failure so the finally can attach the sweep's to it instead of replacing it.
    Throwable fromBody = null;

    try {

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
    } catch (final Throwable e) {
      fromBody = e;
      throw e;
    } finally {
      sweepHotCachesAndReport(fromBody, key -> key.getDatabaseId() == databaseId,
          () -> hotLookupCache.invalidateDatabase(databaseId), "database " + databaseId, removedFromRecordCache,
          removedFromFragmentCache, removedFromPageCache, removedFromRevisionCache);
    }
  }

  @Override
  public void clearCachesForResource(long databaseId, long resourceId) {
    // Before anything else, and unconditionally: the dictionary-derived caches are keyed by
    // (databaseId, resourceId, ...) and a resource recreated with the same ids would otherwise be
    // served its predecessor's verdicts, records and warm markers.
    clearDictionaryCachesForResource(databaseId, resourceId);
    // try/catch/finally around the WHOLE body, not just around the HOT page sweep. The body's
    // exception is CAPTURED rather than left to propagate on its own, because a bare finally that
    // itself throws discards it outright — not even as a suppressed cause — and the body's failure is
    // the one an operator needs. It is rethrown below with the sweep's failure attached. The memoized
    // answers
    // are derived from these pages, and the loops above retire live off-heap frames and build
    // unbounded key lists — any of which can throw. When one did, this method unwound before the
    // memoized answers were ever swept, and truncateTo went on to re-issue the very revision numbers
    // those answers are keyed by: every later lookup at those revisions was served from discarded
    // history, permanently. The page sweep failing is bad; the page sweep failing SILENTLY while the
    // answers derived from it survive is unrecoverable, so the second sweep must not depend on the
    // first one finishing. clearAllCaches has had this guarantee all along.
    // CRITICAL FIX: Remove all pages belonging to this resource from global caches
    // This prevents cache pollution when resource is closed and recreated with same IDs
    // THREAD-SAFE: Collect keys first, then remove atomically to avoid concurrent modification
    // Declared OUTSIDE the try so the finally below can report what was actually removed before a
    // failure, rather than losing the tally with the frame.
    int removedFromRecordCache = 0;
    int removedFromFragmentCache = 0;
    int removedFromPageCache = 0;
    int removedFromRevisionCache = 0;

    // Holds the body's failure so the finally can attach the sweep's to it instead of replacing it.
    Throwable fromBody = null;

    try {

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
    } catch (final Throwable e) {
      fromBody = e;
      throw e;
    } finally {
      sweepHotCachesAndReport(fromBody, key -> key.getDatabaseId() == databaseId && key.getResourceId() == resourceId,
          () -> hotLookupCache.invalidateResource(databaseId, resourceId),
          "resource (db=" + databaseId + ", res=" + resourceId + ')', removedFromRecordCache, removedFromFragmentCache,
          removedFromPageCache, removedFromRevisionCache);
    }
  }

  /**
   * Drop the HOT pages of one scope, then the memoized point-lookup answers DERIVED from them, then
   * report what the whole sweep removed.
   *
   * <p>
   * ONE copy for {@link #clearCachesForDatabase} and {@link #clearCachesForResource}, which differ
   * only in the key predicate and the invalidation scope. Stating the reasoning once is the point —
   * the alternative was two bodies whose identical rationale had to be kept in step by hand.
   * </p>
   *
   * <p>
   * <b>Pages first, answers second.</b> The answers are derived from the pages, so dropping them
   * first leaves a window in which a lookup racing the sweep re-memoizes the entries just removed out
   * of pages the sweep has not reached yet. Note what this ordering does NOT buy: a lookup that
   * already READ the pages before the page sweep can still admit its answer after the answer sweep
   * has passed. Nothing here closes that window — only the quiescence precondition documented on
   * {@link #clearHotPageCache} (nothing is reading the resource's file while it is truncated) does,
   * and it covers the memoized answers for exactly the same reason it covers the pages.
   * </p>
   *
   * <p>
   * <b>Both sweeps always run, and the first failure stays the reported one.</b> The page sweep
   * rethrows, so a plain sequential statement would let its exception skip the answer sweep — the
   * same wrong-data outcome as never sweeping, since the pages would be gone while every answer
   * derived from the discarded history survived under a revision number {@code truncateTo} is about
   * to re-issue. A {@code finally} would run the answer sweep but let ITS exception replace the page
   * sweep's, which is the diagnosis the caller actually needs; hence the explicit capture and
   * {@code addSuppressed}. The counters are logged before the rethrow, because a partially failed
   * sweep is precisely when they matter.
   * </p>
   *
   * <p>
   * The capture is {@code Throwable}, not {@code RuntimeException}. {@code clearHotPageCache} builds
   * a list over every cache entry and calls {@code retire()} on each, so an {@code OutOfMemoryError}
   * during a recovery sweep — or an {@code AssertionError} from a page invariant under {@code -ea} —
   * is a realistic escape, and narrowing the catch to {@code RuntimeException} would let exactly that
   * skip the answer sweep. That is the outcome the paragraph above says this method exists to
   * prevent, so the guarantee has to cover what can actually be thrown.
   * </p>
   *
   * @param fromBody the caller body's failure when it is already propagating, so this attaches its
   *        own rather than replacing it; {@code null} when the body completed normally
   * @param matches selects the HOT page-cache entries of this scope
   * @param answerSweep drops the memoized answers of this scope and returns how many it dropped
   * @param scope human-readable scope for the diagnostic log line
   * @param removedFromRecordCache record-page entries the caller already removed
   * @param removedFromFragmentCache record-fragment entries the caller already removed
   * @param removedFromPageCache page entries the caller already removed
   * @param removedFromRevisionCache revision-root entries the caller already removed
   */
  private void sweepHotCachesAndReport(final @Nullable Throwable fromBody, final Predicate<PageReference> matches,
      final IntSupplier answerSweep, final String scope, final int removedFromRecordCache,
      final int removedFromFragmentCache, final int removedFromPageCache, final int removedFromRevisionCache) {
    Throwable failure = null;
    try {
      clearHotPageCaches(matches);
    } catch (final RuntimeException | Error e) {
      failure = e;
    }
    int removedLookups = 0;
    try {
      removedLookups = answerSweep.getAsInt();
    } catch (final RuntimeException | Error e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    if (removedFromRecordCache + removedFromFragmentCache + removedFromPageCache + removedFromRevisionCache
        + removedLookups > 0) {
      LOGGER.debug(
          "Cleared caches for {}: RecordCache={}, FragmentCache={}, PageCache={}, RevisionCache={}, LookupCache={}",
          scope, removedFromRecordCache, removedFromFragmentCache, removedFromPageCache, removedFromRevisionCache,
          removedLookups);
    }
    if (failure == null) {
      return;
    }
    if (fromBody != null) {
      // The CALLER's body already failed, and its exception is in flight through the finally that
      // invoked this method. Throwing here would silently REPLACE it — Java drops an in-flight
      // exception when a finally completes abruptly, not even recording it as a cause — and the
      // body's failure is both the earlier one and the one that usually explains this one. Attach and
      // return, so the caller sees the original with this hanging off it.
      if (fromBody != failure) {
        fromBody.addSuppressed(failure);
      }
      return;
    }
    // Rethrown unwrapped, in the two shapes the catches above can produce. Neither sweep declares a
    // checked exception, so these are exhaustive and there is nothing to wrap.
    if (failure instanceof final RuntimeException runtime) {
      throw runtime;
    }
    throw (Error) failure;
  }

  /**
   * Sweep both HOT caches, guaranteeing the fragment cache is swept even if the leaf sweep throws.
   *
   * <p>
   * Sequential statements would not: the fragment cache is the one whose stale entries are silently
   * merged into a live leaf after truncation reuses their offsets, so letting an exception from the
   * leaf sweep skip it reaches the same wrong-data outcome as not calling it at all.
   * </p>
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
   * <p>
   * Both truncate/rollback paths depend on this: {@code truncateTo} shortens the data file and the
   * NEXT commit REUSES those offsets, so a HOT fragment cached under a reused offset would serve
   * pre-truncation bytes into a live merge — silent wrong data, no exception. Fragments are immutable
   * only while the file is append-only, which truncation ends.
   * </p>
   *
   * <p>
   * <b>Guards are NOT drained here</b>, unlike the record-page blocks above. A HOT chain fragment is
   * a SHARED cache entry held under a guard for the length of a merge, and draining that guard would
   * free the off-heap slot under it (torn read, or a {@code releaseGuard} underflow when the merge's
   * {@code finally} runs). Dropping the mapping is what the stale-offset hazard actually requires;
   * {@link CacheablePage#retire()} is guard-aware and defers the teardown to the last release, so the
   * slot is still reclaimed, just not out from under a live reader.
   * </p>
   *
   * <p>
   * <b>That deferral only exists for the FRAGMENT cache.</b> Entries of the HOT leaf-page cache are
   * handed out unguarded, so {@code retire()} on one frees its frame there and then — exactly what
   * ordinary eviction does to an unguarded page, and safe for the same reason: this sweep runs only
   * from truncate/rollback, whose documented precondition is that nothing is reading the resource's
   * file. What makes that precondition sufficient is that the sweep is RESOURCE-scoped (see
   * {@code Databases.clearCachesForResource}); a database-wide sweep would reach sibling resources
   * the precondition never covered, and free pages under their live readers.
   * </p>
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
