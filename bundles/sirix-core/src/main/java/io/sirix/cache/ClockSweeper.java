package io.sirix.cache;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.page.PageReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background clock sweeper for page eviction using the second-chance algorithm.
 * <p>
 * This component implements LeanStore/Umbra-style cache eviction with the following characteristics:
 * <ul>
 *   <li><b>Clock-based scanning:</b> Pages are scanned in circular order, minimizing overhead</li>
 *   <li><b>Second-chance algorithm:</b> HOT pages (recently accessed) get a second chance before eviction</li>
 *   <li><b>Revision watermark:</b> Pages needed by active transactions are protected from eviction</li>
 *   <li><b>Guard protection:</b> Pages with active guards (in-use) cannot be evicted</li>
 * </ul>
 * <p>
 * The sweeper runs as a daemon thread and performs incremental scans (10% of cache per cycle)
 * to avoid long pauses. This follows the PostgreSQL bgwriter pattern for background maintenance.
 * <p>
 * <b>Thread Safety:</b> The sweeper uses an eviction lock to coordinate with cache operations.
 * Individual page evictions are protected by ConcurrentHashMap's compute() atomicity.
 *
 * @author Johannes Lichtenberger
 * @see ShardedPageCache
 * @see RevisionEpochTracker
 */
public final class ClockSweeper implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClockSweeper.class);

  private final ShardedPageCache.Shard shard;
  private final ShardedPageCache cache;
  private final RevisionEpochTracker epochTracker;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final int sweepIntervalMs;
  private final int shardIndex;
  private final long databaseId;
  private final long resourceId;

  // Metrics
  private final AtomicLong pagesEvicted = new AtomicLong(0);
  private final AtomicLong pagesSkippedByHot = new AtomicLong(0);
  private final AtomicLong pagesSkippedByWatermark = new AtomicLong(0);
  private final AtomicLong pagesSkippedByGuard = new AtomicLong(0);
  private final AtomicLong pagesSkippedByOwnership = new AtomicLong(0);

  /**
   * Create a new clock sweeper for a shard.
   *
   * @param shard the shard to sweep
   * @param epochTracker the epoch tracker for revision watermark
   * @param sweepIntervalMs how often to sweep (milliseconds)
   * @param shardIndex index of this shard (for logging)
   * @param databaseId database ID to filter pages
   * @param resourceId resource ID to filter pages
   */
  public ClockSweeper(ShardedPageCache.Shard shard, ShardedPageCache cache, RevisionEpochTracker epochTracker,
                      int sweepIntervalMs, int shardIndex, long databaseId, long resourceId) {
    this.shard = shard;
    this.cache = cache;
    this.epochTracker = epochTracker;
    this.sweepIntervalMs = sweepIntervalMs;
    this.shardIndex = shardIndex;
    this.databaseId = databaseId;
    this.resourceId = resourceId;
  }

  /**
   * Stop the sweeper thread.
   */
  public void stop() {
    running.set(false);
  }

  @Override
  public void run() {
    LOGGER.info("ClockSweeper[{}] started (interval={}ms)", shardIndex, sweepIntervalMs);

    while (running.get()) {
      try {
        Thread.sleep(sweepIntervalMs);
        sweep();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.info("ClockSweeper[{}] interrupted", shardIndex);
        break;
      } catch (Exception e) {
        LOGGER.error("ClockSweeper[{}] error during sweep", shardIndex, e);
      }
    }

    LOGGER.info("ClockSweeper[{}] stopped", shardIndex);
  }

  /**
   * Perform one sweep cycle.
   * Scans a fraction of pages (e.g., 10%) per cycle to avoid long pauses.
   */
  private void sweep() {
    if (!shard.evictionLock.tryLock()) {
      // Another sweep in progress, skip this cycle
      return;
    }

    try {
      int minActiveRev = epochTracker.minActiveRevision();
      List<PageReference> keys = new ArrayList<>(shard.map.keySet());
      
      if (keys.isEmpty()) {
        return;
      }

      // Scan 10% of pages per cycle (or minimum 10 pages)
      int pagesToScan = Math.max(10, keys.size() / 10);
      
      for (int i = 0; i < pagesToScan && i < keys.size(); i++) {
        // CRITICAL FIX: Bound clockHand to current keys.size() to handle concurrent modifications
        // The keys list is a snapshot, but the map can grow/shrink concurrently
        if (keys.isEmpty()) {
          break; // No more keys to scan
        }
        int safeIndex = shard.clockHand % keys.size();
        PageReference ref = keys.get(safeIndex);
        
        // Filter by resource if not global (databaseId=0 and resourceId=0 means GLOBAL)
        boolean isGlobalSweeper = (databaseId == 0 && resourceId == 0);
        if (!isGlobalSweeper && (ref.getDatabaseId() != databaseId || ref.getResourceId() != resourceId)) {
          shard.clockHand++;
          pagesSkippedByOwnership.incrementAndGet();
          continue;
        }
        
        // CRITICAL: Use compute() to atomically check guards and evict
        // This prevents TOCTOU race where getAndGuard() could acquire guard
        // between our guardCount check and page.reset()
        shard.map.compute(ref, (k, page) -> {
          if (page == null) {
            // Page was removed by another thread
            return null;
          }

          // All checks and eviction must be atomic within compute()
          
          // Check if page is HOT
          if (page.isHot()) {
            page.clearHot(); // Give second chance
            pagesSkippedByHot.incrementAndGet();
            return page; // Keep in cache
          }
          
          // ATOMIC: Check guard count (PRIMARY protection)
          if (page.getGuardCount() > 0) {
            if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS && LOGGER.isDebugEnabled()) {
              LOGGER.debug("ClockSweeper[{}] skip guarded page key={} type={} rev={} guards={}",
                  shardIndex, page.getPageKey(), page.getIndexType(), page.getRevision(), page.getGuardCount());
            }
            pagesSkippedByGuard.incrementAndGet();
            return page; // Keep in cache
          }
          
          // Check revision watermark
          if (!isGlobalSweeper && page.getRevision() >= minActiveRev) {
            pagesSkippedByWatermark.incrementAndGet();
            return page; // Keep in cache
          }
          
          // Evict page atomically within compute() while holding per-key lock
          try {
            long pageWeight = cache.weightOf(page);

            page.incrementVersion();
            ref.setPage(null);
            page.close();

            // Verify page was actually closed (guards might have been acquired after our check)
            if (!page.isClosed()) {
              pagesSkippedByGuard.incrementAndGet();
              return page; // Keep in cache - another thread is using it
            }

            cache.onEvicted(page, pageWeight);
            pagesEvicted.incrementAndGet();
            return null; // Successfully evicted
          } catch (Exception e) {
            LOGGER.error("Failed to evict page {}: {}", page.getPageKey(), e.getMessage());
            return page; // Keep in cache on error
          }
        });

        // Move clock hand forward (will be bounded on next iteration)
        shard.clockHand++;
      }
    } finally {
      shard.evictionLock.unlock();
    }
  }

  /**
   * Get eviction statistics.
   *
   * @return diagnostic string
   */
  public String getStatistics() {
    return String.format("ClockSweeper[db=%d,res=%d,shard=%d]: evicted=%d, skipped(hot=%d, watermark=%d, guard=%d, ownership=%d)",
        databaseId, resourceId, shardIndex, pagesEvicted.get(), pagesSkippedByHot.get(),
        pagesSkippedByWatermark.get(), pagesSkippedByGuard.get(), pagesSkippedByOwnership.get());
  }
}

