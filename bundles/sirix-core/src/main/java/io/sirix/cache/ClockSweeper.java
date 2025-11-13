package io.sirix.cache;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background clock sweeper for page eviction (second-chance algorithm).
 * <p>
 * Implements LeanStore/Umbra-style eviction:
 * - Scans pages in clock order
 * - Gives HOT pages a second chance (clears HOT bit)
 * - Evicts COLD pages if revision watermark allows (revision < minActiveRevision)
 * - Respects guard count (guardCount == 0)
 * <p>
 * Runs as a background thread per shard for multi-core scalability.
 *
 * @author Johannes Lichtenberger
 */
public final class ClockSweeper implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClockSweeper.class);

  private final ShardedPageCache.Shard shard;
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
  public ClockSweeper(ShardedPageCache.Shard shard, RevisionEpochTracker epochTracker,
                      int sweepIntervalMs, int shardIndex, long databaseId, long resourceId) {
    this.shard = shard;
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
        PageReference ref = keys.get(shard.clockHand);
        
        // Filter by resource if not global (databaseId=0 and resourceId=0 means GLOBAL)
        boolean isGlobalSweeper = (databaseId == 0 && resourceId == 0);
        if (!isGlobalSweeper && (ref.getDatabaseId() != databaseId || ref.getResourceId() != resourceId)) {
          shard.clockHand = (shard.clockHand + 1) % Math.max(1, keys.size());
          pagesSkippedByOwnership.incrementAndGet();
          continue;
        }
        
        KeyValueLeafPage page = shard.map.get(ref);

        if (page == null) {
          // Page was removed, move clock hand
          shard.clockHand = (shard.clockHand + 1) % Math.max(1, keys.size());
          continue;
        }

        // Check if page is HOT
        if (page.isHot()) {
          page.clearHot(); // Give second chance
          pagesSkippedByHot.incrementAndGet();
        } else if (page.getGuardCount() > 0) {
          // Guard is active on the page (PRIMARY protection mechanism)
          pagesSkippedByGuard.incrementAndGet();
        } else if (!isGlobalSweeper && page.getRevision() >= minActiveRev) {
          // For per-resource sweepers: Check revision watermark
          // For global sweepers: Skip this check (rely on guards for correctness)
          pagesSkippedByWatermark.incrementAndGet();
        } else {
          // Evict: increment version, reset page, remove from map
          try {
            page.incrementVersion();
            page.reset(); // Clear data, keep MemorySegments
            
            // Optionally release physical memory via madvise
            // (allocator.reset() would do this)
            
            shard.map.remove(ref);
            ref.setPage(null);
            pagesEvicted.incrementAndGet();
            
            if (KeyValueLeafPage.DEBUG_MEMORY_LEAKS && pagesEvicted.get() % 100 == 0) {
              LOGGER.debug("ClockSweeper[{}] evicted {} pages (skipped: hot={}, watermark={}, guard={})",
                  shardIndex, pagesEvicted.get(), pagesSkippedByHot.get(),
                  pagesSkippedByWatermark.get(), pagesSkippedByGuard.get());
            }
          } catch (Exception e) {
            LOGGER.error("ClockSweeper[{}] failed to evict page {}", shardIndex, page.getPageKey(), e);
          }
        }

        // Move clock hand
        shard.clockHand = (shard.clockHand + 1) % Math.max(1, keys.size());
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

