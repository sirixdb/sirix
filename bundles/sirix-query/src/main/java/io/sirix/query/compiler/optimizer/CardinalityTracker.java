package io.sirix.query.compiler.optimizer;

import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks cardinality estimation accuracy and triggers plan invalidation
 * when estimates diverge significantly from actual execution results.
 *
 * <p>Implements dampened adaptive re-optimization: a single outlier execution
 * won't invalidate a good plan. Only after {@link #DRIFT_CONSECUTIVE_THRESHOLD}
 * consecutive executions with a {@link #DRIFT_RATIO_THRESHOLD}x or greater
 * mismatch will the plan be evicted from cache.</p>
 *
 * <p>Thread-safe singleton. Designed for negligible overhead per query —
 * one ConcurrentHashMap lookup + one atomic increment per execution.</p>
 */
public final class CardinalityTracker {

  private static final Logger LOG = LoggerFactory.getLogger(CardinalityTracker.class);

  /** Ratio threshold: actual/estimated or estimated/actual must exceed this. */
  static final double DRIFT_RATIO_THRESHOLD = 10.0;

  /** Number of consecutive mismatched executions before plan invalidation. */
  static final int DRIFT_CONSECUTIVE_THRESHOLD = 3;

  /** Maximum tracked entries to prevent unbounded growth. */
  private static final int MAX_TRACKED_KEYS = 1024;

  private static final CardinalityTracker INSTANCE = new CardinalityTracker();

  private final ConcurrentHashMap<String, DriftState> driftStates = new ConcurrentHashMap<>();

  private CardinalityTracker() {}

  public static CardinalityTracker getInstance() {
    return INSTANCE;
  }

  /**
   * Record an execution and check for cardinality drift.
   *
   * <p>If the estimated and actual cardinalities diverge by more than
   * {@link #DRIFT_RATIO_THRESHOLD}x for {@link #DRIFT_CONSECUTIVE_THRESHOLD}
   * consecutive executions, the cached plan is invalidated.</p>
   *
   * @param cacheKey      the plan cache key for this query
   * @param estimatedCard the optimizer's estimated cardinality (-1 if unknown)
   * @param actualCard    the actual result count from execution
   * @param planCache     the plan cache to invalidate on drift
   */
  public void record(String cacheKey, long estimatedCard, long actualCard, PlanCache planCache) {
    if (cacheKey == null || estimatedCard <= 0 || actualCard < 0) {
      return; // nothing to compare
    }

    final double ratio = estimatedCard > actualCard
        ? (double) estimatedCard / Math.max(1, actualCard)
        : (double) actualCard / Math.max(1, estimatedCard);

    if (ratio >= DRIFT_RATIO_THRESHOLD) {
      // Mismatch detected — increment consecutive count
      final DriftState state = driftStates.computeIfAbsent(cacheKey, k -> {
        // Evict oldest if at capacity (simple size check)
        if (driftStates.size() >= MAX_TRACKED_KEYS) {
          // Remove arbitrary entry to make room — ConcurrentHashMap iteration is safe
          final var iter = driftStates.keySet().iterator();
          if (iter.hasNext()) {
            iter.next();
            iter.remove();
          }
        }
        return new DriftState();
      });

      final int consecutive = state.consecutiveMismatches.incrementAndGet();

      if (consecutive >= DRIFT_CONSECUTIVE_THRESHOLD) {
        LOG.debug("Cardinality drift detected for query (estimated={}, actual={}, ratio={:.1f}x, " +
                "consecutive={}). Invalidating cached plan.",
            estimatedCard, actualCard, ratio, consecutive);
        planCache.invalidate(cacheKey);
        driftStates.remove(cacheKey);
      }
    } else {
      // Estimate was accurate — reset drift counter
      driftStates.remove(cacheKey);
    }
  }

  /**
   * Clear all tracked drift state (e.g., for testing).
   */
  public void clear() {
    driftStates.clear();
  }

  /**
   * @return number of currently tracked query keys
   */
  public int trackedKeyCount() {
    return driftStates.size();
  }

  private static final class DriftState {
    final AtomicInteger consecutiveMismatches = new AtomicInteger(0);
  }
}
