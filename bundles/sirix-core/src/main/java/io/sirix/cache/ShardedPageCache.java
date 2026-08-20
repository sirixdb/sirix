package io.sirix.cache;

import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Simple page cache with direct eviction control.
 * <p>
 * Uses a single ConcurrentHashMap with clock-based eviction. Simplified from multi-shard design for
 * easier debugging and maintenance.
 * <p>
 * Provides: - Direct control over eviction (revision watermark + guardCount checks) - Clock-based
 * second-chance eviction algorithm - ConcurrentHashMap's built-in lock-free read optimization
 * <p>
 * Inspired by LeanStore/Umbra buffer management architectures.
 * <p>
 * <b>Locking Strategy:</b> - Per-key atomicity via ConcurrentHashMap.compute() - evictionLock
 * prevents concurrent ClockSweeper sweeps and clear() - No global lock - optimized for
 * high-concurrency workloads
 * <p>
 * <b>Note on clear() race:</b> There is a benign race between clear() and concurrent operations.
 * Since clear() is typically called only at shutdown and uses evictionLock for coordination with
 * ClockSweeper, this race is acceptable. Pages use volatile fields and synchronized close() for
 * safety.
 *
 * @author Johannes Lichtenberger
 */
public final class ShardedPageCache<V extends CacheablePage> implements Cache<PageReference, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardedPageCache.class);

  static final boolean DEBUG_MEMORY_LEAKS =
      Boolean.getBoolean("sirix.debug.memoryLeaks");

  private final ConcurrentHashMap<PageReference, V> map = new ConcurrentHashMap<>();
  private final ReentrantLock evictionLock = new ReentrantLock();
  private final Shard<V> shard;
  /** Preallocated callback + state guarded by evictionLock for allocation-free exact removal. */
  private final BiFunction<PageReference, V, V> conditionalRemovalFunction;
  private CacheablePage conditionalRemovalExpected;
  private boolean conditionalRemovalSucceeded;
  private final long maxWeightBytes;
  private final AtomicLong currentWeightBytes = new AtomicLong(0L);

  /**
   * Weight actually CHARGED per cached entry. Removal/eviction must subtract exactly what
   * insertion added: a page's native size can become 0 once it is closed, and a HOT leaf's
   * composite weight can change before publication, so symmetric weightOf-based accounting drifted
   * upward until the cache was permanently pinned in the severe-eviction branch.
   */
  private final ConcurrentHashMap<PageReference, CacheCharge> insertedWeights = new ConcurrentHashMap<>();

  /** Identity-stamped charge so a failed admission can never roll back a racing successor's weight. */
  private static final class CacheCharge {
    final long weight;

    CacheCharge(long weight) {
      this.weight = weight;
    }
  }

  /** Slow-miss publication state; the cache-hit path allocates nothing. */
  private static final class GuardedLoadState<T extends CacheablePage> {
    T candidate;
    T displaced;
    CacheCharge appliedCharge;
    CacheCharge previousCharge;
    boolean chargeAttempted;
    boolean weightCounterAdjusted;
    boolean candidateGuarded;
  }

  /** Publish an already-validated weight without re-reading mutable page state. */
  private void chargeWeight(PageReference key, long weight) {
    final CacheCharge charge = weight > 0 ? new CacheCharge(weight) : null;
    final CacheCharge previous = replaceCharge(key, charge);
    final long delta = weight - chargedWeight(previous);
    if (delta != 0) {
      adjustCurrentWeight(delta);
    }
  }

  /** Install one pre-created charge token, returning the exact token it replaced. */
  private CacheCharge replaceCharge(PageReference key, CacheCharge charge) {
    return charge != null ? insertedWeights.put(key, charge) : insertedWeights.remove(key);
  }

  private static long chargedWeight(CacheCharge charge) {
    return charge != null ? charge.weight : 0L;
  }

  /** Stage an identity-addressable charge so a failed CHM publication can restore its predecessor. */
  private void chargeGuardedCandidate(PageReference key, long weight, GuardedLoadState<?> state) {
    state.appliedCharge = weight > 0 ? new CacheCharge(weight) : null;
    state.previousCharge = insertedWeights.get(key);
    state.chargeAttempted = true;
    final CacheCharge actualPrevious = replaceCharge(key, state.appliedCharge);
    state.previousCharge = actualPrevious;
    final long delta = weight - chargedWeight(actualPrevious);
    if (delta != 0) {
      adjustCurrentWeight(delta);
    }
    state.weightCounterAdjusted = true;
  }

  /** Roll back only this admission's token; an intervening successor charge wins by identity. */
  private void rollbackGuardedCandidateCharge(PageReference key, GuardedLoadState<?> state) {
    if (!state.chargeAttempted) {
      return;
    }

    final CacheCharge applied = state.appliedCharge;
    final CacheCharge previous = state.previousCharge;
    final boolean restored;
    if (applied != null) {
      restored = previous != null
          ? insertedWeights.replace(key, applied, previous)
          : insertedWeights.remove(key, applied);
    } else if (previous != null) {
      restored = insertedWeights.putIfAbsent(key, previous) == null;
    } else {
      restored = true;
    }
    if (restored && state.weightCounterAdjusted) {
      final long delta = chargedWeight(previous) - chargedWeight(applied);
      if (delta != 0) {
        adjustCurrentWeight(delta);
      }
    }
  }

  /** Subtract exactly the charge recorded at insertion (safe for closed/grown pages). */
  private void unchargeWeight(PageReference key) {
    try {
      final CacheCharge previous = insertedWeights.remove(key);
      if (previous != null && previous.weight > 0) {
        adjustCurrentWeight(-previous.weight);
      }
    } catch (RuntimeException | Error accountingFailure) {
      // Detachment callbacks must still return null: letting an Error escape ConcurrentHashMap.compute
      // after lifecycle mutation retains a dead mapping. The cache is already in a fatal accounting
      // state, so report it without resurrecting ownership of the page.
      LOGGER.error("Failed to uncharge detached cache key {}", key, accountingFailure);
    }
  }

  /** Clear only this page's swizzle; never erase a replacement installed on the same reference. */
  private static void clearSwizzleIfSame(PageReference reference, CacheablePage page) {
    try {
      if (page instanceof Page swizzledPage) {
        reference.clearPageIfSame(swizzledPage);
      }
    } catch (RuntimeException | Error swizzleFailure) {
      LOGGER.error("Failed to clear detached page {} from its cache reference", page.getPageKey(), swizzleFailure);
    }
  }

  /**
   * Retire a page whose cache ownership has ended. A live guard becomes the sole lifecycle owner and
   * performs the deferred physical close on its final release.
   */
  private static void retireDetachedPage(CacheablePage page) {
    try {
      if (!page.isClosed()) {
        page.retire();
      }
    } catch (RuntimeException | Error retirementFailure) {
      // Ownership has already ended. Keeping a half-retired page cache-visible would be worse than
      // reporting the failed physical release and letting the page's own lifecycle finish it.
      LOGGER.error("Failed to retire detached page {}: {}", page.getPageKey(), retirementFailure.getMessage(),
          retirementFailure);
    }
  }

  /**
   * Remove the cache's ownership and recorded charge after an eviction selected {@code page} under
   * {@code reference}'s map-compute lock.
   *
   * <p>A guard can arrive after the caller's guard-count check. {@link CacheablePage#retire()} then
   * marks the page orphaned and defers its physical close to that holder; the mapping and charge must
   * still disappear now. The page-local version is bumped only when no holder deferred the close.</p>
   */
  private void retireAndUnchargeEvictedPage(PageReference reference, CacheablePage page) {
    try {
      retireDetachedPage(page);
      if (page.isClosed()) {
        page.incrementVersion();
      }
    } catch (RuntimeException | Error versionFailure) {
      LOGGER.error("Failed to finalize detached page {}: {}", page.getPageKey(), versionFailure.getMessage(),
          versionFailure);
    } finally {
      try {
        clearSwizzleIfSame(reference, page);
      } finally {
        unchargeWeight(reference);
      }
    }
  }

  /**
   * Remove and uncharge {@code expected} only if it is still the exact mapping at {@code key}.
   * Closed-page cleanup is rare and takes the eviction lock; the common removePage path already
   * owns it and calls {@link #removeMappingIfSameWhileLocked(PageReference, CacheablePage)} directly.
   */
  private boolean removeMappingIfSame(PageReference key, CacheablePage expected) {
    evictionLock.lock();
    try {
      return removeMappingIfSameWhileLocked(key, expected);
    } finally {
      evictionLock.unlock();
    }
  }

  /**
   * Exact conditional removal without a per-call holder or capturing lambda allocation.
   * The callback and its mutable success state are cache-owned and serialized by evictionLock.
   */
  private boolean removeMappingIfSameWhileLocked(PageReference key, CacheablePage expected) {
    if (!evictionLock.isHeldByCurrentThread()) {
      throw new IllegalStateException("Exact cache removal requires the eviction lock");
    }
    if (conditionalRemovalExpected != null) {
      throw new IllegalStateException("Nested exact cache removal is not supported");
    }

    conditionalRemovalExpected = expected;
    conditionalRemovalSucceeded = false;
    try {
      map.compute(key, conditionalRemovalFunction);
      return conditionalRemovalSucceeded;
    } finally {
      conditionalRemovalExpected = null;
      conditionalRemovalSucceeded = false;
    }
  }

  /** Preallocated {@link ConcurrentHashMap#compute} callback; state is guarded by evictionLock. */
  private V removeExpectedMapping(PageReference reference, V current) {
    if (current != conditionalRemovalExpected) {
      return current;
    }
    clearSwizzleIfSame(reference, current);
    unchargeWeight(reference);
    conditionalRemovalSucceeded = true;
    return null;
  }

  /** Apply an accounting delta without allowing the published byte count to wrap. */
  private void adjustCurrentWeight(long delta) {
    long current;
    long adjusted;
    do {
      current = currentWeightBytes.get();
      if (delta > 0) {
        adjusted = saturatedAdd(current, delta);
      } else {
        // Both operands originate from non-negative recorded weights, so this subtraction cannot
        // overflow a long. Clamp defensively if a racing lifecycle path already removed a charge.
        adjusted = Math.max(0L, current + delta);
      }
    } while (!currentWeightBytes.compareAndSet(current, adjusted));
  }

  // ===== CACHE HIT/MISS INSTRUMENTATION =====
  // Use LongAdder for high-contention counters (better scalability than AtomicLong)
  private static final LongAdder CACHE_HITS = new LongAdder();
  private static final LongAdder CACHE_MISSES = new LongAdder();
  private static final LongAdder CACHE_EVICTIONS = new LongAdder();

  /** Get cache hit count for diagnostics */
  public static long getCacheHits() {
    return CACHE_HITS.sum();
  }

  /** Get cache miss count for diagnostics */
  public static long getCacheMisses() {
    return CACHE_MISSES.sum();
  }

  /** Get cache eviction count for diagnostics */
  public static long getCacheEvictions() {
    return CACHE_EVICTIONS.sum();
  }

  /** Reset cache counters */
  public static void resetCacheCounters() {
    CACHE_HITS.reset();
    CACHE_MISSES.reset();
    CACHE_EVICTIONS.reset();
  }
  // ===== END INSTRUMENTATION =====

  /**
   * Create a new page cache.
   *
   */
  public ShardedPageCache(long maxWeightBytes) {
    this.shard = new Shard(map, evictionLock);
    this.conditionalRemovalFunction = this::removeExpectedMapping;
    this.maxWeightBytes = maxWeightBytes;
    LOGGER.info("Created ShardedPageCache (simplified single-map design) with maxWeight={} bytes", maxWeightBytes);
  }

  /**
   * Get the single shard (for ClockSweeper compatibility).
   */
  public Shard<V> getShard(PageReference ref) {
    return shard;
  }

  /**
   * Current tracked weight of cached pages in bytes. Exposed for the metrics SPI
   * ({@code SirixMetricsRegistry}) to publish as a Prometheus gauge.
   */
  public long getCurrentWeightBytes() {
    return currentWeightBytes.get();
  }

  /** Maximum weight (bytes) this cache will hold before eviction. */
  public long getMaxWeightBytes() {
    return maxWeightBytes;
  }

  /**
   * Callback for eviction: adjust the tracked weight and bump the eviction counter.
   */
  void onEvicted(PageReference ref, CacheablePage page) {
    retireAndUnchargeEvictedPage(ref, page);
    CACHE_EVICTIONS.increment();
  }

  /**
   * Compute the weight (bytes) of a cached page.
   *
   * <p>HOT leaves add their allocation-free retained-heap estimate to the separately reported native
   * frame. Re-putting a page samples its current estimate and replaces the recorded charge.</p>
   */
  long weightOf(CacheablePage page) {
    if (page == null) {
      return 0L;
    }

    final long nativeBytes = page.getActualMemorySize();
    if (!(page instanceof HOTLeafPage hotLeafPage)) {
      return nativeBytes;
    }

    return saturatedAdd(nativeBytes, hotLeafPage.estimatedCanonicalCacheRetainedHeapBytes());
  }

  private static long saturatedAdd(long left, long right) {
    try {
      return Math.addExact(left, right);
    } catch (ArithmeticException ignored) {
      return Long.MAX_VALUE;
    }
  }

  /** Record a cleanup failure without allowing self-suppression to replace the primary failure. */
  private static void addSuppressedSafely(Throwable primary, Throwable secondary) {
    if (primary == secondary) {
      return;
    }
    try {
      primary.addSuppressed(secondary);
    } catch (RuntimeException | Error ignored) {
      // Preserve the failure that triggered cleanup even if suppression itself is unavailable.
    }
  }

  /**
   * Shard wrapper for ClockSweeper compatibility. Note: clockHand should only be accessed while
   * holding evictionLock.
   */
  public static final class Shard<V extends CacheablePage> {
    final ConcurrentHashMap<PageReference, V> map;
    final ReentrantLock evictionLock;
    int clockHand; // Only access while holding evictionLock

    Shard(ConcurrentHashMap<PageReference, V> map, ReentrantLock lock) {
      this.map = map;
      this.evictionLock = lock;
      this.clockHand = 0;
    }
  }

  @Override
  public V get(PageReference key) {
    V page = map.get(key);
    if (page != null) {
      if (page.isClosed()) {
        removeMappingIfSame(key, page);
        return null;
      }
      page.markAccessed();
    }
    return page;
  }

  @Override
  public V get(PageReference key,
      BiFunction<? super PageReference, ? super V, ? extends V> mappingFunction) {
    V existing = map.get(key);
    if (existing != null && !existing.isClosed()) {
      existing.markAccessed();
      return existing;
    }

    V page = map.compute(key, (k, existingValue) -> {
      if (existingValue != null && !existingValue.isClosed()) {
        existingValue.markAccessed();
        return existingValue;
      }
      V newPage = mappingFunction.apply(k, existingValue);
      if (newPage != null && !newPage.isClosed()) {
        // Validate and sample the candidate before mutating the old mapping. If HOT cache-shape
        // validation fails, ConcurrentHashMap keeps the existing value and its exact charge intact.
        final long newWeight = weightOf(newPage);
        newPage.markAccessed();
        newPage.setLastCacheKey(k);
        if (DEBUG_MEMORY_LEAKS && newPage.getPageKey() == 0) {
          LOGGER.debug("[CACHE-COMPUTE] Page 0 computed and caching: {} rev={} instance={} guardCount={}",
              newPage.getIndexType(), newPage.getRevision(), System.identityHashCode(newPage), newPage.getGuardCount());
        }
        // Replace the recorded charge before lifecycle mutation. After validation succeeds, the
        // remaining detach operations are fail-closed and cannot make compute retain a dead value.
        chargeWeight(k, newWeight);
        if (existingValue != null) {
          clearSwizzleIfSame(k, existingValue);
        }
        return newPage;
      }
      if (existingValue != null) {
        clearSwizzleIfSame(k, existingValue);
        unchargeWeight(k);
      }
      return null;
    });

    evictIfOverBudget();

    if (DEBUG_MEMORY_LEAKS && page != null && page.getPageKey() == 0) {
      V cached = map.get(key);
      boolean inCache = (cached == page);
      LOGGER.debug("[CACHE-VERIFY] Page 0 after compute: {} rev={} instance={} inCache={} cachedInstance={}",
          page.getIndexType(), page.getRevision(), System.identityHashCode(page), inCache, cached != null
              ? System.identityHashCode(cached)
              : "null");
    }

    return page;
  }

  @Override
  public V getAndGuard(PageReference key) {
    V existing = map.get(key);
    if (existing != null && existing.acquireGuard()) {
      existing.markAccessed();
      return existing;
    }

    return map.compute(key, (k, existingValue) -> {
      // acquireGuard() can fail even after an isClosed() check: a non-compute closer
      // (truncate sweep, TIL teardown) may close the page concurrently. NEVER hand out a
      // page whose guard was not actually acquired — the caller would use it unprotected.
      if (existingValue != null && !existingValue.isClosed() && existingValue.acquireGuard()) {
        existingValue.markAccessed();
        return existingValue;
      }
      if (existingValue != null) {
        retireDetachedPage(existingValue);
        clearSwizzleIfSame(k, existingValue);
        unchargeWeight(k);
      }
      return null;
    });
  }

  @Override
  public V getOrLoadAndGuard(PageReference key, Function<PageReference, V> loader) {
    V existing = map.get(key);
    if (existing != null && existing.acquireGuard()) {
      existing.markAccessed();
      CACHE_HITS.increment();
      return existing;
    }

    final GuardedLoadState<V> loadState = new GuardedLoadState<>();
    final V page;
    try {
      page = map.compute(key, (k, existingInCompute) -> {
        // acquireGuard() can fail even after an isClosed() check: a non-compute closer
        // (truncate sweep, TIL teardown) may close the page concurrently. A failed acquire
        // means the mapping is dead — replace it with a fresh load instead of handing out a
        // page the caller would use unguarded.
        if (existingInCompute != null && !existingInCompute.isClosed() && existingInCompute.acquireGuard()) {
          CACHE_HITS.increment();
          existingInCompute.markAccessed();
          return existingInCompute;
        }
        CACHE_MISSES.increment();
        V loaded = loader.apply(k);
        loadState.candidate = loaded;
        loadState.displaced = existingInCompute;
        long loadedWeight = 0L;
        if (loaded != null && !loaded.isClosed()) {
          // Admission must fail before the existing mapping is retired or uncharged.
          loadedWeight = weightOf(loaded);
          loaded.markAccessed();
          if (!loaded.acquireGuard()) {
            // Freshly loaded page closed before we could guard it (cannot normally happen —
            // the instance is still private). Free it and treat as a miss.
            retireDetachedPage(loaded);
            loaded = null;
          }
        }

        if (loaded == null || loaded.isClosed()) {
          if (existingInCompute != null) {
            retireDetachedPage(existingInCompute);
            clearSwizzleIfSame(k, existingInCompute);
            unchargeWeight(k);
          }
          return null;
        }

        // From this point until compute publishes its return, the candidate is still private and
        // this method owns its one guard. Export just enough identity-stamped state for an outer
        // CHM failure to restore the displaced mapping's charge without touching a racing successor.
        loadState.candidateGuarded = true;
        loaded.setLastCacheKey(k);
        chargeGuardedCandidate(k, loadedWeight, loadState);
        return loaded;
      });
    } catch (final RuntimeException | Error publicationFailure) {
      cleanupFailedGuardedLoad(key, loadState, publicationFailure);
      throw publicationFailure;
    }

    if (loadState.candidate != null && loadState.candidate == page) {
      try {
        finalizeDisplacedGuardedLoad(key, loadState);
      } catch (final RuntimeException | Error finalizationFailure) {
        try {
          page.releaseGuard();
        } catch (final RuntimeException | Error guardReleaseFailure) {
          addSuppressedSafely(finalizationFailure, guardReleaseFailure);
        }
        throw finalizationFailure;
      }
    }
    try {
      evictIfOverBudget();
      return page;
    } catch (final RuntimeException | Error enforcementFailure) {
      if (page != null) {
        try {
          page.releaseGuard();
        } catch (final RuntimeException | Error guardReleaseFailure) {
          addSuppressedSafely(enforcementFailure, guardReleaseFailure);
        }
      }
      throw enforcementFailure;
    }
  }

  /** Complete deferred old-page ownership transfer only after CHM published the guarded candidate. */
  private void finalizeDisplacedGuardedLoad(PageReference key, GuardedLoadState<V> state) {
    final V displaced = state.displaced;
    if (displaced != null && displaced != state.candidate) {
      clearSwizzleIfSame(key, displaced);
      retireDetachedPage(displaced);
    }
  }

  /** Preserve the exact publication failure while restoring charge and private candidate ownership. */
  private void cleanupFailedGuardedLoad(PageReference key, GuardedLoadState<V> state, Throwable primaryFailure) {
    final V candidate = state.candidate;
    if (candidate == null) {
      return;
    }

    // Normally a throwing compute leaves its old mapping untouched. Keep the identity check because
    // a cache/JDK implementation that published before surfacing a failure has already transferred
    // candidate ownership; in that case its charge stays and the displaced page still needs cleanup.
    final boolean candidatePublished = map.get(key) == candidate;
    if (candidatePublished) {
      try {
        finalizeDisplacedGuardedLoad(key, state);
      } catch (final RuntimeException | Error finalizationFailure) {
        addSuppressedSafely(primaryFailure, finalizationFailure);
      }
    } else {
      try {
        rollbackGuardedCandidateCharge(key, state);
      } catch (final RuntimeException | Error rollbackFailure) {
        addSuppressedSafely(primaryFailure, rollbackFailure);
      }
      try {
        candidate.retire();
      } catch (final RuntimeException | Error retirementFailure) {
        addSuppressedSafely(primaryFailure, retirementFailure);
      }
    }

    if (state.candidateGuarded) {
      try {
        candidate.releaseGuard();
      } catch (final RuntimeException | Error guardReleaseFailure) {
        addSuppressedSafely(primaryFailure, guardReleaseFailure);
      } finally {
        state.candidateGuarded = false;
      }
    }
  }

  @Override
  public void put(PageReference key, V value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }

    // Validate before entering compute: a rejected candidate must not retire the existing winner.
    final long valueWeight = weightOf(value);
    value.markAccessed();
    value.setLastCacheKey(key);
    map.compute(key, (k, existing) -> {
      chargeWeight(k, valueWeight);
      if (existing != null && existing != value) {
        // Returning value transfers this key's cache ownership from existing to value. Any reader
        // that already guarded existing owns its deferred lifetime; without a guard there is no
        // remaining owner, so retire closes it immediately. This applies equally to record and HOT
        // leaves: a transaction that wants to retain a page must first removePage() and take
        // ownership, rather than leave one instance simultaneously owned by the TIL and cache.
        clearSwizzleIfSame(k, existing);
        retireDetachedPage(existing);
      }
      return value;
    });

    evictIfOverBudget();
  }

  @Override
  public void putIfAbsent(PageReference key, V value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }
    value.markAccessed();
    map.compute(key, (k, existing) -> {
      if (existing != null) {
        if (DEBUG_MEMORY_LEAKS && value.getPageKey() == 0) {
          LOGGER.debug("[CACHE-SKIP] Page 0 NOT added (already exists): {} rev={} newInstance={} existingInstance={}",
              value.getIndexType(), value.getRevision(), System.identityHashCode(value),
              System.identityHashCode(existing));
        }
        return existing;
      }

      // Compute the potentially-throwing HOT admission weight before publishing any mapping or
      // accounting mutation. An existing winner bypasses this because value is never admitted.
      final long valueWeight = weightOf(value);
      value.setLastCacheKey(k);
      if (DEBUG_MEMORY_LEAKS && value.getPageKey() == 0) {
        LOGGER.debug("[CACHE-ADD] Page 0 added to cache: {} rev={} instance={} guardCount={}", value.getIndexType(),
            value.getRevision(), System.identityHashCode(value), value.getGuardCount());
      }
      chargeWeight(k, valueWeight);
      return value;
    });
    evictIfOverBudget();
  }

  @Override
  public void clear() {
    evictionLock.lock();
    try {
      java.util.List<V> snapshot = new java.util.ArrayList<>(map.values());

      for (V page : snapshot) {
        if (page.isClosed()) {
          continue;
        }
        // Global teardown path (clearAllCaches at shutdown / test reset). Orphan rather than
        // drain: draining forges the "I'm done" signal for whoever actually holds the guard, and
        // clearAllCaches is callable while transactions are live. The orphan bit reclaims the frame
        // at the holder's last release instead of under it, and reclaims immediately when the count
        // is already zero — which is every page here once nothing leaks guards.
        page.retire();
      }

      for (final PageReference key : map.keySet()) {
        key.setPage(null);
      }
      map.clear();
      insertedWeights.clear();
      currentWeightBytes.set(0L);
      shard.clockHand = 0;
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public V removeAndGet(PageReference key) {
    // One compute: whatever is mapped when the entry goes is exactly what the caller is handed, so
    // a page cached by another thread between a get and a remove cannot slip out unowned.
    final V[] removed = (V[]) new CacheablePage[1];
    map.compute(key, (k, page) -> {
      if (page != null) {
        removed[0] = page;
        unchargeWeight(k);
        clearSwizzleIfSame(k, page);
      }
      return null;
    });
    return removed[0];
  }

  @Override
  public void remove(PageReference key) {
    map.compute(key, (k, page) -> {
      if (page != null) {
        unchargeWeight(k);
        clearSwizzleIfSame(k, page);
      }
      return null;
    });
  }

  /**
   * Remove a page from the cache by reference identity, without closing it.
   *
   * <p>Unlike {@link #remove(PageReference)} this matches on instance identity (HOT leaf and
   * record pages do not override {@code equals}) and does <em>not</em> release the page's off-heap
   * memory — the caller keeps ownership. Used so the transaction-intent log can take a dirty,
   * transaction-private page out of the shared cache, preventing the sweeper and pressure
   * eviction from reclaiming its slot before commit serializes it.</p>
   */
  @Override
  public void removePage(V page) {
    if (page == null || map.isEmpty()) {
      return;
    }
    evictionLock.lock();
    try {
      // Fast path: the page remembers the reference it was last cached under. Verified by IDENTITY
      // before acting, so a stale remembered key simply falls through to the scan below.
      final PageReference remembered = page.lastCacheKey();
      if (remembered != null && removeMappingIfSameWhileLocked(remembered, page)) {
        return;
      }
      for (final var entry : map.entrySet()) {
        if (entry.getValue() == page && removeMappingIfSameWhileLocked(entry.getKey(), page)) {
          return;
        }
      }
    } finally {
      evictionLock.unlock();
    }
  }

  /**
   * Whether this cache currently holds the given page INSTANCE.
   *
   * <p>The inherited default is {@code asMap().containsValue(page)} — a linear scan of the whole
   * cache. That runs per orphaned page when a write transaction tears down its page containers, so
   * a large cache turned teardown into O(entries x containers).
   *
   * <p>A page remembers the reference it was last cached under, which answers the question in O(1)
   * whenever it is still cached there — the overwhelmingly common case, since a container's HOT
   * leaf usually IS the cache's instance. Only a positive identity match is conclusive: a page
   * re-cached under a NEW reference (copy-on-write moves the root reference's key) remembers just
   * the latest one, so a mismatch has to fall back to the exact scan. Getting that backwards would
   * free a page the cache is still handing to readers.
   */
  @Override
  public boolean containsPage(V page) {
    if (page == null || map.isEmpty()) {
      return false;
    }
    final PageReference remembered = page.lastCacheKey();
    if (remembered != null && map.get(remembered) == page) {
      return true;
    }
    return map.containsValue(page);
  }

  @Override
  public java.util.Map<PageReference, V> getAll(Iterable<? extends PageReference> keys) {
    java.util.Map<PageReference, V> result = new java.util.HashMap<>();
    for (PageReference key : keys) {
      V page = get(key);
      if (page != null) {
        result.put(key, page);
      }
    }
    return result;
  }

  @Override
  public void putAll(java.util.Map<? extends PageReference, ? extends V> map) {
    for (java.util.Map.Entry<? extends PageReference, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void toSecondCache() {
    // No-op: we don't have a two-level cache structure
  }

  @Override
  public ConcurrentMap<PageReference, V> asMap() {
    return map;
  }

  @Override
  public void close() {
    clear();
  }


  /**
   * Get the number of shards (always 1 in simplified design).
   *
   * @return shard count (1)
   */
  public int getShardCount() {
    return 1;
  }

  /**
   * Get total number of cached pages.
   *
   * @return total page count
   */
  public long size() {
    return map.size();
  }

  /**
   * Evict cold, unguarded pages until the cache is within the configured memory budget.
   *
   * <h2>Two modes of pressure</h2>
   *
   * <ul>
   *   <li><b>Normal over-budget (&lt; 110% of limit):</b> non-blocking {@code tryLock},
   *       two-pass HOT-bit algorithm. Lets concurrent writers through and delegates
   *       to the background {@link ClockSweeper}.</li>
   *   <li><b>Severe over-budget (&ge; 110% of limit):</b> blocking {@code lock},
   *       single-pass eviction — HOT bit is ignored since at high concurrency the
   *       two-pass approach never catches up (bit gets re-set between sweeps).</li>
   * </ul>
   *
   * <p>Why the split: at 20 parallel readers, tryLock succeeds only intermittently
   * and the two-pass HOT logic leaves every page permanently hot during a scan.
   * That starves the allocator budget and surfaces as {@code OutOfMemoryError} in
   * {@code MemorySegmentAllocator.allocate} 10+ seconds later. Forcing a blocking
   * one-pass eviction when we're significantly over budget makes eviction keep up
   * with allocation pressure.
   */
  private void evictIfOverBudget() {
    if (maxWeightBytes <= 0) {
      return;
    }

    final long currentWeight = currentWeightBytes.get();
    if (currentWeight <= maxWeightBytes) {
      return;
    }

    // Pressure level: normal = two-pass HOT-bit; severe = blocking one-pass.
    // 10% over-budget threshold matches the retry budget we allow the allocator
    // before it bubbles OOM — eviction must finish before that fires.
    final boolean severe = currentWeight > (maxWeightBytes + maxWeightBytes / 10);

    // Fast path: if we're not severely over budget, skip synchronous eviction
    // entirely on the put-path and let the background {@link ClockSweeper}
    // handle the trickle. Walking the full CHM entry set on every put was
    // ~9% of cold-cache on-CPU time at 100M scale (profiled). The sweeper
    // runs periodically, so a transient few-percent overshoot is fine —
    // the allocator only OOMs when >10% over, which is the severe branch.
    if (!severe) {
      return;
    }

    if (severe) {
      evictionLock.lock();
    } else if (!evictionLock.tryLock()) {
      return; // Let the ClockSweeper handle the transient case.
    }

    try {
      var iterator = map.entrySet().iterator();
      while (currentWeightBytes.get() > maxWeightBytes && iterator.hasNext()) {
        var entry = iterator.next();
        PageReference ref = entry.getKey();

        // Keep eviction atomic with respect to other cache operations
        map.compute(ref, (k, page) -> {
          if (page == null) {
            return null;
          }
          // Two-pass HOT-bit only in the non-severe path; in severe mode we
          // evict cold-or-hot unguarded pages in a single pass.
          if (!severe && page.isHot()) {
            page.clearHot();
            return page;
          }

          // Skip guarded pages
          if (page.getGuardCount() > 0) {
            if (DEBUG_MEMORY_LEAKS && LOGGER.isDebugEnabled()) {
              LOGGER.debug("Eviction skipped: guarded page key={} type={} rev={} guards={} hot={}", page.getPageKey(),
                  page.getIndexType(), page.getRevision(), page.getGuardCount(), page.isHot());
            }
            return page;
          }

          try {
            // A guard may arrive after the check above. Retire makes that holder the deferred
            // lifecycle owner, while the cache mapping and charge still disappear immediately.
            retireAndUnchargeEvictedPage(ref, page);
            return null;
          } catch (Exception e) {
            LOGGER.error("Failed to evict page {} during budget enforcement: {}", page.getPageKey(), e.getMessage());
            return page;
          }
        });
      }

      // If we still exceed budget and diagnostics enabled, log a small sample of guarded pages.
      if (DEBUG_MEMORY_LEAKS && currentWeightBytes.get() > maxWeightBytes && LOGGER.isWarnEnabled()) {
        logGuardedPagesSample();
      }
    } finally {
      evictionLock.unlock();
    }
  }

  /**
   * Force eviction under global allocator pressure. Evicts unguarded pages even if
   * this cache is within its own budget — the global allocator budget is shared across
   * all caches and the TIL, so this cache must shed pages when the allocator cannot
   * satisfy an allocation regardless of local budget state.
   */
  public void evictUnderPressure() {
    if (maxWeightBytes <= 0 || map.isEmpty()) {
      return;
    }
    // Shed a quarter of what is actually HELD, not a quarter of the budget. Gating on the budget
    // made this a no-op in the one situation it exists for: a cache far under its own budget whose
    // pages are nevertheless pinning every frame the allocator owns. The caller only reaches here
    // because an allocation already failed, so making no progress means the retry loop OOMs.
    final long target = Math.min(maxWeightBytes, currentWeightBytes.get()) * 3 / 4;
    evictionLock.lock();
    try {
      var iterator = map.entrySet().iterator();
      while (currentWeightBytes.get() > target && iterator.hasNext()) {
        var entry = iterator.next();
        final PageReference ref = entry.getKey();
        map.compute(ref, (k, page) -> {
          if (page == null || page.getGuardCount() > 0) {
            return page;
          }
          // Second-chance: give a recently-accessed page one reprieve before
          // eviction, matching ClockSweeper.sweep() and evictIfOverBudget().
          if (page.isHot()) {
            page.clearHot();
            return page;
          }
          try {
            // Retire before detaching so a guard acquired after getGuardCount() owns the deferred
            // close. The recorded insertion charge is removed even when physical release waits.
            retireAndUnchargeEvictedPage(ref, page);
            return null;
          } catch (Exception e) {
            LOGGER.debug("evictUnderPressure failed for page {}: {}", page.getPageKey(), e.getMessage());
            return page;
          }
        });
      }
    } finally {
      evictionLock.unlock();
    }
  }

  private void logGuardedPagesSample() {
    int logged = 0;
    for (V page : map.values()) {
      if (page.getGuardCount() > 0) {
        LOGGER.warn("Guarded page prevents eviction: key={} type={} rev={} guards={} hot={}", page.getPageKey(),
            page.getIndexType(), page.getRevision(), page.getGuardCount(), page.isHot());
        if (++logged >= 5) {
          break;
        }
      }
    }
    if (logged == 0 && LOGGER.isDebugEnabled()) {
      LOGGER.debug("No guarded pages found, but cache over budget. CurrentWeight={} MaxWeight={}",
          currentWeightBytes.get(), maxWeightBytes);
    }
  }

  public String getDiagnostics() {
    long totalPages = 0;
    long totalMemory = 0;
    long hotPages = 0;

    for (V page : map.values()) {
      totalPages++;
      totalMemory += page.getActualMemorySize();
      if (page.isHot()) {
        hotPages++;
      }
    }

    return String.format("ShardedPageCache: pages=%d, hot=%d (%.1f%%), memory=%.2fMB", totalPages, hotPages,
        totalPages > 0
            ? (hotPages * 100.0 / totalPages)
            : 0,
        totalMemory / (1024.0 * 1024.0));
  }
}
