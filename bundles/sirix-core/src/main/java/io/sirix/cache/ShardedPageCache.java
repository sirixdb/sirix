package io.sirix.cache;

import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
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
 * prevents concurrent ClockSweeper sweeps - cache hits remain lock-free - a shared lifecycle lock
 * covers only mapping mutations, allowing clear() to establish a quiescent point without
 * serializing normal mutations across keys.
 *
 * @author Johannes Lichtenberger
 */
public final class ShardedPageCache<V extends CacheablePage> implements Cache<PageReference, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShardedPageCache.class);

  static final boolean DEBUG_MEMORY_LEAKS = Boolean.getBoolean("sirix.debug.memoryLeaks");

  private final ConcurrentHashMap<PageReference, V> map = new ConcurrentHashMap<>();
  private final ConcurrentMap<PageReference, V> readOnlyMap = new ReadOnlyConcurrentMap<>(map);
  private final ReentrantLock evictionLock = new ReentrantLock();
  private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
  private final Shard<V> shard;
  /** Preallocated callback + state guarded by evictionLock for allocation-free exact removal. */
  private final BiFunction<PageReference, V, V> conditionalRemovalFunction;
  private CacheablePage conditionalRemovalExpected;
  private boolean conditionalRemovalSucceeded;
  private boolean conditionalRemovalRetires;
  private RetirementFailures conditionalRemovalFailures;
  private final long maxWeightBytes;
  private final AtomicLong currentWeightBytes = new AtomicLong(0L);

  /**
   * Weight actually CHARGED per cached entry. Removal/eviction must subtract exactly what insertion
   * added: a page's native size can become 0 once it is closed, and a HOT leaf's composite weight can
   * change before publication, so symmetric weightOf-based accounting drifted upward until the cache
   * was permanently pinned in the severe-eviction branch.
   */
  private final ConcurrentHashMap<PageReference, CacheCharge> insertedWeights = new ConcurrentHashMap<>();

  /**
   * Identity-stamped charge so a failed admission can never roll back a racing successor's weight.
   */
  private static final class CacheCharge {
    final long weight;

    CacheCharge(long weight) {
      this.weight = weight;
    }
  }

  /**
   * Live observational view of the ownership map.
   *
   * <p>
   * Publishing the raw {@link ConcurrentHashMap} would let callers bypass page retirement, swizzle
   * cleanup, exact weight accounting, and the lifecycle lock used by {@link #clear()}.
   * {@link ConcurrentMap} permits implementations to reject optional mutation operations. This
   * wrapper does so for every direct and default-method mutation route, while preserving lock-free
   * reads and weakly consistent iteration. Its collection views are unmodifiable as well, including
   * iterator removal and {@link Map.Entry#setValue(Object)}.
   * </p>
   */
  private static final class ReadOnlyConcurrentMap<K, T> extends AbstractMap<K, T> implements ConcurrentMap<K, T> {

    private final ConcurrentMap<K, T> delegate;
    private final Map<K, T> unmodifiable;

    private ReadOnlyConcurrentMap(final ConcurrentMap<K, T> delegate) {
      this.delegate = delegate;
      unmodifiable = Collections.unmodifiableMap(delegate);
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
      return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
      return delegate.containsValue(value);
    }

    @Override
    public T get(final Object key) {
      return delegate.get(key);
    }

    @Override
    public T getOrDefault(final Object key, final T defaultValue) {
      return delegate.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(final BiConsumer<? super K, ? super T> action) {
      delegate.forEach(action);
    }

    @Override
    public Set<K> keySet() {
      return unmodifiable.keySet();
    }

    @Override
    public Collection<T> values() {
      return unmodifiable.values();
    }

    @Override
    public Set<Entry<K, T>> entrySet() {
      return unmodifiable.entrySet();
    }

    @Override
    public T put(final K key, final T value) {
      throw readOnly();
    }

    @Override
    public T remove(final Object key) {
      throw readOnly();
    }

    @Override
    public void putAll(final Map<? extends K, ? extends T> mappings) {
      throw readOnly();
    }

    @Override
    public void clear() {
      throw readOnly();
    }

    @Override
    public T putIfAbsent(final K key, final T value) {
      throw readOnly();
    }

    @Override
    public boolean remove(final Object key, final Object value) {
      throw readOnly();
    }

    @Override
    public boolean replace(final K key, final T oldValue, final T newValue) {
      throw readOnly();
    }

    @Override
    public T replace(final K key, final T value) {
      throw readOnly();
    }

    @Override
    public void replaceAll(final BiFunction<? super K, ? super T, ? extends T> function) {
      throw readOnly();
    }

    @Override
    public T computeIfAbsent(final K key, final Function<? super K, ? extends T> mappingFunction) {
      throw readOnly();
    }

    @Override
    public T computeIfPresent(final K key, final BiFunction<? super K, ? super T, ? extends T> remappingFunction) {
      throw readOnly();
    }

    @Override
    public T compute(final K key, final BiFunction<? super K, ? super T, ? extends T> remappingFunction) {
      throw readOnly();
    }

    @Override
    public T merge(final K key, final T value, final BiFunction<? super T, ? super T, ? extends T> remappingFunction) {
      throw readOnly();
    }

    private static UnsupportedOperationException readOnly() {
      return new UnsupportedOperationException("ShardedPageCache.asMap() is read-only; use the cache API");
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

  private static final class RetirementFailures {
    private Throwable first;

    private void retain(final Throwable failure) {
      first = retainCleanupFailure(first, failure);
    }

    private void throwIfPresent() {
      rethrowCleanupFailure(first);
    }

    /**
     * Surface retained retirement failures WITHOUT failing the caller. Budget evictions run inside
     * operations on UNRELATED pages that already succeeded — throwing an arbitrary evicted page's
     * retirement failure at that caller converts background hygiene into a foreground query failure,
     * with the caller's own page left published but its guard released. The failure stays loud: an
     * ERROR log plus a monotonic counter health checks and tests can watch. Operations that own the
     * failed page (clear, close, remove) keep {@link #throwIfPresent()}.
     */
    private void logIfPresent(final String operation) {
      if (first != null) {
        EVICTION_RETIREMENT_FAILURES.incrementAndGet();
        LOGGER.error("Page retirement failed during {} — the evicted page's frame may be leaked; "
            + "eviction continued and the triggering operation is unaffected", operation, first);
      }
    }
  }

  /** Retirement failures surfaced by evictions running on behalf of unrelated operations. */
  private static final AtomicLong EVICTION_RETIREMENT_FAILURES = new AtomicLong();

  /** Monotonic count of eviction-path retirement failures — observability for health checks. */
  public static long evictionRetirementFailureCount() {
    return EVICTION_RETIREMENT_FAILURES.get();
  }

  /** Publish an already-validated weight without re-reading mutable page state. */
  private void chargeWeight(PageReference key, long weight) {
    final CacheCharge charge = weight > 0
        ? new CacheCharge(weight)
        : null;
    final CacheCharge previous = replaceCharge(key, charge);
    final long delta = weight - chargedWeight(previous);
    if (delta != 0) {
      adjustCurrentWeight(delta);
    }
  }

  /** Install one pre-created charge token, returning the exact token it replaced. */
  private CacheCharge replaceCharge(PageReference key, CacheCharge charge) {
    return charge != null
        ? insertedWeights.put(key, charge)
        : insertedWeights.remove(key);
  }

  private static long chargedWeight(CacheCharge charge) {
    return charge != null
        ? charge.weight
        : 0L;
  }

  /** Stage an identity-addressable charge so a failed CHM publication can restore its predecessor. */
  private void chargeGuardedCandidate(PageReference key, long weight, GuardedLoadState<?> state) {
    state.appliedCharge = weight > 0
        ? new CacheCharge(weight)
        : null;
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
  private void unchargeWeight(PageReference key, RetirementFailures failures) {
    try {
      final CacheCharge previous = insertedWeights.remove(key);
      if (previous != null && previous.weight > 0) {
        adjustCurrentWeight(-previous.weight);
      }
    } catch (RuntimeException | Error accountingFailure) {
      failures.retain(accountingFailure);
    }
  }

  /** Clear only this page's swizzle; never erase a replacement installed on the same reference. */
  private static void clearSwizzleIfSame(PageReference reference, CacheablePage page, RetirementFailures failures) {
    try {
      if (page instanceof Page swizzledPage) {
        reference.clearPageIfSame(swizzledPage);
      }
    } catch (RuntimeException | Error swizzleFailure) {
      failures.retain(swizzleFailure);
    }
  }

  /**
   * Retire a page whose cache ownership has ended. A live guard becomes the sole lifecycle owner and
   * performs the deferred physical close on its final release.
   */
  private static void retireDetachedPage(CacheablePage page, RetirementFailures failures) {
    try {
      if (!page.isClosed()) {
        page.retire();
      }
    } catch (RuntimeException | Error retirementFailure) {
      failures.retain(retirementFailure);
    }
  }

  /**
   * Remove the cache's ownership and recorded charge after an eviction selected {@code page} under
   * {@code reference}'s map-compute lock.
   *
   * <p>
   * A guard can arrive after the caller's guard-count check. {@link CacheablePage#retire()} then
   * marks the page orphaned and defers its physical close to that holder; the mapping and charge must
   * still disappear now. The page-local version is bumped only when no holder deferred the close.
   * </p>
   */
  private void retireAndUnchargeEvictedPage(PageReference reference, CacheablePage page, RetirementFailures failures) {
    try {
      retireDetachedPage(page, failures);
      if (page.isClosed()) {
        try {
          page.incrementVersion();
        } catch (RuntimeException | Error versionFailure) {
          failures.retain(versionFailure);
        }
      }
    } finally {
      try {
        clearSwizzleIfSame(reference, page, failures);
      } finally {
        unchargeWeight(reference, failures);
      }
    }
  }

  /**
   * Remove and uncharge {@code expected} only if it is still the exact mapping at {@code key}.
   * Closed-page cleanup is rare and takes the eviction lock; the common removePage path already owns
   * it and calls {@link #removeMappingIfSameWhileLocked(PageReference, CacheablePage)} directly.
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
   * Exact conditional removal without a per-call holder or capturing lambda allocation. The callback
   * and its mutable success state are cache-owned and serialized by evictionLock.
   */
  private boolean removeMappingIfSameWhileLocked(PageReference key, CacheablePage expected) {
    if (!evictionLock.isHeldByCurrentThread()) {
      throw new IllegalStateException("Exact cache removal requires the eviction lock");
    }
    if (conditionalRemovalExpected != null) {
      throw new IllegalStateException("Nested exact cache removal is not supported");
    }

    final RetirementFailures failures = new RetirementFailures();
    final boolean removed = removeMappingIfSameWhileLocked(key, expected, false, failures);
    failures.throwIfPresent();
    return removed;
  }

  /**
   * Preallocated-callback removal used by both instance transfer and clear. The eviction lock owns
   * the callback's mutable state; clear additionally owns the lifecycle write lock.
   */
  private boolean removeMappingIfSameWhileLocked(PageReference key, CacheablePage expected, boolean retire,
      RetirementFailures failures) {
    conditionalRemovalExpected = expected;
    conditionalRemovalSucceeded = false;
    conditionalRemovalRetires = retire;
    conditionalRemovalFailures = failures;
    try {
      map.compute(key, conditionalRemovalFunction);
      return conditionalRemovalSucceeded;
    } finally {
      conditionalRemovalExpected = null;
      conditionalRemovalSucceeded = false;
      conditionalRemovalRetires = false;
      conditionalRemovalFailures = null;
    }
  }

  /** Preallocated {@link ConcurrentHashMap#compute} callback; state is guarded by evictionLock. */
  private V removeExpectedMapping(PageReference reference, V current) {
    if (current != conditionalRemovalExpected) {
      return current;
    }
    if (conditionalRemovalRetires) {
      retireDetachedPage(current, conditionalRemovalFailures);
    }
    clearSwizzleIfSame(reference, current, conditionalRemovalFailures);
    unchargeWeight(reference, conditionalRemovalFailures);
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
  Throwable onEvicted(PageReference ref, CacheablePage page) {
    final RetirementFailures failures = new RetirementFailures();
    retireAndUnchargeEvictedPage(ref, page, failures);
    CACHE_EVICTIONS.increment();
    return failures.first;
  }

  /**
   * Compute the weight (bytes) of a cached page.
   *
   * <p>
   * HOT leaves add their allocation-free retained-heap estimate to the separately reported native
   * frame. Re-putting a page samples its current estimate and replaces the recorded charge.
   * </p>
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

  static Throwable retainCleanupFailure(final Throwable primary, final Throwable secondary) {
    if (secondary == null) {
      return primary;
    }
    if (primary == null) {
      return secondary;
    }
    addSuppressedSafely(primary, secondary);
    return primary;
  }

  @SuppressWarnings("unchecked")
  static <T extends Throwable> void rethrowCleanupFailure(final Throwable failure) throws T {
    if (failure != null) {
      throw (T) failure;
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
  public V get(PageReference key, BiFunction<? super PageReference, ? super V, ? extends V> mappingFunction) {
    V existing = map.get(key);
    if (existing != null && !existing.isClosed()) {
      existing.markAccessed();
      return existing;
    }

    final RetirementFailures failures = new RetirementFailures();
    final V page;
    lifecycleLock.readLock().lock();
    try {
      page = map.compute(key, (k, existingValue) -> {
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
                newPage.getIndexType(), newPage.getRevision(), System.identityHashCode(newPage),
                newPage.getGuardCount());
          }
          // Replace the recorded charge before lifecycle mutation. After validation succeeds, the
          // remaining detach operations are fail-closed and cannot make compute retain a dead value.
          chargeWeight(k, newWeight);
          if (existingValue != null) {
            clearSwizzleIfSame(k, existingValue, failures);
            retireDetachedPage(existingValue, failures);
          }
          return newPage;
        }
        if (existingValue != null) {
          retireDetachedPage(existingValue, failures);
          clearSwizzleIfSame(k, existingValue, failures);
          unchargeWeight(k, failures);
        }
        return null;
      });
      failures.throwIfPresent();
    } finally {
      lifecycleLock.readLock().unlock();
    }

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

    final RetirementFailures failures = new RetirementFailures();
    lifecycleLock.readLock().lock();
    try {
      final V guarded = map.compute(key, (k, existingValue) -> {
        // acquireGuard() can fail even after an isClosed() check: a non-compute closer
        // (truncate sweep, TIL teardown) may close the page concurrently. NEVER hand out a
        // page whose guard was not actually acquired — the caller would use it unprotected.
        if (existingValue != null && !existingValue.isClosed() && existingValue.acquireGuard()) {
          existingValue.markAccessed();
          return existingValue;
        }
        if (existingValue != null) {
          retireDetachedPage(existingValue, failures);
          clearSwizzleIfSame(k, existingValue, failures);
          unchargeWeight(k, failures);
        }
        return null;
      });
      failures.throwIfPresent();
      return guarded;
    } finally {
      lifecycleLock.readLock().unlock();
    }
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
    final RetirementFailures failures = new RetirementFailures();
    final V page;
    lifecycleLock.readLock().lock();
    try {
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
              retireDetachedPage(loaded, failures);
              loaded = null;
            }
          }

          if (loaded == null || loaded.isClosed()) {
            if (existingInCompute != null) {
              retireDetachedPage(existingInCompute, failures);
              clearSwizzleIfSame(k, existingInCompute, failures);
              unchargeWeight(k, failures);
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

      if (failures.first != null) {
        if (loadState.candidateGuarded && loadState.candidate == page) {
          try {
            page.releaseGuard();
          } catch (final RuntimeException | Error guardReleaseFailure) {
            failures.retain(guardReleaseFailure);
          } finally {
            loadState.candidateGuarded = false;
          }
        }
        failures.throwIfPresent();
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
    } finally {
      lifecycleLock.readLock().unlock();
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
      final RetirementFailures failures = new RetirementFailures();
      clearSwizzleIfSame(key, displaced, failures);
      retireDetachedPage(displaced, failures);
      failures.throwIfPresent();
    }
  }

  /**
   * Preserve the exact publication failure while restoring charge and private candidate ownership.
   */
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
    final RetirementFailures failures = new RetirementFailures();
    lifecycleLock.readLock().lock();
    try {
      map.compute(key, (k, existing) -> {
        chargeWeight(k, valueWeight);
        if (existing != null && existing != value) {
          // Returning value transfers this key's cache ownership from existing to value. Any reader
          // that already guarded existing owns its deferred lifetime; without a guard there is no
          // remaining owner, so retire closes it immediately. This applies equally to record and HOT
          // leaves: a transaction that wants to retain a page must first removePage() and take
          // ownership, rather than leave one instance simultaneously owned by the TIL and cache.
          clearSwizzleIfSame(k, existing, failures);
          retireDetachedPage(existing, failures);
        }
        return value;
      });
      failures.throwIfPresent();
    } finally {
      lifecycleLock.readLock().unlock();
    }

    evictIfOverBudget();
  }

  @Override
  public void putIfAbsent(PageReference key, V value) {
    if (value == null) {
      throw new NullPointerException("Cannot cache null page");
    }
    value.markAccessed();
    lifecycleLock.readLock().lock();
    try {
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
    } finally {
      lifecycleLock.readLock().unlock();
    }
    evictIfOverBudget();
  }

  @Override
  public void clear() {
    final RetirementFailures failures = new RetirementFailures();
    lifecycleLock.writeLock().lock();
    evictionLock.lock();
    try {
      // With admissions quiesced, CHM's weak iterator is stable. Remove one ownership unit at a
      // time through the preallocated compute callback: no cache-sized snapshot and no interval in
      // which a mapping can survive without its exact charge (or vice versa).
      for (final PageReference key : map.keySet()) {
        final V page = map.get(key);
        if (page != null) {
          removeMappingIfSameWhileLocked(key, page, true, failures);
        }
      }
      // A prior accounting failure may have left a charge without a mapping. Clear such diagnostics
      // only after every actual page owner has been retired.
      insertedWeights.clear();
      currentWeightBytes.set(0L);
      shard.clockHand = 0;
    } finally {
      evictionLock.unlock();
      lifecycleLock.writeLock().unlock();
    }
    failures.throwIfPresent();
  }

  @Override
  public V removeAndGet(PageReference key) {
    // One compute: whatever is mapped when the entry goes is exactly what the caller is handed, so
    // a page cached by another thread between a get and a remove cannot slip out unowned.
    final V[] removed = (V[]) new CacheablePage[1];
    final RetirementFailures failures = new RetirementFailures();
    lifecycleLock.readLock().lock();
    try {
      map.compute(key, (k, page) -> {
        if (page != null) {
          removed[0] = page;
          unchargeWeight(k, failures);
          clearSwizzleIfSame(k, page, failures);
        }
        return null;
      });
      if (failures.first != null && removed[0] != null) {
        retireDetachedPage(removed[0], failures);
      }
      failures.throwIfPresent();
    } finally {
      lifecycleLock.readLock().unlock();
    }
    return removed[0];
  }

  @Override
  public void remove(PageReference key) {
    final RetirementFailures failures = new RetirementFailures();
    lifecycleLock.readLock().lock();
    try {
      map.compute(key, (k, page) -> {
        if (page != null) {
          unchargeWeight(k, failures);
          clearSwizzleIfSame(k, page, failures);
        }
        return null;
      });
      failures.throwIfPresent();
    } finally {
      lifecycleLock.readLock().unlock();
    }
  }

  /**
   * Remove a page from the cache by reference identity, without closing it.
   *
   * <p>
   * Unlike {@link #remove(PageReference)} this matches on instance identity (HOT leaf and record
   * pages do not override {@code equals}) and does <em>not</em> release the page's off-heap memory —
   * the caller keeps ownership. Used so the transaction-intent log can take a dirty,
   * transaction-private page out of the shared cache, preventing the sweeper and pressure eviction
   * from reclaiming its slot before commit serializes it.
   * </p>
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
   * <p>
   * The inherited default is {@code asMap().containsValue(page)} — a linear scan of the whole cache.
   * That runs per orphaned page when a write transaction tears down its page containers, so a large
   * cache turned teardown into O(entries x containers).
   *
   * <p>
   * A page remembers the reference it was last cached under, which answers the question in O(1)
   * whenever it is still cached there — the overwhelmingly common case, since a container's HOT leaf
   * usually IS the cache's instance. Only a positive identity match is conclusive: a page re-cached
   * under a NEW reference (copy-on-write moves the root reference's key) remembers just the latest
   * one, so a mismatch has to fall back to the exact scan. Getting that backwards would free a page
   * the cache is still handing to readers.
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

  /**
   * Return the cache's live, weakly consistent observational view.
   *
   * <p>
   * All mutation operations, including mutations through collection views and {@link ConcurrentMap}
   * default methods, throw {@link UnsupportedOperationException}. Callers must use this cache's
   * mutation methods so lifecycle and weight ownership remain exact.
   * </p>
   */
  @Override
  public ConcurrentMap<PageReference, V> asMap() {
    return readOnlyMap;
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
   * <li><b>Normal over-budget (&lt; 110% of limit):</b> non-blocking {@code tryLock}, two-pass
   * HOT-bit algorithm. Lets concurrent writers through and delegates to the background
   * {@link ClockSweeper}.</li>
   * <li><b>Severe over-budget (&ge; 110% of limit):</b> blocking {@code lock}, single-pass eviction —
   * HOT bit is ignored since at high concurrency the two-pass approach never catches up (bit gets
   * re-set between sweeps).</li>
   * </ul>
   *
   * <p>
   * Why the split: at 20 parallel readers, tryLock succeeds only intermittently and the two-pass HOT
   * logic leaves every page permanently hot during a scan. That starves the allocator budget and
   * surfaces as {@code OutOfMemoryError} in {@code MemorySegmentAllocator.allocate} 10+ seconds
   * later. Forcing a blocking one-pass eviction when we're significantly over budget makes eviction
   * keep up with allocation pressure.
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

    final RetirementFailures failures = new RetirementFailures();
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

          retireAndUnchargeEvictedPage(ref, page, failures);
          return null;
        });
      }

      // If we still exceed budget and diagnostics enabled, log a small sample of guarded pages.
      if (DEBUG_MEMORY_LEAKS && currentWeightBytes.get() > maxWeightBytes && LOGGER.isWarnEnabled()) {
        logGuardedPagesSample();
      }
    } finally {
      evictionLock.unlock();
    }
    failures.logIfPresent("evictIfOverBudget");
  }

  /**
   * Force eviction under global allocator pressure. Evicts unguarded pages even if this cache is
   * within its own budget — the global allocator budget is shared across all caches and the TIL, so
   * this cache must shed pages when the allocator cannot satisfy an allocation regardless of local
   * budget state.
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
    final RetirementFailures failures = new RetirementFailures();
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
          retireAndUnchargeEvictedPage(ref, page, failures);
          return null;
        });
      }
    } finally {
      evictionLock.unlock();
    }
    failures.logIfPresent("evictUnderPressure");
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
