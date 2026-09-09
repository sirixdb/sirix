package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * Verdict bitsets for string predicates over global projection value dictionaries.
 *
 * <p>
 * Bounded by WEIGHT, not by count, because a verdict's size follows the dictionary's cardinality:
 * one bit per id, so 34 KB for a 275k-entry dictionary and ~2.3 MB for an 18M-entry one. A count
 * bound that is comfortable at the first scale silently becomes hundreds of megabytes at the
 * second, which is the failure this project has hit before by sizing a cache from a small
 * measurement.
 * </p>
 *
 * <p>
 * Missing is always safe: the caller recomputes the sweep it would have run anyway, so the bound
 * costs time and never an answer.
 * </p>
 */
public final class GlobalVerdictCache implements Cache<GlobalVerdictCacheKey, long[]> {

  private final com.github.benmanes.caffeine.cache.Cache<GlobalVerdictCacheKey, long[]> cache;

  /**
   * @param maxWeightBytes total verdict bytes retained; entries beyond it are evicted
   */
  public GlobalVerdictCache(final long maxWeightBytes) {
    if (maxWeightBytes <= 0L) {
      throw new IllegalArgumentException("verdict cache budget must be positive, got " + maxWeightBytes);
    }
    cache = Caffeine.newBuilder().maximumWeight(maxWeightBytes).weigher((GlobalVerdictCacheKey key, long[] verdict) -> {
      final long bytes = (long) verdict.length * Long.BYTES;
      return (int) Math.min(bytes, Integer.MAX_VALUE);
    }).scheduler(scheduler).build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  /**
   * The verdict for {@code key}, or {@code null}.
   *
   * <p>
   * A COPY. The kernels bit-test the array they are handed in their innermost loop, so the array
   * itself has to stay a plain {@code long[]} — an immutable wrapper with a {@code test(id)} call
   * would put a method call inside a per-row loop, which is what the bitset shape exists to avoid.
   * Copying once per predicate protects the cached array from any future consumer instead: at a
   * million distinct values that is 34 KB against the ~58 ms sweep it replaces, and even at a hundred
   * million it is ~2.3 MB against the same sweep — roughly 250x cheaper than recomputing, so
   * correctness here costs a rounding error.
   * </p>
   */
  @Override
  public long[] get(final GlobalVerdictCacheKey key) {
    final long[] cached = cache.getIfPresent(key);
    return cached == null
        ? null
        : cached.clone();
  }

  /**
   * REFUSED. The mapping function for a verdict is a full sweep of every distinct value, and
   * {@code asMap().compute()} runs it under the bin lock — so a second thread asking the same
   * question would block for the length of a sweep instead of running its own, and a third asking a
   * DIFFERENT question that happens to hash to the same bin would block for no reason at all. Use
   * {@link #get(GlobalVerdictCacheKey)} then {@link #put}: two callers may both compute on a miss,
   * which costs a duplicated sweep at worst and never a stall.
   */
  @Override
  public long[] get(final GlobalVerdictCacheKey key,
      final BiFunction<? super GlobalVerdictCacheKey, ? super long[], ? extends long[]> mappingFunction) {
    throw new UnsupportedOperationException(
        "compute-under-bin-lock is refused for verdicts: use get(key) then put(key, verdict)");
  }

  /** No second tier: a verdict is cheap to recompute relative to promoting it anywhere. */
  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<GlobalVerdictCacheKey, long[]> getAll(final Iterable<? extends GlobalVerdictCacheKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void put(final GlobalVerdictCacheKey key, final long[] value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends GlobalVerdictCacheKey, ? extends long[]> map) {
    cache.putAll(map);
  }

  @Override
  public void remove(final GlobalVerdictCacheKey key) {
    cache.invalidate(key);
  }

  @Override
  public ConcurrentMap<GlobalVerdictCacheKey, long[]> asMap() {
    return cache.asMap();
  }

  @Override
  public void close() {
    cache.invalidateAll();
  }
}
