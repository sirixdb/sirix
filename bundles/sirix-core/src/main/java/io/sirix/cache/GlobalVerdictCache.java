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
 * bound that is comfortable at the first scale silently becomes hundreds of megabytes at the second,
 * which is the failure this project has hit before by sizing a cache from a small measurement.
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
    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeightBytes)
                    .weigher((GlobalVerdictCacheKey key, long[] verdict) -> {
                      final long bytes = (long) verdict.length * Long.BYTES;
                      return (int) Math.min(bytes, Integer.MAX_VALUE);
                    })
                    .scheduler(scheduler)
                    .build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public long[] get(final GlobalVerdictCacheKey key) {
    return cache.getIfPresent(key);
  }

  @Override
  public long[] get(final GlobalVerdictCacheKey key,
      final BiFunction<? super GlobalVerdictCacheKey, ? super long[], ? extends long[]> mappingFunction) {
    // getIfPresent first, deliberately: asMap().compute() locks the bin AND runs the mapping
    // function even on a hit, and this function is a full sweep of every distinct value — exactly
    // the work the cache exists to avoid. Same defect already fixed in NamesCache and PageCache.
    final long[] hit = cache.getIfPresent(key);
    if (hit != null) {
      return hit;
    }
    return cache.asMap().compute(key, mappingFunction);
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
