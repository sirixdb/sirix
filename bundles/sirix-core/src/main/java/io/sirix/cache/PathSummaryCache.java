package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class PathSummaryCache implements Cache<PathSummaryCacheKey, PathSummaryData> {

  private final com.github.benmanes.caffeine.cache.Cache<PathSummaryCacheKey, PathSummaryData> cache;

  public PathSummaryCache(final int maxSize) {
    cache = Caffeine.newBuilder().initialCapacity(maxSize).maximumSize(maxSize).scheduler(scheduler).build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public PathSummaryData get(PathSummaryCacheKey key) {
    return cache.getIfPresent(key);
  }

  @Override
  public PathSummaryData get(PathSummaryCacheKey key,
      BiFunction<? super PathSummaryCacheKey, ? super PathSummaryData, ? extends PathSummaryData> mappingFunction) {
    // Load-if-absent, not compute: asMap().compute() invokes the mapping function on a HIT too,
    // which turns a cache into a recompute-and-overwrite. Nothing calls this overload today —
    // PathSummaryReader uses the single-argument get — but it is the same trap that cost the
    // PageCache, the RevisionRootPageCache and the NamesCache their reason to exist, and a future
    // caller would pay it silently.
    final PathSummaryData hit = cache.getIfPresent(key);
    if (hit != null) {
      return hit;
    }
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void putIfAbsent(PathSummaryCacheKey key, PathSummaryData value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public void put(PathSummaryCacheKey key, PathSummaryData value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends PathSummaryCacheKey, ? extends PathSummaryData> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<PathSummaryCacheKey, PathSummaryData> getAll(Iterable<? extends PathSummaryCacheKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(PathSummaryCacheKey key) {
    cache.invalidate(key);
  }

  @Override
  public ConcurrentMap<PathSummaryCacheKey, PathSummaryData> asMap() {
    return cache.asMap();
  }

  @Override
  public void close() {}
}
