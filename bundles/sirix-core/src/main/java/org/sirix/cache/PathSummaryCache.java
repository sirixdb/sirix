package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;

public class PathSummaryCache implements Cache<Integer, PathSummaryData> {

  private final com.github.benmanes.caffeine.cache.Cache<Integer, PathSummaryData> cache;

  public PathSummaryCache(final int maxSize) {
    cache = Caffeine.newBuilder()
                    .maximumSize(maxSize)
                    .build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public PathSummaryData get(Integer key) {
    return cache.getIfPresent(key);
  }

  @Override
  public void put(Integer key, PathSummaryData value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends PathSummaryData> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Integer, PathSummaryData> getAll(Iterable<? extends Integer> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(Integer key) {
    cache.invalidate(key);
  }

  @Override
  public void close() {
  }
}
