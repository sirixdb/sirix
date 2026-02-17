package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.function.BiFunction;

public final class PageKeyToOffsetCache implements Cache<Long, Long> {

  private final com.github.benmanes.caffeine.cache.Cache<Long, Long> cache;

  public PageKeyToOffsetCache() {
    cache = Caffeine.newBuilder().build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public Long get(Long key) {
    return cache.getIfPresent(key);
  }

  @Override
  public Long get(Long key, BiFunction<? super Long, ? super Long, ? extends Long> mappingFunction) {
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void put(Long key, @NonNull Long value) {
    cache.put(key, value);
  }

  @Override
  public void putIfAbsent(Long key, Long value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public void putAll(Map<? extends Long, ? extends Long> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Long, Long> getAll(Iterable<? extends Long> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(Long key) {
    cache.invalidate(key);
  }

  @Override
  public void close() {}
}
