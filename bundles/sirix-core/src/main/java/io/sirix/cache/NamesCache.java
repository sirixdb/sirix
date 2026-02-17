package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.index.name.Names;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public final class NamesCache implements Cache<NamesCacheKey, Names> {

  private final com.github.benmanes.caffeine.cache.Cache<NamesCacheKey, Names> cache;

  public NamesCache(final int maxSize) {
    cache = Caffeine.newBuilder().initialCapacity(maxSize).maximumSize(maxSize).scheduler(scheduler).build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public Names get(NamesCacheKey key) {
    return cache.getIfPresent(key);
  }

  @Override
  public Names get(NamesCacheKey key,
      BiFunction<? super NamesCacheKey, ? super Names, ? extends Names> mappingFunction) {
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void put(NamesCacheKey key, @NonNull Names value) {
    cache.put(key, value);
  }

  @Override
  public void putIfAbsent(NamesCacheKey key, Names value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public void putAll(Map<? extends NamesCacheKey, ? extends Names> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<NamesCacheKey, Names> getAll(Iterable<? extends NamesCacheKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(NamesCacheKey key) {
    cache.invalidate(key);
  }

  @Override
  public void close() {}
}
