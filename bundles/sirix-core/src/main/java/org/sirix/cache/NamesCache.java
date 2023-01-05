package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.sirix.index.name.Names;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class NamesCache implements Cache<NamesCacheKey, Names> {

  private final com.github.benmanes.caffeine.cache.Cache<NamesCacheKey, Names> cache;

  public NamesCache(final int maxSize) {
    cache = Caffeine.newBuilder()
                    .maximumSize(maxSize)
                    .build();
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
  public void put(NamesCacheKey key, Names value) {
    cache.put(key, value);
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
  public void close() {
  }
}
