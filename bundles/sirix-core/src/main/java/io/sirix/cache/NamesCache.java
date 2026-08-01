package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.index.name.Names;

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
    // Fast path. asMap().compute() locks the key's bin AND invokes the mapping function
    // unconditionally — including on a hit. The only caller is a pure load-if-absent
    // (NamePage.getNames, whose function walks the whole name dictionary out of storage and then
    // copies it), so going through compute() rebuilt a dictionary that was already cached and then
    // overwrote the entry with the fresh copy: the cache kept the names alive but never saved the
    // reconstruction it exists to save. Same defect as the one already fixed in PageCache and
    // RevisionRootPageCache; this instance was paid on the first name lookup of every read
    // transaction. getIfPresent is lock-free.
    final Names hit = cache.getIfPresent(key);
    if (hit != null) {
      return hit;
    }
    // Miss: compute under the bin lock so concurrent misses on the same revision rebuild once.
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void put(NamesCacheKey key, Names value) {
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
