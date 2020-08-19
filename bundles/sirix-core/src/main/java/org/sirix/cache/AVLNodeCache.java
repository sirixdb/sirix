package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.sirix.index.avltree.AVLNode;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class AVLNodeCache implements Cache<AVLIndexKey, AVLNode<?, ?>> {

  private final com.github.benmanes.caffeine.cache.Cache<AVLIndexKey, AVLNode<?, ?>> pageCache;

  public AVLNodeCache(final int maxSize) {
    pageCache = Caffeine.newBuilder()
                        .maximumSize(maxSize)
                        .expireAfterWrite(5, TimeUnit.SECONDS)
                        .expireAfterAccess(5, TimeUnit.SECONDS)
                        .build();
  }

  @Override
  public void clear() {
    pageCache.invalidateAll();
  }

  @Override
  public AVLNode<?, ?> get(AVLIndexKey key) {
    return pageCache.getIfPresent(key);
  }

  @Override
  public void put(AVLIndexKey key, AVLNode<?, ?> value) {
    pageCache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends AVLIndexKey, ? extends AVLNode<?, ?>> map) {
    pageCache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<AVLIndexKey, AVLNode<?, ?>> getAll(Iterable<? extends AVLIndexKey> keys) {
    return pageCache.getAllPresent(keys);
  }

  @Override
  public void remove(AVLIndexKey key) {
    pageCache.invalidate(key);
  }

  @Override
  public void close() {}
}
