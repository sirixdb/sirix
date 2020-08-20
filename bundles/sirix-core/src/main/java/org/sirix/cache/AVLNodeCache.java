package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.sirix.index.avltree.AVLNode;

import javax.annotation.Nonnull;
import java.util.Map;

public final class AVLNodeCache implements Cache<AVLIndexKey, AVLNode<?, ?>> {

  private final com.github.benmanes.caffeine.cache.Cache<AVLIndexKey, AVLNode<?, ?>> pageCache;

  public AVLNodeCache(final int maxSize) {
    final RemovalListener<AVLIndexKey, AVLNode<?, ?>> removalListener =
        (AVLIndexKey key, AVLNode<?, ?> value, RemovalCause cause) -> {
          assert key != null;
          assert value != null;
          final AVLNode<?, ?> parent = value.getParent();

          if (parent != null) {
            if (parent.getLeftChild().equals(value)) {
              parent.setLeftChild(null);
            } else if (parent.getRightChild().equals(value)) {
              parent.setRightChild(null);
            }
          }
        };

    pageCache = Caffeine.newBuilder().maximumSize(maxSize).removalListener(removalListener).build();
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
  public void put(AVLIndexKey key, @Nonnull AVLNode<?, ?> value) {
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
  public void close() {
  }
}
