package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.index.redblacktree.RBNode;

import java.util.Map;

public final class RedBlackTreeNodeCache implements Cache<RBIndexKey, RBNode<?, ?>> {

  private final com.github.benmanes.caffeine.cache.Cache<RBIndexKey, RBNode<?, ?>> cache;

  public RedBlackTreeNodeCache(final int maxSize) {
    final RemovalListener<RBIndexKey, RBNode<?, ?>> removalListener =
        (RBIndexKey key, RBNode<?, ?> value, RemovalCause cause) -> {
          assert key != null;
          assert value != null;
          final RBNode<?, ?> parent = value.getParent();

          if (parent != null) {
            if (value.equals(parent.getLeftChild())) {
              parent.setLeftChild(null);
            } else if (value.equals(parent.getRightChild())) {
              parent.setRightChild(null);
            }
          }
        };

    cache = Caffeine.newBuilder().maximumSize(maxSize).removalListener(removalListener).build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public RBNode<?, ?> get(RBIndexKey key) {
    return cache.getIfPresent(key);
  }

  @Override
  public void put(RBIndexKey key, @NonNull RBNode<?, ?> value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends RBIndexKey, ? extends RBNode<?, ?>> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<RBIndexKey, RBNode<?, ?>> getAll(Iterable<? extends RBIndexKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(RBIndexKey key) {
    cache.invalidate(key);
  }

  @Override
  public void close() {
  }
}
