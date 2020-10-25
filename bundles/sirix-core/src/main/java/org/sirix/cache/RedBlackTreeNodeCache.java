package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.sirix.index.redblacktree.RBNode;

import javax.annotation.Nonnull;
import java.util.Map;

public final class RedBlackTreeNodeCache implements Cache<RBIndexKey, RBNode<?, ?>> {

  private final com.github.benmanes.caffeine.cache.Cache<RBIndexKey, RBNode<?, ?>> pageCache;

  public RedBlackTreeNodeCache(final int maxSize) {
    final RemovalListener<RBIndexKey, RBNode<?, ?>> removalListener =
        (RBIndexKey key, RBNode<?, ?> value, RemovalCause cause) -> {
          assert key != null;
          assert value != null;
          final RBNode<?, ?> parent = value.getParent();

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
  public RBNode<?, ?> get(RBIndexKey key) {
    return pageCache.getIfPresent(key);
  }

  @Override
  public void put(RBIndexKey key, @Nonnull RBNode<?, ?> value) {
    pageCache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends RBIndexKey, ? extends RBNode<?, ?>> map) {
    pageCache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<RBIndexKey, RBNode<?, ?>> getAll(Iterable<? extends RBIndexKey> keys) {
    return pageCache.getAllPresent(keys);
  }

  @Override
  public void remove(RBIndexKey key) {
    pageCache.invalidate(key);
  }

  @Override
  public void close() {
  }
}
