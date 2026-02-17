package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.node.interfaces.Node;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;

public final class RedBlackTreeNodeCache implements Cache<RBIndexKey, Node> {

  private final com.github.benmanes.caffeine.cache.Cache<RBIndexKey, Node> cache;

  public RedBlackTreeNodeCache(final int maxSize) {
    final RemovalListener<RBIndexKey, Node> removalListener = (RBIndexKey key, Node value, RemovalCause cause) -> {
      assert key != null;
      assert value != null;

      if (value instanceof RBNodeKey<?> rbNodeKey) {
        final RBNodeKey<?> parent = rbNodeKey.getParent();

        if (parent != null) {
          if (value.equals(parent.getLeftChild())) {
            parent.setLeftChild(null);
          } else if (value.equals(parent.getRightChild())) {
            parent.setRightChild(null);
          }
        }
      }
    };

    cache = Caffeine.newBuilder()
                    .initialCapacity(maxSize)
                    .maximumSize(maxSize)
                    .removalListener(removalListener)
                    .scheduler(scheduler)
                    .build();
  }

  @Override
  public void putIfAbsent(RBIndexKey key, Node value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public Node get(RBIndexKey key) {
    return cache.getIfPresent(key);
  }

  /**
   * Zero-allocation lookup using any object with compatible hashCode/equals. Uses asMap().get() which
   * accepts Object per the Java Map contract, enabling use of mutable {@link RBIndexKeyLookup} for
   * cache lookups without allocation.
   *
   * @param key the lookup key (must have compatible hashCode/equals with RBIndexKey)
   * @return the node, or null if not present
   */
  @Override
  public Node lookup(Object key) {
    return cache.asMap().get(key);
  }

  @Override
  public void put(RBIndexKey key, @NonNull Node value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends RBIndexKey, ? extends Node> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<RBIndexKey, Node> getAll(Iterable<? extends RBIndexKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(RBIndexKey key) {
    cache.invalidate(key);
  }

  @Override
  public void close() {}
}
