package io.sirix.index.redblacktree.keyvalue;

import io.sirix.utils.ToStringHelper;
import java.util.Objects;
import io.sirix.index.redblacktree.interfaces.References;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Set;

/**
 * Text node-ID references.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NodeReferences implements References {
  /** A {@link Set} of node-keys. */
  private final Roaring64Bitmap nodeKeys;

  /**
   * Default constructor.
   */
  public NodeReferences() {
    nodeKeys = new Roaring64Bitmap();
  }

  /**
   * Constructor.
   *
   * @param nodeKeys node keys
   */
  public NodeReferences(final Roaring64Bitmap nodeKeys) {
    assert nodeKeys != null;
    this.nodeKeys = nodeKeys.clone();
  }

  @Override
  public boolean isPresent(final long nodeKey) {
    return nodeKeys.contains(nodeKey);
  }

  @Override
  public Roaring64Bitmap getNodeKeys() {
    return nodeKeys;
  }

  @Override
  public NodeReferences addNodeKey(final long nodeKey) {
    nodeKeys.add(nodeKey);
    return this;
  }

  @Override
  public boolean removeNodeKey(long nodeKey) {
    boolean containsNodeKey = nodeKeys.contains(nodeKey);
    nodeKeys.removeLong(nodeKey);
    return containsNodeKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeKeys);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final NodeReferences refs) {
      return nodeKeys.equals(refs.nodeKeys);
    }
    return false;
  }

  @Override
  public String toString() {
    final ToStringHelper helper = ToStringHelper.of(this);
    final LongIterator iterator = nodeKeys.getLongIterator();
    while (iterator.hasNext()) {
      final var nodeKey = iterator.next();
      helper.add("referenced node key", nodeKey);
    }
    return helper.toString();
  }

  @Override
  public boolean hasNodeKeys() {
    return !nodeKeys.isEmpty();
  }

  @Override
  public boolean contains(long nodeKey) {
    return nodeKeys.contains(nodeKey);
  }
}
