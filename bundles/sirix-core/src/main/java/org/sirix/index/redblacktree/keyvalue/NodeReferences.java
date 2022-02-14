package org.sirix.index.redblacktree.keyvalue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.index.redblacktree.interfaces.References;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Text node-ID references.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NodeReferences implements References {
  /** A {@link Set} of node-keys. */
  private final Set<Long> nodeKeys;

  /**
   * Default constructor.
   */
  public NodeReferences() {
    nodeKeys = new HashSet<>();
  }

  /**
   * Constructor.
   *
   * @param nodeKeys node keys
   */
  public NodeReferences(final Set<Long> nodeKeys) {
    assert nodeKeys != null;
    this.nodeKeys = new HashSet<>(nodeKeys);
  }

  @Override
  public boolean isPresent(final @NonNegative long nodeKey) {
    return nodeKeys.contains(nodeKey);
  }

  @Override
  public Set<Long> getNodeKeys() {
    return Collections.unmodifiableSet(nodeKeys);
  }

  @Override
  public NodeReferences addNodeKey(final @NonNegative long nodeKey) {
    nodeKeys.add(nodeKey);
    return this;
  }

  @Override
  public boolean removeNodeKey(@NonNegative long nodeKey) {
    return nodeKeys.remove(nodeKey);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKeys);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof NodeReferences) {
      final NodeReferences refs = (NodeReferences) obj;
      return nodeKeys.equals(refs.nodeKeys);
    }
    return false;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (final long nodeKey : nodeKeys) {
      helper.add("referenced node key", nodeKey);
    }
    return helper.toString();
  }

  @Override
  public boolean hasNodeKeys() {
    return !nodeKeys.isEmpty();
  }

  @Override
  public boolean contains(@NonNegative long nodeKey) {
    return nodeKeys.contains(nodeKey);
  }
}
