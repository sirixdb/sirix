package org.sirix.index.redblacktree.keyvalue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.sirix.index.redblacktree.interfaces.References;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Text node-ID references.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NodeReferences implements References {
  /** A {@link Set} of node-keys. */
  private final Set<Long> mNodeKeys;

  /**
   * Default constructor.
   */
  public NodeReferences() {
    mNodeKeys = new HashSet<>();
  }

  /**
   * Constructor.
   *
   * @param nodeKeys node keys
   */
  public NodeReferences(final Set<Long> nodeKeys) {
    assert nodeKeys != null;
    mNodeKeys = new HashSet<>(nodeKeys);
  }

  @Override
  public boolean isPresent(final @Nonnegative long nodeKey) {
    return mNodeKeys.contains(nodeKey);
  }

  @Override
  public Set<Long> getNodeKeys() {
    return Collections.unmodifiableSet(mNodeKeys);
  }

  @Override
  public NodeReferences addNodeKey(final @Nonnegative long nodeKey) {
    mNodeKeys.add(nodeKey);
    return this;
  }

  @Override
  public boolean removeNodeKey(@Nonnegative long nodeKey) {
    return mNodeKeys.remove(nodeKey);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeKeys);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof NodeReferences) {
      final NodeReferences refs = (NodeReferences) obj;
      return mNodeKeys.equals(refs.mNodeKeys);
    }
    return false;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (final long nodeKey : mNodeKeys) {
      helper.add("referenced node key", nodeKey);
    }
    return helper.toString();
  }

  @Override
  public boolean hasNodeKeys() {
    return !mNodeKeys.isEmpty();
  }

  @Override
  public boolean contains(@Nonnegative long nodeKey) {
    return mNodeKeys.contains(nodeKey);
  }
}
