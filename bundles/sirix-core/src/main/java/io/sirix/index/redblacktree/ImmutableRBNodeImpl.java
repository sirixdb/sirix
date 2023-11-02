package io.sirix.index.redblacktree;

import io.sirix.index.redblacktree.interfaces.ImmutableRBNodeKey;
import io.sirix.node.AbstractForwardingNode;
import io.sirix.node.NodeKind;

import io.sirix.node.delegates.NodeDelegate;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Immutable RBNode.
 *
 * @author Johannes Lichtenberger
 *
 * @param <K> key which has to be comparable (implement Comparable interface)
 */
public final class ImmutableRBNodeImpl<K extends Comparable<? super K>> extends AbstractForwardingNode
    implements ImmutableRBNodeKey<K> {

  /** {@link RBNodeKey} to wrap. */
  private final RBNodeKey<K> node;

  /**
   * Constructor.
   *
   * @param node {@link RBNodeKey} to wrap.
   */
  public ImmutableRBNodeImpl(final RBNodeKey<K> node) {
    assert node != null;
    this.node = node;
  }

  @Override
  public NodeKind getKind() {
    return node.getKind();
  }

  @Override
  public K getKey() {
    return node.getKey();
  }

  @Override
  public long getValueNodeKey() {
    return node.getValueNodeKey();
  }

  @Override
  public boolean isChanged() {
    return node.isChanged();
  }

  @Override
  public boolean hasLeftChild() {
    return node.hasLeftChild();
  }

  @Override
  public boolean hasRightChild() {
    return node.hasRightChild();
  }

  @Override
  public long getLeftChildKey() {
    return node.getLeftChildKey();
  }

  @Override
  public long getRightChildKey() {
    return node.getRightChildKey();
  }

  @Override
  protected @NonNull NodeDelegate delegate() {
    return node.delegate();
  }
}
