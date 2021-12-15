package org.sirix.index.redblacktree;

import org.sirix.index.redblacktree.interfaces.ImmutableRBNode;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NodeDelegate;

/**
 * Immutable RBNode.
 *
 * @author Johannes Lichtenberger
 *
 * @param <K> key which has to be comparable (implement Comparable interface)
 * @param <V> value
 */
public final class ImmutableRBNodeImpl<K extends Comparable<? super K>, V> extends AbstractForwardingNode
    implements ImmutableRBNode<K, V> {

  /** {@link RBNode} to wrap. */
  private final RBNode<K, V> node;

  /**
   * Constructor.
   *
   * @param node {@link RBNode} to wrap.
   */
  public ImmutableRBNodeImpl(final RBNode<K, V> node) {
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
  public V getValue() {
    return node.getValue();
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
  protected NodeDelegate delegate() {
    return node.delegate();
  }
}
