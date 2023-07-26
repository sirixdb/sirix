package io.sirix.index.redblacktree.interfaces;

import io.sirix.node.interfaces.Node;

/**
 * Immutable RBNode.
 */
public interface ImmutableRBNodeValue<V> extends Node {

  /**
   * Node value.
   * 
   * @return the node value
   */
  V getValue();

  /**
   * Determines node has a left child.
   * 
   * @return {@code true}, if it has a left child, {@code false} otherwise
   */
  boolean hasLeftChild();

  /**
   * Determines node has a right child.
   * 
   * @return {@code true}, if it has a left child, {@code false} otherwise
   */
  boolean hasRightChild();

  /**
   * Get left child.
   * 
   * @return left child pointer
   */
  long getLeftChildKey();

  /**
   * Get right child.
   * 
   * @return right child pointer
   */
  long getRightChildKey();
}
