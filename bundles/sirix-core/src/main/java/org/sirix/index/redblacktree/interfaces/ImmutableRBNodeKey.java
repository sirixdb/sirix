package org.sirix.index.redblacktree.interfaces;

import org.sirix.node.interfaces.Node;

/**
 * Immutable RBNode.
 */
public interface ImmutableRBNodeKey<K extends Comparable<? super K>> extends Node {

  /**
   * Key to be indexed.
   * 
   * @return key reference
   */
  K getKey();

  /**
   * Node key of value node.
   * 
   * @return the node key of the value node
   */
  long getValueNodeKey();

  /**
   * Flag which determines if node is changed.
   * 
   * @return {@code true} if it has been changed in memory, {@code false} otherwise
   */
  boolean isChanged();

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
