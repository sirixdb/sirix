package org.sirix.index.redblacktree.interfaces;

/**
 * Mutable RBNode.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface MutableRBNode<K extends Comparable<? super K>, V> extends ImmutableRBNode<K, V> {
  /**
   * Set the key.
   * 
   * @param key key to set
   */
  void setKey(K key);

  /**
   * Set the value.
   * 
   * @param value value to set
   */
  void setValue(V value);

  /**
   * Set left child.
   * 
   * @param left child pointer
   */
  void setLeftChildKey(long left);

  /**
   * Set right child.
   * 
   * @param right child pointer
   */
  void setRightChildKey(long right);

  /**
   * Flag which determines if node is changed.
   * 
   * @param changed flag which indicates if node is changed or not
   */
  public void setChanged(boolean changed);
}
