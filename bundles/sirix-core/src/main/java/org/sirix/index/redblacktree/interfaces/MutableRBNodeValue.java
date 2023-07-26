package org.sirix.index.redblacktree.interfaces;

/**
 * Mutable RBNode.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface MutableRBNodeValue<V> extends ImmutableRBNodeValue<V> {
  /**
   * Set the value.
   * 
   * @param value value to set
   */
  void setValue(V value);
}
