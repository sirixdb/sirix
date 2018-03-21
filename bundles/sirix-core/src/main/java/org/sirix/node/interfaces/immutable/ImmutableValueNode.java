package org.sirix.node.interfaces.immutable;

/**
 * Immutable value node (for instance text-, attribute-node...).
 * 
 * @author Johannes Lichtenberger
 *
 */
public interface ImmutableValueNode {
  /**
   * Return a byte array representation of the node value.
   * 
   * @return the value of the node
   */
  byte[] getRawValue();

  /**
   * Return the string value of the node.
   * 
   * @return the string value of the node
   */
  String getValue();
}
