package io.sirix.node.interfaces;

/**
 * Interface for nodes that hold a boolean value.
 * Implemented by both BooleanNode (array element) and ObjectBooleanNode (object value).
 */
public interface BooleanValueNode extends Node {

  /**
   * Get the boolean value.
   * @return the boolean value
   */
  boolean getValue();

  /**
   * Set the boolean value.
   * @param value the boolean value to set
   */
  void setValue(boolean value);
}
