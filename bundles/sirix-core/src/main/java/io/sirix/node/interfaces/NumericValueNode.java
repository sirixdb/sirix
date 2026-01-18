package io.sirix.node.interfaces;

/**
 * Interface for nodes that hold a numeric value.
 * Implemented by both NumberNode (array element) and ObjectNumberNode (object value).
 */
public interface NumericValueNode extends Node {

  /**
   * Get the numeric value.
   * @return the numeric value
   */
  Number getValue();

  /**
   * Set the numeric value.
   * @param value the numeric value to set
   */
  void setValue(Number value);
}
