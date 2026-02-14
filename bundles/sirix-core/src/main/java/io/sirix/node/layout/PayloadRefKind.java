package io.sirix.node.layout;

/**
 * Classification of out-of-line payload blocks referenced by a fixed slot.
 */
public enum PayloadRefKind {
  VALUE_BLOB,
  ATTRIBUTE_VECTOR,
  NAMESPACE_VECTOR
}
