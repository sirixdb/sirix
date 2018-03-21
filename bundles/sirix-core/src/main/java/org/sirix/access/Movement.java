package org.sirix.access;

/**
 * Determines movement after {@code attribute}- or {@code namespace}-insertions.
 */
public enum Movement {
  /** Move to parent element node. */
  TOPARENT,

  /** Do not move. */
  NONE
}
