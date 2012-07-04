package org.sirix.access;

/**
 * How is the Hash for this storage computed?
 */
public enum EHashKind {
  /** Rolling hash, only nodes on ancestor axis are touched. */
  Rolling,
  /**
   * Postorder hash, all nodes on ancestor plus postorder are at least
   * read.
   */
  Postorder,
  /** No hash structure after all. */
  None;
}
