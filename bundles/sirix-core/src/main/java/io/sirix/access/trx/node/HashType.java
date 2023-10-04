package io.sirix.access.trx.node;

/**
 * How is the Hash for this storage computed?
 */
public enum HashType {
  /** Rolling hash, only nodes on ancestor axis are touched. */
  ROLLING,
  /**
   * Postorder hash, all nodes on ancestor plus postorder are at least read.
   */
  POSTORDER,
  /** No hash structure after all. */
  NONE;
}
