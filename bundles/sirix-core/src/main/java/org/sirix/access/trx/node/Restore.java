package org.sirix.access.trx.node;

/** Determines if a log must be replayed or not. */
public enum Restore {
  /** Yes, it must be replayed. */
  YES,

  /** No, it must not be replayed. */
  NO
}
