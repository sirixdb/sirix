package org.sirix.api;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.NoSuchElementException;

/**
 * Determines that a {@link NodeCursor} hasn't moved to the node.
 *
 * @author Johannes Lichtenberger
 *
 */
public class NotMoved extends Move<NodeCursor> {
  /** Singleton instance. */
  static final NotMoved INSTANCE = new NotMoved();

  /** Private constructor. */
  private NotMoved() {}

  @Override
  public boolean hasMoved() {
    return false;
  }

  @Override
  public NodeCursor trx() {
    throw new NoSuchElementException("NotMoved.trx() cannot be called if the transaction hasn't moved");
  }

  @Override
  public boolean equals(final @Nullable Object object) {
    return object == this;
  }

  @Override
  public int hashCode() {
    return 0x598df91c;
  }
}
