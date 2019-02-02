package org.sirix.access.trx.node;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.sirix.api.NodeCursor;

/**
 * Determines if the {@link NodeCursor} moved to a node or not. Based on the idea of providing a
 * wrapper just like in Google Guava's {@link Optional} class.
 *
 * @author Johannes Lichtenberger
 *
 * @param <T> type parameter, the cursor
 */
public abstract class Move<T extends NodeCursor> {
  /**
   * Returns a {@link Moved} instance with no contained reference.
   */
  @SuppressWarnings("unchecked")
  public static <T extends NodeCursor> Move<T> notMoved() {
    return (Move<T>) NotMoved.INSTANCE;
  }

  /**
   * Returns a {@code Moved} instance containing the given non-null reference.
   */
  public static <T extends NodeCursor> Moved<T> moved(final T moved) {
    return new Moved<T>(checkNotNull(moved));
  }

  /**
   * Determines if the cursor has moved.
   *
   * @return {@code true} if it has moved, {@code false} otherwise
   */
  public abstract boolean hasMoved();

  /**
   * Get the cursor reference.
   *
   * @return cursor reference
   * @throws NoSuchElementException if the cursor couldn't be moved
   */
  public abstract T getCursor();
}
