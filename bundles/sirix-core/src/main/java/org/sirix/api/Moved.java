package org.sirix.api;

import static com.google.common.base.Preconditions.checkNotNull;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;

/**
 * Determines that a {@link NodeCursor} has been moved.
 *
 * @author Johannes Lichtenberger
 *
 * @param <T> the cursor instance
 */
public final class Moved<T extends NodeCursor> extends Move<T> {

  /** {@link NodeCursor} implementation. */
  private final T nodeCursor;

  /**
   * Constructor.
   *
   * @param nodeCursor the cursor which has been moved
   */
  public Moved(final T nodeCursor) {
    this.nodeCursor = checkNotNull(nodeCursor);
  }

  @Override
  public boolean hasMoved() {
    return true;
  }

  @Override
  public T trx() {
    return nodeCursor;
  }

  @Override
  public boolean equals(final @Nullable Object object) {
    if (this == object)
      return true;

    if (!(object instanceof Moved))
      return false;

    final Moved<?> other = (Moved<?>) object;
    return nodeCursor.equals(other.nodeCursor);
  }

  @Override
  public int hashCode() {
    return 0x598df91c + nodeCursor.hashCode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("cursor", nodeCursor).toString();
  }
}
