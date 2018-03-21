package org.sirix.access;

import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.sirix.api.NodeCursor;

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
  public NodeCursor get() {
    throw new NoSuchElementException("NotMoved.get() cannot be called on an absent value");
  }

  @Override
  public boolean equals(final @Nullable Object object) {
    return object == this;
  }

  @Override
  public int hashCode() {
    return 0x598df91c;
  }

  private Object readResolve() {
    return INSTANCE;
  }
}
