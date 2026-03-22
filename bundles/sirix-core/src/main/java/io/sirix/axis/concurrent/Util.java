package io.sirix.axis.concurrent;

import io.sirix.api.Axis;
import io.sirix.settings.Fixed;
import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/** Utilities. */
public final class Util {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(Util.class));

  /**
   * Get next key.
   * 
   * @param axis the {@link Axis}
   * @return the next result of the axis. If the axis has no next result, the null node key is
   *         returned.
   */
  public static long getNext(final Axis axis) {
    return axis.hasNext()
        ? axis.next()
        : Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * Checks, whether the given node key belongs to a node or an atomic value. Returns true for a node
   * and throws an exception for an atomic value, because these are not allowed in the except
   * expression.
   * 
   * @param nodeKey the node key to validate
   * @return {@code true}, if key is a key of a node, {@code false} for atomic values
   */
  public static boolean isValid(final long nodeKey) {
    if (nodeKey < 0) {
      LOGGER.error("err:XPTY0004 Atomic values are not allowed in this expression.");
      return false;
    }
    return true;
  }

}
