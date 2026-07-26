package io.sirix.query.json;

import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.ChildAxis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.LongConsumer;

/**
 * Install-point for the child axis used to scan a JSON array's element children (the path brackit
 * takes for {@code for $x in jn:doc('db','res')[]} — array unbox materializes the element list once
 * via {@link AbstractJsonDBArray}'s {@code values} cache). By default this is the standard
 * {@link ChildAxis}, so behaviour is unchanged. A storage backend that can do better — notably the
 * io_uring enterprise module, whose prefetching child axis overlaps cold page reads with
 * consumption — may {@link #install} a custom factory once at startup; every array scan then routes
 * through it (falling back to {@link ChildAxis} for non-io_uring resources).
 *
 * <p>The factory may return an {@link AutoCloseable} axis (a prefetching axis holds a prefetcher);
 * {@link #forEachChild} owns that lifecycle and always closes it.
 */
public final class JsonScanAxisFactory {

  /** Creates the child axis used to walk an array node's element children. */
  @FunctionalInterface
  public interface Factory {
    Axis createChildAxis(JsonNodeReadOnlyTrx rtx);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonScanAxisFactory.class);

  /** Default: the standard, non-prefetching child axis. */
  private static volatile Factory factory = ChildAxis::new;

  private JsonScanAxisFactory() {
  }

  /**
   * Install a custom child-axis factory for array scans (e.g. an io_uring prefetching axis).
   * Passing {@code null} restores the default {@link ChildAxis}.
   */
  public static void install(final Factory newFactory) {
    factory = newFactory == null ? ChildAxis::new : newFactory;
  }

  /** Create the installed factory's child axis for {@code rtx} (positioned on the array node). */
  public static Axis childAxis(final JsonNodeReadOnlyTrx rtx) {
    return factory.createChildAxis(rtx);
  }

  /**
   * Walk an array node's element children with the installed factory's axis, invoking
   * {@code consumer} for each child (the cursor is positioned on the child before the call), then
   * closing the axis if it holds resources.
   */
  public static void forEachChild(final JsonNodeReadOnlyTrx rtx, final LongConsumer consumer) {
    final Axis axis = factory.createChildAxis(rtx);
    try {
      while (axis.hasNext()) {
        consumer.accept(axis.nextLong());
      }
    } finally {
      if (axis instanceof AutoCloseable closeable) {
        try {
          closeable.close();
        } catch (final Exception e) {
          LOGGER.warn("Failed to close scan axis", e);
        }
      }
    }
  }
}
