package io.sirix.axis;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.utils.AbstractComputingIterator;

/**
 * TemporalAxis abstract class.
 *
 * <p>Implements {@link AutoCloseable} so consumers (typically a wrapping
 * {@code TemporalSirix*Stream}) can release any resources the axis holds — most
 * importantly, prefetched read-only transactions held by look-ahead variants. The
 * default {@link #close()} is a no-op for axes that hold no per-iteration resources.
 *
 * @author Johannes Lichtenberger
 */
public abstract class AbstractTemporalAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractComputingIterator<R> implements AutoCloseable {

  public abstract ResourceSession<R, W> getResourceSession();

  /**
   * Release any resources this axis holds. Safe to call multiple times. The default
   * implementation does nothing; look-ahead axes override to cancel pending prefetches.
   */
  @Override
  public void close() {
    // no-op default
  }
}
