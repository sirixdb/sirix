package org.sirix.axis.temporal;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.axis.AbstractTemporalAxis;

import static java.util.Objects.requireNonNull;

/**
 * Open the first revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FirstAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** Sirix {@link ResourceSession}. */
  private final ResourceSession<R, W> resourceSession;

  /** Node key to lookup and retrieve. */
  private final long nodeKey;

  /** Determines if it's the first call. */
  private boolean first;

  /**
   * Constructor.
   *
   * @param resourceSession the resource manager
   * @param rtx the transactional cursor
   */
  public FirstAxis(final ResourceSession<R, W> resourceSession, final R rtx) {
    this.resourceSession = requireNonNull(resourceSession);
    nodeKey = rtx.getNodeKey();
    first = true;
  }

  @Override
  protected R computeNext() {
    if (first) {
      first = false;
      final R rtx = resourceSession.beginNodeReadOnlyTrx(1);
      if (rtx.moveTo(nodeKey)) {
        return rtx;
      } else {
        rtx.close();
        return endOfData();
      }
    } else {
      return endOfData();
    }
  }

  @Override
  public ResourceSession<R, W> getResourceManager() {
    return resourceSession;
  }
}
