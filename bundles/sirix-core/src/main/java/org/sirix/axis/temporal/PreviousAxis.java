package org.sirix.axis.temporal;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.axis.AbstractTemporalAxis;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Open the previous revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class PreviousAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** Sirix {@link ResourceSession}. */
  private final ResourceSession<R, W> resourceSession;

  /** Determines if it's the first call. */
  private boolean first;

  /** The revision number. */
  private int revision;

  /** Node key to lookup and retrieve. */
  private final long nodeKey;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link NodeReadOnlyTrx}
   */
  public PreviousAxis(final ResourceSession<R, W> resourceSession, final R rtx) {
    this.resourceSession = checkNotNull(resourceSession);
    nodeKey = rtx.getNodeKey();
    revision = rtx.getRevisionNumber() - 1;
    first = true;
  }

  @Override
  protected R computeNext() {
    if (revision > 0 && first) {
      first = false;
      final R rtx = resourceSession.beginNodeReadOnlyTrx(revision);
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
