package org.sirix.axis.temporal;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.AbstractTemporalAxis;

import static java.util.Objects.requireNonNull;

/**
 * Retrieve a node by node key in all revisions. In each revision a {@link XmlNodeReadOnlyTrx} is
 * opened which is moved to the node with the given node key if it exists. Otherwise the iterator
 * has no more elements (the {@link XmlNodeReadOnlyTrx} moved to the node by it's node key).
 *
 * @author Johannes Lichtenberger
 *
 */
public final class AllTimeAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** The revision number. */
  private int revision;

  /** Sirix {@link ResourceSession}. */
  private final ResourceSession<R, W> resourceSession;

  /** Node key to lookup and retrieve. */
  private final long nodeKey;

  /** Determines if node has been found before and now has been deleted. */
  private boolean hasMoved;

  /**
   * Constructor.
   *
   * @param resourceSession the resource manager
   * @param rtx the read only transactional cursor
   */
  public AllTimeAxis(final ResourceSession<R, W> resourceSession, final R rtx) {
    this.resourceSession = requireNonNull(resourceSession);
    revision = 1;
    nodeKey = rtx.getNodeKey();
  }

  @Override
  protected R computeNext() {
    while (revision <= resourceSession.getMostRecentRevisionNumber()) {
      final R rtx = resourceSession.beginNodeReadOnlyTrx(revision);
      revision++;
      if (rtx.moveTo(nodeKey)) {
        hasMoved = true;
        return rtx;
      } else if (hasMoved) {
        rtx.close();
        return endOfData();
      }
    }

    return endOfData();
  }

  @Override
  public ResourceSession<R, W> getResourceManager() {
    return resourceSession;
  }
}
