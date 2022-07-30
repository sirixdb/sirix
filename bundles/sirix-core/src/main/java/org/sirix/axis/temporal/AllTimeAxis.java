package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.AbstractTemporalAxis;

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

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<R, W> resourceManager;

  /** Node key to lookup and retrieve. */
  private long nodeKey;

  /** Determines if node has been found before and now has been deleted. */
  private boolean hasMoved;

  /**
   * Constructor.
   *
   * @param resourceManager the resource manager
   * @param rtx the read only transactional cursor
   */
  public AllTimeAxis(final ResourceManager<R, W> resourceManager, final R rtx) {
    this.resourceManager = checkNotNull(resourceManager);
    revision = 1;
    nodeKey = rtx.getNodeKey();
  }

  @Override
  protected R computeNext() {
    while (revision <= resourceManager.getMostRecentRevisionNumber()) {
      final R rtx = resourceManager.beginNodeReadOnlyTrx(revision);
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
  public ResourceManager<R, W> getResourceManager() {
    return resourceManager;
  }
}
