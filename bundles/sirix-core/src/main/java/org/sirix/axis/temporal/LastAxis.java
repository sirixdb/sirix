package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Optional;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Open the last revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class LastAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<R, W> resourceManager;

  /** Node key to lookup and retrieve. */
  private long nodeKey;

  /** Determines if it's the first call. */
  private boolean first;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link NodeReadOnlyTrx}
   */
  public LastAxis(final ResourceManager<R, W> resourceManager, final R rtx) {
    this.resourceManager = checkNotNull(resourceManager);
    nodeKey = rtx.getNodeKey();
    first = true;
  }

  @Override
  protected R computeNext() {
    if (first) {
      first = false;

      final Optional<R> optionalRtx =
          resourceManager.getNodeReadTrxByRevisionNumber(resourceManager.getMostRecentRevisionNumber());

      final R rtx;
      if (optionalRtx.isPresent()) {
        rtx = optionalRtx.get();
      } else {
        rtx = resourceManager.beginNodeReadOnlyTrx(resourceManager.getMostRecentRevisionNumber());
      }

      if (rtx.moveTo(nodeKey).hasMoved()) {
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
  public ResourceManager<R, W> getResourceManager() {
    return resourceManager;
  }
}
