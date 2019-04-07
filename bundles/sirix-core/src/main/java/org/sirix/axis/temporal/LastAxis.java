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
  private final ResourceManager<R, W> mResourceManager;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /** Determines if it's the first call. */
  private boolean mFirst;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link NodeReadOnlyTrx}
   */
  public LastAxis(final ResourceManager<R, W> resourceManager, final R rtx) {
    mResourceManager = checkNotNull(resourceManager);
    mNodeKey = rtx.getNodeKey();
    mFirst = true;
  }

  @Override
  protected R computeNext() {
    if (mFirst) {
      mFirst = false;

      final Optional<R> optionalRtx =
          mResourceManager.getNodeReadTrxByRevisionNumber(mResourceManager.getMostRecentRevisionNumber());

      final R rtx;
      if (optionalRtx.isPresent()) {
        rtx = optionalRtx.get();
      } else {
        rtx = mResourceManager.beginNodeReadOnlyTrx(mResourceManager.getMostRecentRevisionNumber());
      }

      if (rtx.moveTo(mNodeKey).hasMoved()) {
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
    return mResourceManager;
  }
}
