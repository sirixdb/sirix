package org.sirix.axis.temporal;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Open the first revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FirstAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<R, W> mResourceManager;

  /** Node key to lookup and retrieve. */
  private final long mNodeKey;

  /** Determines if it's the first call. */
  private boolean mFirst;

  /**
   * Constructor.
   *
   * @param resourceManager the resource manager
   * @param rtx the transactional cursor
   */
  public FirstAxis(final ResourceManager<R, W> resourceManager, final R rtx) {
    mResourceManager = checkNotNull(resourceManager);
    mNodeKey = rtx.getNodeKey();
    mFirst = true;
  }

  @Override
  protected R computeNext() {
    if (mFirst) {
      mFirst = false;
      final Optional<R> optionalRtx = mResourceManager.getNodeReadTrxByRevisionNumber(1);

      final R rtx;
      if (optionalRtx.isPresent()) {
        rtx = optionalRtx.get();
      } else {
        rtx = mResourceManager.beginNodeReadOnlyTrx(1);
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
