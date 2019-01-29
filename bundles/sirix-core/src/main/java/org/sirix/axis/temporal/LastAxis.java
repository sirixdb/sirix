package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Open the last revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class LastAxis<R extends NodeReadTrx> extends AbstractTemporalAxis<R> {

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> mResourceManager;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /** Sirix {@link NodeReadTrx}. */
  private R mRtx;

  /** Determines if it's the first call. */
  private boolean mFirst;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link NodeReadTrx}
   */
  public LastAxis(final R rtx) {
    mResourceManager = checkNotNull(rtx.getResourceManager());
    mNodeKey = rtx.getNodeKey();
    mFirst = true;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected R computeNext() {
    if (mFirst) {
      mFirst = false;
      mRtx = (R) mResourceManager.beginNodeReadOnlyTrx(mResourceManager.getMostRecentRevisionNumber());
      return mRtx.moveTo(mNodeKey).hasMoved()
          ? mRtx
          : endOfData();
    } else {
      return endOfData();
    }
  }

  @Override
  public R getTrx() {
    return mRtx;
  }
}
