package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Open the first revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FirstAxis<R extends NodeReadTrx> extends AbstractTemporalAxis<R> {

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> mResourceManager;

  /** Node key to lookup and retrieve. */
  private final long mNodeKey;

  /** Sirix {@link NodeReadTrx}. */
  private R mRtx;

  /** Determines if it's the first call. */
  private boolean mFirst;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link XdmNodeReadOnlyTrx}
   */
  public FirstAxis(final R rtx) {
    mResourceManager = checkNotNull(rtx.getResourceManager());
    mNodeKey = rtx.getNodeKey();
    mFirst = true;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected R computeNext() {
    if (mFirst) {
      mFirst = false;
      mRtx = (R) mResourceManager.beginReadOnlyTrx(1);
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
