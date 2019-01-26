package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Open the previous revision and try to move to the node with the given node key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class PreviousAxis<R extends NodeReadTrx> extends AbstractTemporalAxis<R> {

  /** Determines if it's the first call. */
  private boolean mFirst;

  /** The revision number. */
  private int mRevision;

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> mResourceManager;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /** Sirix {@link NodeReadTrx}. */
  private R mRtx;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link NodeReadTrx}
   */
  public PreviousAxis(final R rtx) {
    mResourceManager = checkNotNull(rtx.getResourceManager());
    mRevision = 0;
    mNodeKey = rtx.getNodeKey();
    mRevision = rtx.getRevisionNumber() - 1;
    mFirst = true;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected R computeNext() {
    if (mRevision > 0 && mFirst) {
      mFirst = false;
      mRtx = (R) mResourceManager.beginReadOnlyTrx(mRevision);
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
