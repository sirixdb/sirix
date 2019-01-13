package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Retrieve a node by node key in all revisions. In each revision a {@link XdmNodeReadTrx} is opened
 * which is moved to the node with the given node key if it exists. Otherwise the iterator has no
 * more elements (the {@link XdmNodeReadTrx} moved to the node by it's node key).
 *
 * @author Johannes Lichtenberger
 *
 */
public final class AllTimeAxis<R extends NodeReadTrx> extends AbstractTemporalAxis<R> {

  /** The revision number. */
  private int mRevision;

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> mResourceManager;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /** Sirix {@link NodeReadTrx}. */
  private R mRtx;

  /** Determines if node has been found before and now has been deleted. */
  private boolean mHasMoved;

  /**
   * Determines private boolean mHasMoved;
   *
   * /** Constructor.
   *
   * @param rtx Sirix {@link NodeReadTrx}
   */
  public AllTimeAxis(final R rtx) {
    mResourceManager = checkNotNull(rtx.getResourceManager());
    mRevision = 1;
    mNodeKey = rtx.getNodeKey();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected R computeNext() {
    while (mRevision <= mResourceManager.getMostRecentRevisionNumber()) {
      mRtx = (R) mResourceManager.beginNodeReadTrx(mRevision++);

      if (mRtx.moveTo(mNodeKey).hasMoved()) {
        mHasMoved = true;
        return mRtx;
      } else if (mHasMoved) {
        return endOfData();
      }
    }

    return endOfData();
  }

  @Override
  public R getTrx() {
    return mRtx;
  }
}
