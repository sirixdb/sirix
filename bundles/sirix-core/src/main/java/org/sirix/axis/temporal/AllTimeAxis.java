package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Retrieve a node by node key in all revisions. In each revision a {@link XdmNodeReadTrx} is opened
 * which is moved to the node with the given node key if it exists. Otherwise the iterator has no
 * more elements (the {@link XdmNodeReadTrx} moved to the node by it's node key).
 *
 * @author Johannes Lichtenberger
 *
 */
public final class AllTimeAxis extends AbstractTemporalAxis {

  /** The revision number. */
  private int mRevision;

  /** Sirix {@link ResourceManager}. */
  private final ResourceManager mSession;

  /** Node key to lookup and retrieve. */
  private long mNodeKey;

  /** Sirix {@link XdmNodeReadTrx}. */
  private XdmNodeReadTrx mRtx;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link XdmNodeReadTrx}
   */
  public AllTimeAxis(final XdmNodeReadTrx rtx) {
    mSession = checkNotNull(rtx.getResourceManager());
    mRevision = 1;
    mNodeKey = rtx.getNodeKey();
  }

  @Override
  protected XdmNodeReadTrx computeNext() {
    while (mRevision <= mSession.getMostRecentRevisionNumber()) {
      mRtx = mSession.beginNodeReadTrx(mRevision++);
      if (mRtx.moveTo(mNodeKey).hasMoved())
        return mRtx;
    }

    return endOfData();
  }

  @Override
  public XdmNodeReadTrx getTrx() {
    return mRtx;
  }
}
