package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.IncludeSelf;

/**
 * Retrieve a node by node key in all earlier revisions. In each revision a {@link XdmNodeReadTrx}
 * is opened which is moved to the node with the given node key if it exists. Otherwise the iterator
 * has no more elements (the {@link XdmNodeReadTrx} moved to the node by it's node key).
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class PastAxis extends AbstractTemporalAxis {

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
  public PastAxis(final XdmNodeReadTrx rtx) {
    // Using telescope pattern instead of builder (only one optional parameter.
    this(rtx, IncludeSelf.NO);
  }

  /**
   * Constructor.
   * 
   * @param includeSelf determines if current revision must be included or not
   */
  public PastAxis(final XdmNodeReadTrx rtx, final IncludeSelf includeSelf) {
    mSession = checkNotNull(rtx.getResourceManager());
    mRevision = 0;
    mNodeKey = rtx.getNodeKey();
    mRevision = checkNotNull(includeSelf) == IncludeSelf.YES ? rtx.getRevisionNumber()
        : rtx.getRevisionNumber() - 1;
  }

  @Override
  protected XdmNodeReadTrx computeNext() {
    if (mRevision > 0) {
      mRtx = mSession.beginNodeReadTrx(mRevision--);
      return mRtx.moveTo(mNodeKey).hasMoved() ? mRtx : endOfData();
    } else {
      return endOfData();
    }
  }

  @Override
  public XdmNodeReadTrx getTrx() {
    return mRtx;
  }
}
