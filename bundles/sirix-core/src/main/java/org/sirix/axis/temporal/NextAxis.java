package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Open the next revision and try to move to the node with the given node key.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class NextAxis extends AbstractTemporalAxis {

	/** Determines if it's the first call. */
	private boolean mFirst;

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
	public NextAxis(final XdmNodeReadTrx rtx) {
		mSession = checkNotNull(rtx.getResourceManager());
		mRevision = 0;
		mNodeKey = rtx.getNodeKey();
		mRevision = rtx.getRevisionNumber() + 1;
		mFirst = true;
	}

	@Override
	protected XdmNodeReadTrx computeNext() {
		if (mRevision <= mSession.getMostRecentRevisionNumber() && mFirst) {
			mFirst = false;
			mRtx = mSession.beginNodeReadTrx(mRevision);
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
