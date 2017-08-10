package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Open the last revision and try to move to the node with the given node key.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class LastAxis extends AbstractTemporalAxis {

	/** Sirix {@link ResourceManager}. */
	private final ResourceManager mSession;

	/** Node key to lookup and retrieve. */
	private long mNodeKey;

	/** Sirix {@link XdmNodeReadTrx}. */
	private XdmNodeReadTrx mRtx;

	/** Determines if it's the first call. */
	private boolean mFirst;

	/**
	 * Constructor.
	 * 
	 * @param rtx Sirix {@link XdmNodeReadTrx}
	 */
	public LastAxis(final XdmNodeReadTrx rtx) {
		mSession = checkNotNull(rtx.getResourceManager());
		mNodeKey = rtx.getNodeKey();
		mFirst = true;
	}

	@Override
	protected XdmNodeReadTrx computeNext() {
		if (mFirst) {
			mFirst = false;
			mRtx = mSession.beginNodeReadTrx(mSession.getMostRecentRevisionNumber());
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
