package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Open the previous revision and try to move to the node with the given node
 * key.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class PreviousAxis extends AbstractTemporalAxis {

	/** Determines if it's the first call. */
	private boolean mFirst;

	/** The revision number. */
	private int mRevision;

	/** Sirix {@link Session}. */
	private final Session mSession;

	/** Node key to lookup and retrieve. */
	private long mNodeKey;

	/** Sirix {@link NodeReadTrx}. */
	private NodeReadTrx mRtx;

	/**
	 * Constructor.
	 * 
	 * @param rtx
	 *          Sirix {@link NodeReadTrx}
	 */
	public PreviousAxis(final NodeReadTrx rtx) {
		mSession = checkNotNull(rtx.getSession());
		mRevision = 0;
		mNodeKey = rtx.getNodeKey();
		mRevision = rtx.getRevisionNumber() - 1;
		mFirst = true;
	}

	@Override
	protected NodeReadTrx computeNext() {
		if (mRevision > 0 && mFirst) {
			mFirst = false;
			mRtx = mSession.beginNodeReadTrx(mRevision);
			return mRtx.moveTo(mNodeKey).hasMoved() ? mRtx : endOfData();
		} else {
			return endOfData();
		}
	}

	@Override
	public NodeReadTrx getTrx() {
		return mRtx;
	}
}
