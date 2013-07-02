package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkNotNull;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.exception.SirixException;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Open the last revision and try to move to the node with the given node key.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class LastAxis extends AbstractTemporalAxis {
	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(LastAxis.class));

	/** Sirix {@link Session}. */
	private final Session mSession;

	/** Node key to lookup and retrieve. */
	private long mNodeKey;

	/** Sirix {@link NodeReadTrx}. */
	private NodeReadTrx mRtx;

	/** Determines if it's the first call. */
	private boolean mFirst;

	/**
	 * Constructor.
	 * 
	 * @param rtx
	 *          Sirix {@link NodeReadTrx}
	 */
	public LastAxis(final NodeReadTrx rtx) {
		mSession = checkNotNull(rtx.getSession());
		mNodeKey = rtx.getNodeKey();
		mFirst = true;
	}

	@Override
	protected NodeReadTrx computeNext() {
		if (mFirst) {
			mFirst = false;
			try {
				mRtx = mSession.beginNodeReadTrx(mSession.getMostRecentRevisionNumber());
			} catch (final SirixException e) {
				LOGGER.error(e.getMessage(), e);
			}
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
