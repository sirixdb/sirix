package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.exception.SirixException;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Retrieve a node by node key in all revisions. In each revision a
 * {@link NodeReadTrx} is opened which is moved to the node with the given node
 * key if it exists. Otherwise the iterator has no more elements (the
 * {@link NodeReadTrx} moved to the node by it's node key).
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class AllTimeAxis extends AbstractTemporalAxis {

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(AllTimeAxis.class));

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
	 * @param session
	 *          {@link Sirix} session
	 * @param nodeKey
	 *          the key of the node to lookup in each revision
	 */
	public AllTimeAxis(final NodeReadTrx rtx) {
		mSession = checkNotNull(rtx.getSession());
		mRevision = 1;
		mNodeKey = rtx.getNodeKey();
	}

	@Override
	protected NodeReadTrx computeNext() {
		if (mRevision <= mSession.getLastRevisionNumber()) {
			try {
				mRtx = mSession.beginNodeReadTrx(mRevision++);
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
