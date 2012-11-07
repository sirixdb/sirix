package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

/**
 * Open the previous revision and try to move to the node with the given node key.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class PreviousAxis extends AbstractIterator<NodeReadTrx> {
	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(AllTimeAxis.class));

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
	 * @param session
	 *          {@link Sirix} session
	 * @param nodeKey
	 *          the key of the node to lookup in each revision
	 * @param revision
	 *          current revision
	 */
	public PreviousAxis(final @Nonnull Session session,
			final @Nonnegative long nodeKey, final @Nonnegative int revision) {
		mSession = checkNotNull(session);
		mRevision = 0;
		checkArgument(nodeKey > -1, "nodeKey must be >= 0!");
		mNodeKey = nodeKey;
		checkArgument(revision > -1, "revision must be >= 0!");
		mRevision = revision - 1;
		mFirst = true;
	}

	@Override
	protected NodeReadTrx computeNext() {
		if (mRevision >= 0 && mFirst) {
			mFirst = false;
			try {
				mRtx = mSession.beginNodeReadTrx(mRevision);
			} catch (final SirixException e) {
				LOGGER.error(e.getMessage(), e);
			}
			return mRtx.moveTo(mNodeKey).hasMoved() ? mRtx : endOfData();
		} else {
			return endOfData();
		}
	}

}
