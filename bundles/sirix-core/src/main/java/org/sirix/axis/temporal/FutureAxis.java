package org.sirix.axis.temporal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Retrieve a node by node key in all future revisions. In each revision a
 * {@link NodeReadTrx} is opened which is moved to the node with the given node
 * key if it exists. Otherwise the iterator has no more elements (the
 * {@link NodeReadTrx} moved to the node by it's node key).
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class FutureAxis extends AbstractTemporalAxis {

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(FutureAxis.class));

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
	public FutureAxis(final Session session,
			final @Nonnegative long nodeKey, final @Nonnegative int revision) {
		// Using telescope pattern instead of builder (only one optional parameter).
		this(session, nodeKey, revision, IncludeSelf.NO);
	}

	/**
	 * Constructor.
	 * 
	 * @param session
	 *          {@link Sirix} session
	 * @param nodeKey
	 *          the key of the node to lookup in each revision
	 * @param revision
	 *          current revision
	 * @param includeSelf
	 * 					determines if current revision must be included or not
	 */
	public FutureAxis(final Session session,
			final @Nonnegative long nodeKey, final @Nonnegative int revision,
			final IncludeSelf includeSelf) {
		mSession = checkNotNull(session);
		mRevision = 0;
		checkArgument(nodeKey > -1, "nodeKey must be >= 0!");
		mNodeKey = nodeKey;
		checkArgument(revision > -1, "revision must be >= 0!");
		mRevision = checkNotNull(includeSelf) == IncludeSelf.YES ? revision
				: revision + 1;
	}

	@Override
	protected NodeReadTrx computeNext() {
		// != a little bit faster?
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
