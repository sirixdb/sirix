package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.xquery.node.AbsTemporalNode;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;

/**
 * Stream wrapping a Sirix {@link IAxis}.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class SirixStream implements Stream<DBNode> {
	/** Sirix {@link IAxis}. */
	private final IAxis mAxis;

	/** {@link DBCollection} the nodes belong to. */
	private final DBCollection<? extends AbsTemporalNode> mCollection;

	/** Sirix {@link INodeReadTrx}. */
	private final INodeReadTrx mRtx;

	/**
	 * Constructor.
	 * 
	 * @param pAxis
	 *          Sirix {@link IAxis}
	 * @param pWtx
	 *          optional {@link INodeWriteTrx}
	 * @param pCollection
	 *          {@link DBCollection} the nodes belong to
	 */
	public SirixStream(final IAxis pAxis, final DBCollection<? extends AbsTemporalNode> pCollection) {
		mAxis = checkNotNull(pAxis);
		mRtx = mAxis.getTrx();
		mCollection = checkNotNull(pCollection);
	}

	@Override
	public DBNode next() throws DocumentException {
		if (mAxis.hasNext()) {
			mAxis.next();
			return new DBNode(mRtx, mCollection);
		}
		return null;
	}

	@Override
	public void close() {
	}

}
