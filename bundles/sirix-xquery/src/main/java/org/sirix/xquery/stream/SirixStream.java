package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.xquery.node.AbsTemporalNode;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;

/**
 * Stream wrapping a Sirix {@link Axis}.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class SirixStream implements Stream<DBNode> {
	/** Sirix {@link Axis}. */
	private final Axis mAxis;

	/** {@link DBCollection} the nodes belong to. */
	private final DBCollection<? extends AbsTemporalNode> mCollection;

	/** Sirix {@link NodeReadTrx}. */
	private final NodeReadTrx mRtx;

	/**
	 * Constructor.
	 * 
	 * @param pAxis
	 *          Sirix {@link Axis}
	 * @param pWtx
	 *          optional {@link NodeWriteTrx}
	 * @param pCollection
	 *          {@link DBCollection} the nodes belong to
	 */
	public SirixStream(final @Nonnull Axis pAxis,
			final @Nonnull DBCollection<? extends AbsTemporalNode> pCollection) {
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
