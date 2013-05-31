package org.sirix.xquery.stream;

import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.xquery.node.DBNode;

/**
 * Wraps a single node into a {@link Stream}.
 * 
 * @author Johannes Lichtenberger
 */
public final class SingleNodeStream implements Stream<DBNode> {
	/** Determines if it's the first call. */
	private boolean mFirst;

	/** The {@link DBNode} to receive. */
	private final DBNode mNode;

	/**
	 * Constructor.
	 * 
	 * @param node
	 *          the {@link DBNode}
	 */
	public SingleNodeStream(final DBNode node) {
		mNode = node;
		mFirst = true;
	}

	@Override
	public DBNode next() throws DocumentException {
		if (mFirst) {
			mFirst = false;
			return mNode;
		}
		return null;
	}

	@Override
	public void close() {
	}
}
