package org.sirix.xquery.function.sdb;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for commiting a new revision. The result is the new commited
 * revision number. Supported signature is:
 * </p>
 * <ul>
 * <li>
 * <code>sdb:commit($doc as xs:node) as xs:int</code></li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class Commit extends AbstractFunction {

	/** Get most recent revision function name. */
	public final static QNm COMMIT = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX,
			"commit");

	/**
	 * Constructor.
	 * 
	 * @param name
	 *          the name of the function
	 * @param signature
	 *          the signature of the function
	 */
	public Commit(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
			throws QueryException {
		final DBNode doc = ((DBNode) args[0]);

		if (doc.getTrx() instanceof NodeWriteTrx) {
			final NodeWriteTrx wtx = (NodeWriteTrx) doc.getTrx();
			final long revision = wtx.getRevisionNumber();
			try {
				wtx.commit();
			} catch (final SirixException e) {
				throw new QueryException(new QNm("Couldn't be commited: "
						+ e.getMessage()), e);
			}
			return new Int64(revision);
		} else {
			throw new QueryException(new QNm(
					"The transaction is not a write transaction!"));
		}
	}
}
