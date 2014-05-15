package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.NodeReadTrx;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for getting a path. The result is the path. Supported signature is:
 * </p>
 * <ul>
 * <li>
 * <code>sdb:getPath($doc as xs:node) as xs:string</code></li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class GetPath extends AbstractFunction {

	/** Move to function name. */
	public final static QNm GET_PATH = new QNm(SDBFun.SDB_NSURI,
			SDBFun.SDB_PREFIX, "getPath");

	/**
	 * Constructor.
	 * 
	 * @param name
	 *          the name of the function
	 * @param signature
	 *          the signature of the function
	 */
	public GetPath(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
			throws QueryException {
		final DBNode doc = ((DBNode) args[0]);

		final NodeReadTrx rtx = doc.getTrx();

		if (rtx.getSession().getResourceConfig().mPathSummary) {
			try (final PathSummaryReader pathSummaryReader = rtx.getSession()
					.openPathSummary(rtx.getRevisionNumber())) {
				pathSummaryReader.moveTo(rtx.getPathNodeKey());
				return new Str(pathSummaryReader.getPathNode()
						.getPath(pathSummaryReader).toString());
			}
		}
		return null;
	}
}
