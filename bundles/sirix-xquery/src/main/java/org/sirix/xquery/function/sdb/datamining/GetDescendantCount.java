package org.sirix.xquery.function.sdb.datamining;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for retrieving the number of descendants of the current node.
 * Supported signature is:
 * </p>
 * <ul>
 * <li>
 * <code>sdb:get-descendant-count($doc as xs:node) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetDescendantCount extends AbstractFunction {

	/** Get number of descendants function name. */
	public final static QNm GET_DESCENDANT_COUNT = new QNm(SDBFun.SDB_NSURI,
			SDBFun.SDB_PREFIX, "get-descendant-count");

	/**
	 * Constructor.
	 *
	 * @param name
	 *          the name of the function
	 * @param signature
	 *          the signature of the function
	 */
	public GetDescendantCount(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
			throws QueryException {
		final DBNode doc = ((DBNode) args[0]);

		return new Int64(doc.getTrx().getDescendantCount());
	}
}
