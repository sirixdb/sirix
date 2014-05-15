package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Bool;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.access.Move;
import org.sirix.api.NodeReadTrx;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for moving a transaction-cursor. The result is either the boolean {@code true} or {@code false}.
 * Supported signature is:
 * </p>
 * <ul>
 * <li>
 * <code>sdb:moveto($doc as xs:node, $nodeKey as xs:) as xs:boolean</code></li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class MoveTo extends AbstractFunction {

	/** Move to function name. */
	public final static QNm MOVE_TO = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX,
			"moveTo");

	/**
	 * Constructor.
	 * 
	 * @param name
	 *          the name of the function
	 * @param signature
	 *          the signature of the function
	 */
	public MoveTo(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
			throws QueryException {
		final DBNode doc = ((DBNode) args[0]);

		final Move<? extends NodeReadTrx> moved = doc.getTrx().moveTo(FunUtil.getLong(args, 1, "", 0 ,null, true));
		
		if (moved.hasMoved()) {
			return Bool.TRUE;
		} else {
			return Bool.FALSE;
		}
	}
}
