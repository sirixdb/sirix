package org.sirix.xquery.function.sdb;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.node.DBCollection;

/**
 * <p>
 * Function for opening a document in a collection/database. If successful, this
 * function returns the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li>
 * <code>sdb:doc($coll as xs:string, $res as xs:string, $revision as xs:int?) as node()</code>
 * </li>
 * <li>
 * <code>sdb:doc($coll as xs:string, $res as xs:string) as node()</code></li>
 * </ul>
 * 
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 * 
 */
public final class Doc extends AbstractFunction {

	/** CAS index function name. */
	public final static QNm DOC = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX,
			"doc");

	/**
	 * Constructor.
	 * 
	 * @param name
	 *          the name of the function
	 * @param signature
	 *          the signature of the function
	 */
	public Doc(QNm name, Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
			throws QueryException {
		if (args.length != 2 && args.length != 3) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}
		final DBCollection col = (DBCollection) ctx.getStore().lookup(
				((Str) args[0]).stringValue());

		if (col == null) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final String expResName = ((Str) args[1]).stringValue();

		final int revision = args.length == 3 ? FunUtil.getInt(args, 2, "revision",
				-1, null, false) : -1;
		return col.getDocument(revision, expResName);
	}
}
