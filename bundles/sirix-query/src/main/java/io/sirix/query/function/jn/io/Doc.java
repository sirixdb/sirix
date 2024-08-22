package io.sirix.query.function.jn.io;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.JsonDBCollection;

/**
 * <p>
 * Function for opening a document in a collection/database. If successful, this
 * function returns the document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>jn:doc($coll as xs:string, $res as xs:string, $revision as xs:int?) as json-item()</code>
 * </li>
 * <li><code>jn:doc($coll as xs:string, $res as xs:string) as json-item()</code></li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class Doc extends AbstractFunction {

	/** Doc function name. */
	public final static QNm DOC = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "doc");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public Doc(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		if (args.length < 2 || args.length > 4) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final JsonDBCollection col = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

		if (col == null) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final String expResName = ((Str) args[1]).stringValue();
		final int revision = FunUtil.getInt(args, 2, "revision", -1, null, false);

		return col.getDocument(expResName, revision);
	}
}
