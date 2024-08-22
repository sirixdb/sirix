package io.sirix.query.function.jn.io;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.json.JsonDBCollection;

/**
 * <p>
 * Function for determining if a database exists or not. Supported signature is:
 * </p>
 * <ul>
 * <li><code>jn:exists-database($coll as xs:string) as xs:boolean</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ExistsDatabase extends AbstractFunction {

	/** Doc function name. */
	public final static QNm EXISTS_DATABASE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "exists-database");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public ExistsDatabase(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		if (args.length != 1) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final JsonDBCollection col = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

		return col != null ? Bool.TRUE : Bool.FALSE;
	}
}
