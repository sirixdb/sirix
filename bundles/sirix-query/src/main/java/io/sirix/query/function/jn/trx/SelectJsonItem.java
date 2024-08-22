package io.sirix.query.function.jn.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;

/**
 * <p>
 * Function for selecting a node denoted by its node key. The first parameter is
 * the context node. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:select-node($doc as xs:node, $nodeKey as xs:integer) as xs:node</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SelectJsonItem extends AbstractFunction {

	/** Move to function name. */
	public final static QNm SELECT_JSON_ITEM = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "select-json-item");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public SelectJsonItem(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		final JsonDBItem node = ((JsonDBItem) args[0]);
		final JsonNodeReadOnlyTrx rtx = node.getTrx();
		final long nodeKey = FunUtil.getLong(args, 1, "nodeKey", 0, null, true);

		if (rtx.moveTo(nodeKey)) {
			return new JsonItemFactory().getSequence(rtx, node.getCollection());
		} else {
			throw new QueryException(new QNm("Couldn't select node."));
		}
	}
}
