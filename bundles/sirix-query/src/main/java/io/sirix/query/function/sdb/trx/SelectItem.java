package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;
import io.sirix.query.node.XmlDBNode;

/**
 * <p>
 * Function for selecting a node denoted by its node key. The first parameter is
 * the context node. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:select-item($doc as xs:structured-item, $nodeKey as xs:integer) as xs:structured-item</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SelectItem extends AbstractFunction {

	/** Move to function name. */
	public final static QNm SELECT_NODE = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "select-item");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public SelectItem(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		final StructuredDBItem<?> item = ((StructuredDBItem<?>) args[0]);
		final NodeReadOnlyTrx rtx = item.getTrx();
		final long nodeKey = FunUtil.getLong(args, 1, "nodeKey", 0, null, true);

		if (rtx.moveTo(nodeKey)) {
			if (rtx instanceof XmlNodeReadOnlyTrx) {
				return new XmlDBNode((XmlNodeReadOnlyTrx) rtx, ((XmlDBNode) item).getCollection());
			} else if (rtx instanceof JsonNodeReadOnlyTrx) {
				final JsonDBItem jsonItem = (JsonDBItem) item;
				return new JsonItemFactory().getSequence((JsonNodeReadOnlyTrx) rtx, jsonItem.getCollection());
			}
		}

		throw new QueryException(new QNm("Couldn't select node."));
	}
}
