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
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;
import io.sirix.query.node.XmlDBNode;

/**
 * <p>
 * Function for getting the parent of an item. The result is the parent item.
 * Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:get-parent($doc as xs:structured-item) as xs:structured-item</code></li>
 * </ul>
 *
 * @author Moshe Uminer
 */

public final class SelectParent extends AbstractFunction {

	public final static QNm SELECT_PARENT = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "select-parent");

	public SelectParent(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		final StructuredDBItem<?> item = ((StructuredDBItem<?>) args[0]);
		final NodeReadOnlyTrx rtx = item.getTrx();

		if (rtx instanceof XmlNodeReadOnlyTrx) {
			if (((XmlNodeReadOnlyTrx) rtx).moveToParent()) {
				return new XmlDBNode((XmlNodeReadOnlyTrx) rtx, ((XmlDBNode) item).getCollection());
			}
		} else if (rtx instanceof JsonNodeReadOnlyTrx) {
			if (((JsonNodeReadOnlyTrx) rtx).moveToParent()) {
				final JsonDBItem jsonItem = (JsonDBItem) item;
				return new JsonItemFactory().getSequence((JsonNodeReadOnlyTrx) rtx, jsonItem.getCollection());
			}
		}

		throw new QueryException(new QNm("Couldn't select parent node."));
	}
}
