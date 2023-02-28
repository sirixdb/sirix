package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonItemFactory;
import org.sirix.xquery.node.XmlDBNode;

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
