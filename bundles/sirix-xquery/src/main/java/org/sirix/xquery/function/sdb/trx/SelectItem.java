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
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonItemFactory;
import org.sirix.xquery.node.XmlDBNode;

/**
 * <p>
 * Function for selecting a node denoted by its node key. The first parameter is the context node.
 * Supported signature is:
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
   * @param name the name of the function
   * @param signature the signature of the function
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
