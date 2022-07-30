package org.sirix.xquery.function.jn.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonItemFactory;

/**
 * <p>
 * Function for selecting a node denoted by its node key. The first parameter is the context node.
 * Supported signature is:
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
   * @param name the name of the function
   * @param signature the signature of the function
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
