package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

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
public final class SelectNode extends AbstractFunction {

  /** Move to function name. */
  public final static QNm SELECT_NODE = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "select-node");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public SelectNode(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    final DBNode node = ((DBNode) args[0]);
    final XdmNodeReadOnlyTrx rtx = node.getTrx();
    final long nodeKey = FunUtil.getLong(args, 1, "nodeKey", 0, null, true);

    if (rtx.moveTo(nodeKey).hasMoved()) {
      return new DBNode(rtx, node.getCollection());
    } else {
      throw new QueryException(new QNm("Couldn't select node."));
    }
  }
}
