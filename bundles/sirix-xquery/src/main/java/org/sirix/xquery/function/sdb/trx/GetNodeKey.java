package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for getting the nodeKey of the node. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:getNodeKey($doc as xs:node) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetNodeKey extends AbstractFunction {

  /** GetNodeKey function name. */
  public final static QNm GET_NODEKEY = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "getNodeKey");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetNodeKey(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    final DBNode doc = ((DBNode) args[0]);

    return new Int64(doc.getNodeKey());
  }
}
