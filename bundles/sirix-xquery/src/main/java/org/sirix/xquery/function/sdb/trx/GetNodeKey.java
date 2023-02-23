package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;

/**
 * <p>
 * Function for getting the nodeKey of the node. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:nodekey($doc as xs:structured-item()) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetNodeKey extends AbstractFunction {

  /** GetNodeKey function name. */
  public final static QNm GET_NODEKEY = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "nodekey");

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
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

    return new Int64(doc.getNodeKey());
  }
}
