package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for rolling back a new revision. The result is the aborted revision number. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>sdb:rollback($doc as xs:node) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class Rollback extends AbstractFunction {

  /** Rollback function name. */
  public final static QNm ROLLBACK = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "rollback");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Rollback(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    final DBNode doc = ((DBNode) args[0]);

    if (doc.getTrx() instanceof XdmNodeWriteTrx) {
      final XdmNodeWriteTrx wtx = (XdmNodeWriteTrx) doc.getTrx();
      final long revision = wtx.getRevisionNumber();
      wtx.rollback();
      return new Int64(revision);
    } else {
      throw new QueryException(new QNm("The transaction is not a write transaction!"));
    }
  }
}
