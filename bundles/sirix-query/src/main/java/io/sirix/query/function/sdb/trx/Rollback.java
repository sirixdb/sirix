package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.api.NodeTrx;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

/**
 * <p>
 * Function for rolling back a new revision. The result is the aborted revision number. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>sdb:rollback($doc as xs:structured-item) as xs:int</code></li>
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
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> document = ((StructuredDBItem<?>) args[0]);

    if (document.getTrx() instanceof NodeTrx) {
      final NodeTrx wtx = (NodeTrx) document.getTrx();
      final long revision = wtx.getRevisionNumber();
      wtx.rollback();
      return new Int64(revision);
    } else {
      throw new QueryException(new QNm("The transaction is not a write transaction!"));
    }
  }
}
