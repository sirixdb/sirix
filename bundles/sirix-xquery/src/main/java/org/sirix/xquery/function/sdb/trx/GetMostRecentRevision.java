package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;

/**
 * <p>
 * Function for getting the most recent revision. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:most-recent-revision($doc as xs:structured-item) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class GetMostRecentRevision extends AbstractFunction {

  /** Get most recent revision function name. */
  public final static QNm MOST_RECENT_REVISION = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "most-recent-revision");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetMostRecentRevision(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

    return new Int32(doc.getTrx().getResourceSession().getMostRecentRevisionNumber());
  }
}
