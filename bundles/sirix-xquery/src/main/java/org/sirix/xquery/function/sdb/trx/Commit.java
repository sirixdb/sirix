package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.XmlDBNode;

/**
 * <p>
 * Function for commiting a new revision. The result is the new commited revision number. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>sdb:commit($doc as xs:node) as xs:int</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 *
 */
public final class Commit extends AbstractFunction {

  /** Get most recent revision function name. */
  public final static QNm COMMIT = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "commit");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Commit(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    final XmlDBNode doc = ((XmlDBNode) args[0]);

    if (doc.getTrx() instanceof XmlNodeTrx) {
      final XmlNodeTrx wtx = (XmlNodeTrx) doc.getTrx();
      final long revision = wtx.getRevisionNumber();
      wtx.commit();
      return new Int64(revision);
    } else {
      final XmlResourceManager manager = doc.getTrx().getResourceManager();
      final XmlNodeTrx wtx;
      if (manager.hasRunningNodeWriteTrx()) {
        wtx = manager.getNodeWriteTrx().get();
      } else {
        wtx = manager.beginNodeTrx();
      }
      final int revision = doc.getTrx().getRevisionNumber();
      if (revision < manager.getMostRecentRevisionNumber()) {
        wtx.revertTo(doc.getTrx().getRevisionNumber());
      }
      wtx.commit();
      return new Int64(revision);
    }
  }
}
