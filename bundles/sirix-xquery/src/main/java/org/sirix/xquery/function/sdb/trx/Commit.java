package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBNode;

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
  public Commit(QNm name, Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args)
      throws QueryException {
    final DBNode doc = ((DBNode) args[0]);

    if (doc.getTrx() instanceof XdmNodeWriteTrx) {
      final XdmNodeWriteTrx wtx = (XdmNodeWriteTrx) doc.getTrx();
      final long revision = wtx.getRevisionNumber();
      wtx.commit();
      return new Int64(revision);
    } else {
      final ResourceManager manager = doc.getTrx().getResourceManager();
      final XdmNodeWriteTrx wtx;
      if (manager.getAvailableNodeWriteTrx() == 0) {
        wtx = manager.getXdmNodeWriteTrx().get();
      } else {
        wtx = manager.beginNodeWriteTrx();
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
