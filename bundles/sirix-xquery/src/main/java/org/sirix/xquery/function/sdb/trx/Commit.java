package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;

/**
 * <p>
 * Function for commiting a new revision. The result is the new commited revision number. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>sdb:commit($doc as xs:structured-item) as xs:int</code></li>
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
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

    if (doc.getTrx() instanceof NodeTrx) {
      final NodeTrx wtx = (NodeTrx) doc.getTrx();
      final long revision = wtx.getRevisionNumber();
      wtx.commit();
      return new Int64(revision);
    } else {
      final ResourceManager<?, ?> manager = doc.getTrx().getResourceManager();
      boolean newTrxOpened = false;
      NodeTrx wtx = null;
      try {
        if (manager.getNodeTrx().isPresent()) {
          wtx = manager.getNodeTrx().get();
        } else {
          newTrxOpened = true;
          wtx = manager.beginNodeTrx();
        }
        final int revision = doc.getTrx().getRevisionNumber();
        if (revision < manager.getMostRecentRevisionNumber()) {
          wtx.revertTo(doc.getTrx().getRevisionNumber());
        }
        final int revisionToCommit = wtx.getRevisionNumber();
        wtx.commit();
        return new Int64(revisionToCommit);
      } finally {
        if (newTrxOpened && wtx != null) {
          wtx.close();
        }
      }
    }
  }
}
