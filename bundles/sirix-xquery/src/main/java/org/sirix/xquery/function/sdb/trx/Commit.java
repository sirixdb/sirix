package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.DateTime;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.DateTimeToInstant;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.sdb.SDBFun;

import java.time.Instant;

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

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

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

    final String commitMessage = args.length >= 2 ? FunUtil.getString(args, 1, "commitMessage", null, null, false) : null;

    final DateTime dateTime = args.length == 3 ? (DateTime) args[2] : null;
    final Instant commitTimesstamp = args.length == 3 ? dateTimeToInstant.convert(dateTime) : null;

    if (doc.getTrx() instanceof NodeTrx) {
      final NodeTrx wtx = (NodeTrx) doc.getTrx();
      final long revision = wtx.getRevisionNumber();
      wtx.commit(commitMessage, commitTimesstamp);
      return new Int64(revision);
    } else {
      final ResourceSession<?, ?> manager = doc.getTrx().getResourceSession();
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
        wtx.commit(commitMessage, commitTimesstamp);
        return new Int64(revisionToCommit);
      } finally {
        if (newTrxOpened && wtx != null) {
          wtx.close();
        }
      }
    }
  }
}
