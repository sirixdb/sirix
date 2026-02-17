package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;

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
    final StructuredDBItem<?> document = ((StructuredDBItem<?>) args[0]);

    final String commitMessage = args.length >= 2
        ? FunUtil.getString(args, 1, "commitMessage", null, null, false)
        : null;

    final DateTime dateTime = args.length == 3
        ? (DateTime) args[2]
        : null;
    final Instant commitTimestamp = args.length == 3
        ? dateTimeToInstant.convert(dateTime)
        : null;

    if (document.getTrx() instanceof NodeTrx) {
      final NodeTrx wtx = (NodeTrx) document.getTrx();
      final long revision = wtx.getRevisionNumber();
      wtx.commit(commitMessage, commitTimestamp);
      return new Int64(revision);
    } else {
      final ResourceSession<?, ?> resourceSession = document.getTrx().getResourceSession();
      boolean newTrxOpened = false;
      NodeTrx wtx = null;
      try {
        if (resourceSession.getNodeTrx().isPresent()) {
          wtx = resourceSession.getNodeTrx().get();
        } else {
          newTrxOpened = true;
          wtx = resourceSession.beginNodeTrx();
        }
        final int revision = document.getTrx().getRevisionNumber();
        if (revision < resourceSession.getMostRecentRevisionNumber()) {
          wtx.revertTo(document.getTrx().getRevisionNumber());
        }
        final int revisionToCommit = wtx.getRevisionNumber();
        wtx.commit(commitMessage, commitTimestamp);
        return new Int64(revisionToCommit);
      } finally {
        if (newTrxOpened && wtx != null) {
          wtx.close();
        }
      }
    }
  }
}
