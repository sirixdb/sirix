package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.ResourceSession;
import io.sirix.index.IndexType;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

/**
 * <p>
 * Function for determining if the item has been deleted or not. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>sdb:is-deleted($item as xs:structured-item) as xs:boolean</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class IsDeleted extends AbstractFunction {

  /**
   * Get function name.
   */
  public final static QNm IS_DELETED = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "is-deleted");

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public IsDeleted(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> item = ((StructuredDBItem<?>) args[0]);
    final NodeReadOnlyTrx rtx = item.getTrx();

    final var resourceSession = rtx.getResourceSession();
    final var mostRecentRevisionNumber = resourceSession.getMostRecentRevisionNumber();
    final NodeReadOnlyTrx rtxInMostRecentRevision = getTrx(resourceSession, mostRecentRevisionNumber);

    final RevisionReferencesNode node =
        rtxInMostRecentRevision.getPageTrx().getRecord(item.getNodeKey(), IndexType.RECORD_TO_REVISIONS, 0);

    if (node == null) {
      return rtxInMostRecentRevision.moveTo(item.getNodeKey()) ? Bool.FALSE : Bool.TRUE;
    } else {
      final var revisions = node.getRevisions();
      final var mostRecentRevisionOfItem = revisions[revisions.length - 1];
      final NodeReadOnlyTrx rtxInMostRecentRevisionOfItem = getTrx(resourceSession, mostRecentRevisionOfItem);

      return rtxInMostRecentRevisionOfItem.moveTo(item.getNodeKey()) ? Bool.FALSE : Bool.TRUE;
    }
  }

  private NodeReadOnlyTrx getTrx(ResourceSession<?, ?> resourceManager, int mostRecentRevisionOfItem) {
    return resourceManager.beginNodeReadOnlyTrx(mostRecentRevisionOfItem);
  }
}
