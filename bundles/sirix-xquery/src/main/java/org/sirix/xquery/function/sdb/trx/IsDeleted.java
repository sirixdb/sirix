package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.Bool;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.ItemSequence;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.IndexType;
import org.sirix.node.RevisionReferencesNode;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonItemFactory;
import org.sirix.xquery.node.XmlDBNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
    final var resMgr = rtx.getResourceManager();
    final var mostRecentRevisionNumber = resMgr.getMostRecentRevisionNumber();
    final NodeReadOnlyTrx rtxInMostRecentRevision = getTrx(resMgr, mostRecentRevisionNumber);
    final Optional<RevisionReferencesNode> optionalNode =
        rtxInMostRecentRevision.getPageTrx().getRecord(item.getNodeKey(), IndexType.RECORD_TO_REVISIONS, 0);
    final var node = optionalNode.orElseThrow(() -> new IllegalStateException());
    final var revisions = node.getRevisions();
    final var mostRecentRevisionOfItem = revisions[revisions.length - 1];
    final var rtxInMostRecentRevisionOfItem = getTrx(resMgr, mostRecentRevisionOfItem);
    return rtxInMostRecentRevisionOfItem.moveTo(item.getNodeKey()).hasMoved() ? Bool.FALSE : Bool.TRUE;
  }

  private NodeReadOnlyTrx getTrx(org.sirix.api.ResourceManager<?, ?> resMgr, int mostRecentRevisionOfItem) {
    final var optionalRtxInMostRecentRevisionOfItem =
        resMgr.getNodeReadTrxByRevisionNumber(mostRecentRevisionOfItem);
    final NodeReadOnlyTrx rtxInMostRecentRevisionOfItem;
    if (optionalRtxInMostRecentRevisionOfItem.isEmpty()) {
      rtxInMostRecentRevisionOfItem = resMgr.beginNodeReadOnlyTrx(mostRecentRevisionOfItem);
    } else {
      rtxInMostRecentRevisionOfItem = optionalRtxInMostRecentRevisionOfItem.get();
    }
    return rtxInMostRecentRevisionOfItem;
  }
}
