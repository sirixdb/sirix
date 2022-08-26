package org.sirix.xquery.function.jn.temporal;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.index.IndexType;
import org.sirix.node.RevisionReferencesNode;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonItemFactory;

/**
 * <p>
 * Function for selecting a node in the revision it first existed. The parameter is the context node. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>jn:first-existing($doc as json-item()) as json-item()*</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class FirstExisting extends AbstractFunction {

  /**
   * Function name.
   */
  public final static QNm FIRST_EXISTING = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "first-existing");

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public FirstExisting(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final JsonDBItem item = (JsonDBItem) args[0];

    final RevisionReferencesNode indexNode =
        item.getTrx().getPageTrx().getRecord(item.getNodeKey(), IndexType.RECORD_TO_REVISIONS, 0);

    if (indexNode != null) {
      final var revision = indexNode.getRevisions()[0];
      final var resourceManager = item.getTrx().getResourceSession();
      final var rtx = resourceManager.beginNodeReadOnlyTrx(revision);
      rtx.moveTo(item.getNodeKey());
      return new JsonItemFactory().getSequence(rtx, item.getCollection());
    }

    final var resourceManager = item.getTrx().getResourceSession();
    for (int revisionNumber = 1; revisionNumber < resourceManager.getMostRecentRevisionNumber(); revisionNumber++) {
      final var rtx = resourceManager.beginNodeReadOnlyTrx(revisionNumber);
      if (rtx.moveTo(item.getNodeKey())) {
        return new JsonItemFactory().getSequence(rtx, item.getCollection());
      } else {
        rtx.close();
      }
    }
    return null;
  }
}
