package org.sirix.xquery.function.jn.temporal;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.jdm.json.JsonItem;
import org.brackit.xquery.module.StaticContext;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.index.IndexType;
import org.sirix.node.RevisionReferencesNode;
import org.sirix.xquery.json.JsonDBItem;
import org.sirix.xquery.json.JsonItemFactory;

/**
 * <p>
 * Function for selecting a node in the revision it most-recently existed. The parameter is the context node. Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>jn:last-existing($doc as json-item()) as json-item()*</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class LastExisting extends AbstractFunction {

  /**
   * Function name.
   */
  public final static QNm LAST_EXISTING = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "last-existing");

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public LastExisting(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final JsonDBItem item = (JsonDBItem) args[0];

    final var resourceManager = item.getTrx().getResourceSession();

    final RevisionReferencesNode indexNode;
    try (final var pageReadOnlyTrx = resourceManager.beginPageReadOnlyTrx()) {
      indexNode = pageReadOnlyTrx.getRecord(item.getNodeKey(), IndexType.RECORD_TO_REVISIONS, 0);
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }

    if (indexNode != null) {
      final var revision = indexNode.getRevisions()[indexNode.getRevisions().length - 1];
      final var rtx = resourceManager.beginNodeReadOnlyTrx(revision);
      final var hasMoved = rtx.moveTo(item.getNodeKey());

      if (hasMoved) {
        if (revision < resourceManager.getMostRecentRevisionNumber()) {
          // Has been inserted, but never been removed, thus open most recent revision.
          rtx.close();
          return getJsonItem(item, resourceManager.getMostRecentRevisionNumber(), resourceManager);
        } else {
          rtx.moveTo(item.getNodeKey());
          return new JsonItemFactory().getSequence(rtx, item.getCollection());
        }
      } else {
        return getJsonItem(item, revision - 1, resourceManager);
      }
    }

    for (int revisionNumber = resourceManager.getMostRecentRevisionNumber(); revisionNumber > 0; revisionNumber--) {
      final var rtx = resourceManager.beginNodeReadOnlyTrx(revisionNumber);
      if (rtx.moveTo(item.getNodeKey())) {
        return new JsonItemFactory().getSequence(rtx, item.getCollection());
      } else {
        rtx.close();
      }
    }
    return null;
  }

  private JsonItem getJsonItem(JsonDBItem item, int revision, JsonResourceSession resourceManager) {
    final var trx = resourceManager.beginNodeReadOnlyTrx(revision);
    trx.moveTo(item.getNodeKey());
    return new JsonItemFactory().getSequence(trx, item.getCollection());
  }
}
