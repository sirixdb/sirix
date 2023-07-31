package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexType;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;
import io.sirix.query.node.XmlDBNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * <p>
 * Function for getting the item in all revisions in which it has been changed (order ascending). Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>sdb:item-history($item as xs:structured-item) as xs:structured-item+</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class ItemHistory extends AbstractFunction {

  /**
   * Get function name.
   */
  public final static QNm NODE_HISTORY = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "item-history");

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public ItemHistory(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> item = ((StructuredDBItem<?>) args[0]);
    final NodeReadOnlyTrx rtx = item.getTrx();

    final var resMgr = rtx.getResourceSession();
    final NodeReadOnlyTrx rtxInMostRecentRevision = resMgr.beginNodeReadOnlyTrx();

    final RevisionReferencesNode node =
        rtxInMostRecentRevision.getPageTrx().getRecord(item.getNodeKey(), IndexType.RECORD_TO_REVISIONS, 0);

    if (node == null) {
      final Deque<Item> sequences = new ArrayDeque<>();
      final var resourceSession = item.getTrx().getResourceSession();
      int revision = resourceSession.getMostRecentRevisionNumber();
      while (revision > 0) {
        final NodeReadOnlyTrx rtxInRevision = resMgr.beginNodeReadOnlyTrx(revision);
        if (rtxInRevision.moveTo(item.getNodeKey())) {
          if (rtxInRevision instanceof XmlNodeReadOnlyTrx) {
            assert item instanceof XmlDBNode;
            final var xmlDBNode = new XmlDBNode((XmlNodeReadOnlyTrx) rtxInRevision, ((XmlDBNode) item).getCollection());
            sequences.addFirst(xmlDBNode);
          } else if (rtxInRevision instanceof JsonNodeReadOnlyTrx trx) {
            assert item instanceof JsonDBItem;
            final var jsonItem = new JsonItemFactory().getSequence(trx, ((JsonDBItem) item).getCollection());
            sequences.addFirst(jsonItem);
          }
          revision = rtxInRevision.getPreviousRevisionNumber();
        } else {
          rtxInRevision.close();
          revision--;
        }
      }

      return new ItemSequence(sequences.toArray(new Item[0]));
    } else {
      final int[] revisions = node.getRevisions();
      final List<Item> sequences = new ArrayList<>(revisions.length);

      for (final int revision : revisions) {
        final NodeReadOnlyTrx rtxInRevision = resMgr.beginNodeReadOnlyTrx(revision);

        if (rtxInRevision.moveTo(item.getNodeKey())) {
          if (rtxInRevision instanceof XmlNodeReadOnlyTrx) {
            assert item instanceof XmlDBNode;
            sequences.add(new XmlDBNode((XmlNodeReadOnlyTrx) rtxInRevision, ((XmlDBNode) item).getCollection()));
          } else if (rtxInRevision instanceof JsonNodeReadOnlyTrx) {
            assert item instanceof JsonDBItem;
            final JsonDBItem jsonItem = (JsonDBItem) item;
            sequences.add(new JsonItemFactory().getSequence((JsonNodeReadOnlyTrx) rtxInRevision,
                                                            jsonItem.getCollection()));
          }
        } else {
          sequences.add(null);
        }
      }

      return new ItemSequence(sequences.toArray(new Item[0]));
    }
  }
}
