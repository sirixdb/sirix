package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.Atomic;
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
import org.sirix.xquery.json.*;
import org.sirix.xquery.node.XmlDBNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
      final List<Item> sequences = new ArrayList<>();
      final var resourceManager = item.getTrx().getResourceSession();
      Item previousItem = null;
      for (int revision = 1; revision <= resourceManager.getMostRecentRevisionNumber(); revision++) {
        final NodeReadOnlyTrx rtxInRevision = resMgr.beginNodeReadOnlyTrx(revision);
        if (rtxInRevision.moveTo(item.getNodeKey())) {
          if (rtxInRevision instanceof XmlNodeReadOnlyTrx) {
            assert item instanceof XmlDBNode;
            final var xmlDBNode = new XmlDBNode((XmlNodeReadOnlyTrx) rtxInRevision, ((XmlDBNode) item).getCollection());
            if (previousItem == null || !Objects.equals(((XmlDBNode) previousItem).getValue(), xmlDBNode.getValue())
                || !Objects.equals(((XmlDBNode) previousItem).getName(), xmlDBNode.getName())) {
              sequences.add(xmlDBNode);
              previousItem = xmlDBNode;
            } else {
              rtxInRevision.close();
            }
          } else if (rtxInRevision instanceof JsonNodeReadOnlyTrx trx) {
            assert item instanceof JsonDBItem;
            final var jsonItem = new JsonItemFactory().getSequence(trx, ((JsonDBItem) item).getCollection());
            if (previousItem == null) {
              sequences.add(jsonItem);
              previousItem = jsonItem;
            } else if (jsonItem instanceof AtomicStrJsonDBItem atomicStrJsonDBItem && !Objects.equals(
                atomicStrJsonDBItem.stringValue(),
                ((AtomicStrJsonDBItem) previousItem).stringValue())) {
              sequences.add(jsonItem);
              previousItem = jsonItem;
            } else if (jsonItem instanceof AtomicBooleanJsonDBItem atomicBooleanJsonDBItem && !Objects.equals(
                atomicBooleanJsonDBItem.booleanValue(),
                previousItem.booleanValue())) {
              sequences.add(jsonItem);
              previousItem = jsonItem;
            } else if (jsonItem instanceof NumericJsonDBItem numericJsonDBItem
                && numericJsonDBItem.cmp((Atomic) previousItem) != 0) {
              sequences.add(jsonItem);
              previousItem = jsonItem;
            } else {
              rtxInRevision.close();
            }
          }
        } else {
          rtxInRevision.close();
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
