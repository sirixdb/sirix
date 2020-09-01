package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.array.DArray;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.ItemSequence;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
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
import java.util.List;
import java.util.Optional;

/**
 * <p>
 * Function for getting the item in all revisions in which it has been changed (order ascending). Supported
 * signature is:
 * </p>
 * <ul>
 * <li><code>sdb:node-history($node as xs:structured-item) as xs:structured-item+</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class NodeHistory extends AbstractFunction {

  /**
   * Get function name.
   */
  public final static QNm NODE_HISTORY = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "node-history");

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public NodeHistory(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> item = ((StructuredDBItem<?>) args[0]);

    final NodeReadOnlyTrx rtx = item.getTrx();
    final Optional<RevisionReferencesNode> optionalNode =
        rtx.getPageTrx().getRecord(item.getNodeKey(), IndexType.RECORD_TO_REVISIONS, 0);

    final RevisionReferencesNode node = optionalNode.orElseThrow(() -> new IllegalStateException());

    final int[] revisions = node.getRevisions();
    final List<Item> sequences = new ArrayList<>(revisions.length);

    final var resMgr = rtx.getResourceManager();

    for (int i = 0, length = revisions.length; i < length; i++) {
      final var revision = revisions[i];
      final var optionalRtxInRevision = resMgr.getNodeReadTrxByRevisionNumber(revision);
      final NodeReadOnlyTrx rtxInRevision;
      if (optionalRtxInRevision.isEmpty()) {
        rtxInRevision = resMgr.beginNodeReadOnlyTrx(revision);
      } else {
        rtxInRevision = optionalRtxInRevision.get();
      }

      if (rtxInRevision.moveTo(item.getNodeKey()).hasMoved()) {
        if (rtxInRevision instanceof XmlNodeReadOnlyTrx) {
          sequences.add(new XmlDBNode((XmlNodeReadOnlyTrx) rtxInRevision, ((XmlDBNode) item).getCollection()));
        } else if (rtxInRevision instanceof JsonNodeReadOnlyTrx) {
          final JsonDBItem jsonItem = (JsonDBItem) item;
          sequences.add(new JsonItemFactory().getSequence((JsonNodeReadOnlyTrx) rtxInRevision,
                                                          jsonItem.getCollection()));
        }
      }
    }

    return new ItemSequence(sequences.toArray(new Item[] {}));
  }
}
