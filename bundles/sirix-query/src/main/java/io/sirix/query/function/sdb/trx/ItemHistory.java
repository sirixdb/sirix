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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * <p>
 * Function for getting the item in all revisions in which it has been changed (order ascending).
 * Supported signature is:
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
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public ItemHistory(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> item = ((StructuredDBItem<?>) args[0]);
    final NodeReadOnlyTrx rtx = item.getTrx();

    final var resourceSession = rtx.getResourceSession();
    final NodeReadOnlyTrx rtxInMostRecentRevision = resourceSession.beginNodeReadOnlyTrx();

    final RevisionReferencesNode node;
    try {
      node = rtxInMostRecentRevision.getStorageEngineReader()
          .getRecord(item.getNodeKey(), IndexType.RECORD_TO_REVISIONS, 0);
    } finally {
      rtxInMostRecentRevision.close();
    }

    if (node == null) {
      final Deque<Item> sequences = new ArrayDeque<>();
      final var itemResourceSession = item.getTrx().getResourceSession();
      int revision = itemResourceSession.getMostRecentRevisionNumber();
      while (revision > 0) {
        final NodeReadOnlyTrx rtxInRevision = resourceSession.beginNodeReadOnlyTrx(revision);
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
      // Fast path: use RECORD_TO_REVISIONS index for the revision list,
      // then open all transactions concurrently using virtual threads.
      final int[] revisions = node.getRevisions();
      final long nodeKey = item.getNodeKey();
      final boolean isJson = item instanceof JsonDBItem;
      final JsonItemFactory jsonItemFactory = isJson ? new JsonItemFactory() : null;

      // Open transactions and resolve nodes in parallel using virtual threads
      final Item[] results = new Item[revisions.length];
      try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
        final List<Future<?>> futures = new ArrayList<>(revisions.length);
        for (int i = 0; i < revisions.length; i++) {
          final int idx = i;
          final int revision = revisions[i];
          futures.add(executor.submit(() -> {
            final NodeReadOnlyTrx rtxInRevision = resourceSession.beginNodeReadOnlyTrx(revision);
            if (rtxInRevision.moveTo(nodeKey)) {
              if (isJson) {
                results[idx] = jsonItemFactory.getSequence(
                    (JsonNodeReadOnlyTrx) rtxInRevision, ((JsonDBItem) item).getCollection());
              } else {
                results[idx] = new XmlDBNode(
                    (XmlNodeReadOnlyTrx) rtxInRevision, ((XmlDBNode) item).getCollection());
              }
            } else {
              rtxInRevision.close();
            }
          }));
        }
        for (final Future<?> future : futures) {
          future.get();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Failed to load item history in parallel", e);
      } catch (Exception e) {
        throw new RuntimeException("Failed to load item history in parallel", e);
      }

      // Filter out nulls (revisions where node wasn't found) and collect in order
      final List<Item> sequences = new ArrayList<>(revisions.length);
      for (final Item result : results) {
        if (result != null) {
          sequences.add(result);
        }
      }

      return new ItemSequence(sequences.toArray(new Item[0]));
    }
  }
}
