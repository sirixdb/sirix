package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for #1065 (defect B): removing the last node key of an index entry leaves the
 * {@code RBNodeKey}/{@code RBNodeValue} pair in the red-black tree (there is no structural
 * delete/rebalance in the copy-on-write tree). Such a tombstone must be treated as absent by
 * point lookups ({@code RBTreeReader#get}) and index scans ({@code IndexFilterAxis}), and
 * re-indexing the same key must revive the entry in place.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlRedBlackTreeTombstoneTest {

  private static final String PATH = "//bla/blabla";

  private static final String VALUE = "duplicate";

  /**
   * {@link Holder} reference.
   */
  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    holder = Holder.openResourceSessionWithRedBlackTreeIndexes();
  }

  @After
  public void tearDown() {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void emptiedEntryIsAbsentForLookupsAndScansAndIsRevivedByReindexing() {
    final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx();

    final XmlIndexController indexController =
        holder.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

    final IndexDef idxDef = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(Path.parse(PATH)), 0,
        IndexDef.DbType.XML);

    indexController.createIndexes(Set.of(idxDef), wtx);

    // //bla with two //bla/blabla children, both holding the same text value.
    final long blaNodeKey = wtx.insertElementAsFirstChild(new QNm("bla")).getNodeKey();
    final long firstBlablaNodeKey = wtx.insertElementAsFirstChild(new QNm("blabla")).getNodeKey();
    final long firstTextNodeKey = wtx.insertTextAsFirstChild(VALUE).getNodeKey();
    wtx.moveTo(firstBlablaNodeKey);
    wtx.insertElementAsRightSibling(new QNm("blabla"));
    final long secondTextNodeKey = wtx.insertTextAsFirstChild(VALUE).getNodeKey();
    wtx.commit();

    final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

    final var pathNodeKeys = wtx.getPathSummary().getPCRsForPath(Path.parse(PATH));
    assertEquals(1, pathNodeKeys.size());
    final long pcr = pathNodeKeys.iterator().nextLong();
    final CASValue casKey = new CASValue(new Str(VALUE), Type.STR, pcr);

    // Both text nodes are indexed under the same key.
    Optional<NodeReferences> refs = lookup(wtx, indexDef, casKey);
    assertTrue(refs.isPresent());
    assertEquals(2L, refs.get().getNodeKeys().getLongCardinality());

    // Removing one of two node keys keeps the entry visible.
    wtx.moveTo(firstTextNodeKey);
    wtx.remove();
    wtx.commit();

    refs = lookup(wtx, indexDef, casKey);
    assertTrue(refs.isPresent());
    assertEquals(1L, refs.get().getNodeKeys().getLongCardinality());
    assertTrue(refs.get().contains(secondTextNodeKey));
    assertEquals(1L, scanCardinality(wtx, indexController, indexDef));

    // Removing the last node key leaves a tombstone: point lookups miss...
    wtx.moveTo(secondTextNodeKey);
    wtx.remove();
    wtx.commit();

    refs = lookup(wtx, indexDef, casKey);
    assertTrue(refs.isEmpty());

    // ...and scans skip the entry instead of yielding an empty reference set.
    assertEquals(0L, scanCardinality(wtx, indexController, indexDef));

    // Re-indexing the same value revives the tombstone in place.
    assertTrue(wtx.moveTo(firstBlablaNodeKey));
    final long revivedTextNodeKey = wtx.insertTextAsFirstChild(VALUE).getNodeKey();
    wtx.commit();

    refs = lookup(wtx, indexDef, casKey);
    assertTrue(refs.isPresent());
    assertEquals(1L, refs.get().getNodeKeys().getLongCardinality());
    assertTrue(refs.get().contains(revivedTextNodeKey));
    assertFalse(refs.get().contains(secondTextNodeKey));
    assertEquals(1L, scanCardinality(wtx, indexController, indexDef));

    assertTrue(wtx.moveTo(blaNodeKey));
    wtx.close();
  }

  private Optional<NodeReferences> lookup(final XmlNodeTrx wtx, final IndexDef indexDef, final CASValue casKey) {
    final RBTreeReader<CASValue, NodeReferences> reader =
        RBTreeReader.getInstance(holder.getResourceSession().getIndexCache(), wtx.getStorageEngineReader(),
            indexDef.getType(), indexDef.getID());
    return reader.get(casKey, SearchMode.EQUAL);
  }

  private long scanCardinality(final XmlNodeTrx wtx, final XmlIndexController indexController,
      final IndexDef indexDef) {
    final Iterator<NodeReferences> iter = indexController.openCASIndex(wtx.getStorageEngineReader(), indexDef,
        indexController.createCASFilter(Set.of(PATH), new Str(VALUE), SearchMode.EQUAL, new XmlPCRCollector(wtx)));
    long cardinality = 0L;
    while (iter.hasNext()) {
      cardinality += iter.next().getNodeKeys().getLongCardinality();
    }
    return cardinality;
  }
}
