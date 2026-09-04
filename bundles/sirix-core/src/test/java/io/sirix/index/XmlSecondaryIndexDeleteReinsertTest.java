package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression coverage for incremental CAS posting deletion and reinsertion: removing the last node
 * key must make the logical entry absent from point lookups and scans, and inserting the same value
 * again must make it visible without rebuilding the index.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlSecondaryIndexDeleteReinsertTest {

  private static final String PATH = "//bla/blabla";

  private static final String VALUE = "duplicate";

  /**
   * {@link Holder} reference.
   */
  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    holder = Holder.openResourceSession();
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

    final IndexDef idxDef =
        IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(Path.parse(PATH)), 0, IndexDef.DbType.XML);

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
    NodeReferences refs = lookup(wtx, indexDef, casKey);
    assertNotNull(refs);
    assertEquals(2L, refs.getNodeKeys().getLongCardinality());

    // Removing one of two node keys keeps the entry visible.
    wtx.moveTo(firstTextNodeKey);
    wtx.remove();
    wtx.commit();

    refs = lookup(wtx, indexDef, casKey);
    assertNotNull(refs);
    assertEquals(1L, refs.getNodeKeys().getLongCardinality());
    assertTrue(refs.contains(secondTextNodeKey));
    assertEquals(1L, scanCardinality(wtx, indexController, indexDef));

    // Removing the last node key leaves a tombstone: point lookups miss...
    wtx.moveTo(secondTextNodeKey);
    wtx.remove();
    wtx.commit();

    refs = lookup(wtx, indexDef, casKey);
    assertNull(refs);

    // ...and scans skip the entry instead of yielding an empty reference set.
    assertEquals(0L, scanCardinality(wtx, indexController, indexDef));

    // Re-indexing the same value revives the tombstone in place.
    assertTrue(wtx.moveTo(firstBlablaNodeKey));
    final long revivedTextNodeKey = wtx.insertTextAsFirstChild(VALUE).getNodeKey();
    wtx.commit();

    refs = lookup(wtx, indexDef, casKey);
    assertNotNull(refs);
    assertEquals(1L, refs.getNodeKeys().getLongCardinality());
    assertTrue(refs.contains(revivedTextNodeKey));
    assertFalse(refs.contains(secondTextNodeKey));
    assertEquals(1L, scanCardinality(wtx, indexController, indexDef));

    assertTrue(wtx.moveTo(blaNodeKey));
    wtx.close();
  }

  private NodeReferences lookup(final XmlNodeTrx wtx, final IndexDef indexDef, final CASValue casKey) {
    final HOTIndexReader<CASValue> reader = HOTIndexReader.create(wtx.getStorageEngineReader(),
        CASKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());
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
