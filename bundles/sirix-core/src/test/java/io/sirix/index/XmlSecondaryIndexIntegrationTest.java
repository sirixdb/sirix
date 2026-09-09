package io.sirix.index;

import io.sirix.api.Movement;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.access.trx.node.xml.XmlIndexController;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** XML CAS-index integration coverage for canonical HOT storage and incremental maintenance. */
public final class XmlSecondaryIndexIntegrationTest {

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
  public void testCASAttributeIndex() throws PathException {
    final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx();

    XmlIndexController indexController = holder.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

    final IndexDef idxDef = IndexDefs.createCASIdxDef(false, Type.STR,
        Collections.singleton(Path.parse("//bla/@foobar")), 0, IndexDef.DbType.XML);

    indexController.createIndexes(Set.of(idxDef), wtx);

    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foo"), "bar", Movement.TOPARENT);
    wtx.insertAttribute(new QNm("foobar"), "baz", Movement.TOPARENT);
    wtx.insertElementAsFirstChild(new QNm("blabla"));
    wtx.insertAttribute(new QNm("foo"), "bar", Movement.TOPARENT);
    wtx.insertAttribute(new QNm("foobar"), "baz", Movement.TOPARENT);
    wtx.moveTo(1);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    final var nodeKey = wtx.insertAttribute(new QNm("foobar"), "bbbb").getNodeKey();
    wtx.commit();

    final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

    HOTIndexReader<CASValue> reader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE,
        indexDef.getType(), indexDef.getID());

    final var pathNodeKeys = wtx.getPathSummary().getPCRsForPath(Path.parse("//bla/@foobar"));

    assertEquals(Set.of(3L, 8L), pathNodeKeys);

    final NodeReferences fooRefs = reader.get(new CASValue(new Str("foo"), Type.STR, 1), SearchMode.EQUAL);
    assertNull(fooRefs);
    final NodeReferences bazRefs1 = reader.get(new CASValue(new Str("baz"), Type.STR, 3), SearchMode.EQUAL);
    check(bazRefs1, new LongLinkedOpenHashSet(new long[] {3L}));
    final NodeReferences bazRefs2 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);
    check(bazRefs2, new LongLinkedOpenHashSet(new long[] {8L}));

    wtx.moveTo(1);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "bbbb", Movement.TOPARENT);
    wtx.moveToAttributeByName(new QNm("foobar"));
    final var secondNodeKey = wtx.getNodeKey();
    wtx.commit();

    reader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, indexDef.getType(),
        indexDef.getID());

    final NodeReferences bazRefs3 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);

    check(bazRefs3, new LongLinkedOpenHashSet(new long[] {8L, 10L}));

    wtx.moveTo(secondNodeKey);
    wtx.remove();
    wtx.commit();

    reader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, indexDef.getType(),
        indexDef.getID());

    final NodeReferences bazRefs4 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);

    check(bazRefs4, new LongLinkedOpenHashSet(new long[] {8L}));

    wtx.moveTo(nodeKey);
    wtx.remove();
    wtx.commit();

    reader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, indexDef.getType(),
        indexDef.getID());

    final NodeReferences bazRefs5 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);

    assertNull(bazRefs5);
  }

  @Test
  public void testCASTextIndex() {
    final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx();

    XmlIndexController indexController = holder.getResourceSession().getWtxIndexController(wtx.getRevisionNumber());

    final IndexDef idxDef = IndexDefs.createCASIdxDef(false, Type.STR,
        Collections.singleton(Path.parse("//bla/blabla")), 0, IndexDef.DbType.XML);

    indexController.createIndexes(Set.of(idxDef), wtx);

    final long blaNodeKey = wtx.insertElementAsFirstChild(new QNm("bla")).getNodeKey();
    wtx.insertTextAsFirstChild("tadaaaa");
    final long blablaNodeKey = wtx.insertElementAsRightSibling(new QNm("blabla")).getNodeKey();
    final long nodeKey = wtx.insertTextAsFirstChild("törööö").getNodeKey();
    wtx.commit();

    final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

    HOTIndexReader<CASValue> reader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE,
        indexDef.getType(), indexDef.getID());

    NodeReferences blablaRefs = reader.get(new CASValue(new Str("törööö"), Type.STR, 2), SearchMode.EQUAL);

    check(blablaRefs, new LongLinkedOpenHashSet(new long[] {4L}));

    wtx.moveTo(nodeKey);
    wtx.remove();

    reader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, indexDef.getType(),
        indexDef.getID());

    blablaRefs = reader.get(new CASValue(new Str("törööö"), Type.STR, 2), SearchMode.EQUAL);

    assertNull(blablaRefs);

    assertTrue(wtx.moveTo(blablaNodeKey));
    wtx.insertTextAsFirstChild("törööö");
    wtx.moveTo(blaNodeKey);
    wtx.remove();
    wtx.commit();

    reader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, indexDef.getType(),
        indexDef.getID());

    blablaRefs = reader.get(new CASValue(new Str("törööö"), Type.STR, 2), SearchMode.EQUAL);

    assertNull(blablaRefs);

    final var pathNodeKeys = wtx.getPathSummary().getPCRsForPath(Path.parse("//bla/blabla"));

    assertTrue(pathNodeKeys.isEmpty());
  }

  private void check(final NodeReferences references, final LongSet keys) {
    assertNotNull(references);
    assertEquals(keys, new LongLinkedOpenHashSet(references.getNodeKeys().toArray()));
  }

}
