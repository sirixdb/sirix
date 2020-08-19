package org.sirix.index;

import com.google.common.collect.ImmutableSet;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.xdm.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.access.trx.node.xml.XmlIndexController;
import org.sirix.api.Movement;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the AVLTree implementation.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlAVLTreeIntegrationTest {

  /**
   * {@link Holder} reference.
   */
  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    holder = Holder.openResourceManager();
  }

  @After
  public void tearDown() {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testCASAttributeIndex() throws PathException {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();

    XmlIndexController indexController = holder.getResourceManager().getWtxIndexController(wtx.getRevisionNumber() - 1);

    final IndexDef idxDef =
        IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(Path.parse("//bla/@foobar")), 0);

    indexController.createIndexes(ImmutableSet.of(idxDef), wtx);

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

    AVLTreeReader<CASValue, NodeReferences> reader =
        AVLTreeReader.getInstance(holder.getResourceManager().getIndexCache(),
                                  wtx.getPageTrx(),
                                  indexDef.getType(),
                                  indexDef.getID());

    final var pathNodeKeys = wtx.getPathSummary().getPCRsForPath(Path.parse("//bla/@foobar"), false);

    assertEquals(ImmutableSet.of(3L, 8L), pathNodeKeys);

    final Optional<NodeReferences> fooRefs = reader.get(new CASValue(new Str("foo"), Type.STR, 1), SearchMode.EQUAL);
    assertTrue(fooRefs.isEmpty());
    final Optional<NodeReferences> bazRefs1 = reader.get(new CASValue(new Str("baz"), Type.STR, 3), SearchMode.EQUAL);
    check(bazRefs1, ImmutableSet.of(3L));
    final Optional<NodeReferences> bazRefs2 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);
    check(bazRefs2, ImmutableSet.of(8L));

    wtx.moveTo(1);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "bbbb", Movement.TOPARENT);
    final var secondNodeKey = wtx.moveToAttributeByName(new QNm("foobar")).trx().getNodeKey();
    wtx.commit();

    reader = AVLTreeReader.getInstance(holder.getResourceManager().getIndexCache(),
                                       wtx.getPageTrx(),
                                       indexDef.getType(),
                                       indexDef.getID());

    final Optional<NodeReferences> bazRefs3 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);

    check(bazRefs3, ImmutableSet.of(8L, 10L));

    wtx.moveTo(secondNodeKey);
    wtx.remove();
    wtx.commit();

    reader = AVLTreeReader.getInstance(holder.getResourceManager().getIndexCache(),
                                       wtx.getPageTrx(),
                                       indexDef.getType(),
                                       indexDef.getID());

    final Optional<NodeReferences> bazRefs4 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);

    check(bazRefs4, ImmutableSet.of(8L));

    wtx.moveTo(nodeKey);
    wtx.remove();
    wtx.commit();

    reader = AVLTreeReader.getInstance(holder.getResourceManager().getIndexCache(),
                                       wtx.getPageTrx(),
                                       indexDef.getType(),
                                       indexDef.getID());

    final Optional<NodeReferences> bazRefs5 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);

    check(bazRefs5, ImmutableSet.of());

    //    try (final var printStream = new PrintStream(new BufferedOutputStream(System.out))) {
    //      reader.dump(printStream);
    //      printStream.flush();
    //    }
  }

  @Test
  public void testCASTextIndex() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();

    XmlIndexController indexController = holder.getResourceManager().getWtxIndexController(wtx.getRevisionNumber() - 1);

    final IndexDef idxDef =
        IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(Path.parse("//bla/blabla")), 0);

    indexController.createIndexes(ImmutableSet.of(idxDef), wtx);

    final long blaNodeKey = wtx.insertElementAsFirstChild(new QNm("bla")).getNodeKey();
    wtx.insertTextAsFirstChild("tadaaaa");
    final long blablaNodeKey = wtx.insertElementAsRightSibling(new QNm("blabla")).getNodeKey();
    final long nodeKey = wtx.insertTextAsFirstChild("törööö").getNodeKey();
    wtx.commit();

    final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

    AVLTreeReader<CASValue, NodeReferences> reader =
        AVLTreeReader.getInstance(holder.getResourceManager().getIndexCache(),
                                  wtx.getPageTrx(),
                                  indexDef.getType(),
                                  indexDef.getID());

    Optional<NodeReferences> blablaRefs = reader.get(new CASValue(new Str("törööö"), Type.STR, 2), SearchMode.EQUAL);

    check(blablaRefs, ImmutableSet.of(4L));

    wtx.moveTo(nodeKey);
    wtx.remove();

    reader = AVLTreeReader.getInstance(holder.getResourceManager().getIndexCache(),
                                       wtx.getPageTrx(),
                                       indexDef.getType(),
                                       indexDef.getID());

    blablaRefs = reader.get(new CASValue(new Str("törööö"), Type.STR, 2), SearchMode.EQUAL);

    check(blablaRefs, ImmutableSet.of());

    assertTrue(wtx.moveTo(blablaNodeKey).hasMoved());
    wtx.insertTextAsFirstChild("törööö");
    wtx.moveTo(blaNodeKey);
    wtx.remove();
    wtx.commit();

    reader = AVLTreeReader.getInstance(holder.getResourceManager().getIndexCache(),
                                       wtx.getPageTrx(),
                                       indexDef.getType(),
                                       indexDef.getID());

    blablaRefs = reader.get(new CASValue(new Str("törööö"), Type.STR, 2), SearchMode.EQUAL);

    check(blablaRefs, ImmutableSet.of());

    final var pathNodeKeys = wtx.getPathSummary().getPCRsForPath(Path.parse("//bla/blabla"), false);

    assertTrue(pathNodeKeys.isEmpty());
  }

  private void check(final Optional<NodeReferences> barRefs, final Set<Long> keys) {
    assertTrue(barRefs.isPresent());
    assertEquals(keys, barRefs.get().getNodeKeys());
  }

}
