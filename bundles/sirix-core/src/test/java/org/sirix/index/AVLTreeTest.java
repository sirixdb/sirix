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
import org.sirix.exception.SirixException;
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
 *
 */
public class AVLTreeTest {

  /** {@link Holder} reference. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.openResourceManager();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testAttributeIndex() throws SirixException, PathException {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();

    final XmlIndexController indexController =
        holder.getResourceManager().getWtxIndexController(wtx.getRevisionNumber() - 1);

    final IndexDef idxDef = IndexDefs.createCASIdxDef(false, Optional.ofNullable(Type.STR),
        Collections.singleton(Path.parse("//bla/@foobar")), 0);

    indexController.createIndexes(ImmutableSet.of(idxDef), wtx);

    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foo"), "bar", Movement.TOPARENT);
    wtx.insertAttribute(new QNm("foobar"), "baz", Movement.TOPARENT);
    wtx.insertElementAsFirstChild(new QNm("blabla"));
    wtx.insertAttribute(new QNm("foo"), "bar", Movement.TOPARENT);
    wtx.insertAttribute(new QNm("foobar"), "baz", Movement.TOPARENT);
    wtx.moveTo(1);
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.insertAttribute(new QNm("foobar"), "bbbb", Movement.TOPARENT);
    wtx.commit();

    final IndexDef indexDef = indexController.getIndexes().getIndexDef(0, IndexType.CAS);

    final AVLTreeReader<CASValue, NodeReferences> reader =
        AVLTreeReader.getInstance(wtx.getPageTrx(), indexDef.getType(), indexDef.getID());
    final Optional<NodeReferences> fooRefs = reader.get(new CASValue(new Str("foo"), Type.STR, 1), SearchMode.EQUAL);
    assertTrue(fooRefs.isEmpty());
    final Optional<NodeReferences> bazRefs1 = reader.get(new CASValue(new Str("baz"), Type.STR, 3), SearchMode.EQUAL);
    check(bazRefs1, ImmutableSet.of(3L));
    final Optional<NodeReferences> bazRefs2 = reader.get(new CASValue(new Str("bbbb"), Type.STR, 8), SearchMode.EQUAL);
    check(bazRefs2, ImmutableSet.of(8L));
  }

  @Test
  public void testTextIndex() throws SirixException {
    // final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    // wtx.insertElementAsFirstChild(new QNm("bla"));
    // wtx.insertTextAsFirstChild("bla");
    // wtx.insertElementAsRightSibling(new QNm("blabla"));
    // wtx.insertTextAsFirstChild("blabla");
    // wtx.commit();
    // final AVLTreeReader<CASValue, NodeReferences> textIndex = wtx
    // .getTextValueIndex();
  }

  private void check(final Optional<NodeReferences> barRefs, final Set<Long> keys) {
    assertTrue(barRefs.isPresent());
    assertEquals(keys, barRefs.get().getNodeKeys());
  }

}
