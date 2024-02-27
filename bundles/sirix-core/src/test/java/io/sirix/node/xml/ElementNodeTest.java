/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node.xml;

import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.utils.NamePageHash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.exception.SirixException;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.settings.Constants;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Element node test.
 */
public class ElementNodeTest {

  /**
   * {@link Holder} instance.
   */
  private Holder holder;

  /**
   * Sirix {@link PageReadOnlyTrx} instance.
   */
  private PageReadOnlyTrx pageReadTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    holder = Holder.generateDeweyIDResourceMgr();
    pageReadTrx = holder.getResourceManager().beginPageReadOnlyTrx();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    holder.close();
  }

  @Test
  public void testElementNode() {
    final NodeDelegate del = new NodeDelegate(13,
                                              14,
                                              LongHashFunction.xx3(), Constants.NULL_REVISION_NUMBER,
                                              0,
                                              SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(del, 12L, 17L, 16L, 1L, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(del, 17, 18, 19, 1);

    final ElementNode node =
        new ElementNode(strucDel, nameDel, new LongArrayList(), new LongArrayList(), new QNm("ns", "a", "p"));
    var bytes = Bytes.elasticHeapByteBuffer();
    node.setHash(node.computeHash(bytes));

    // Create empty node.
    node.insertAttribute(97);
    node.insertAttribute(98);
    node.insertNamespace(99);
    node.insertNamespace(100);
    check(node);

    // Serialize and deserialize node.
    final Bytes<ByteBuffer> data = Bytes.elasticHeapByteBuffer();
    node.getKind().serialize(data, node, pageReadTrx);
    final ElementNode node2 =
        (ElementNode) NodeKind.ELEMENT.deserialize(data, node.getNodeKey(), node.getDeweyID().toBytes(), pageReadTrx);
    check(node2);
  }

  private final void check(final ElementNode node) {
    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(12L, node.getFirstChildKey());
    assertEquals(16L, node.getLeftSiblingKey());
    assertEquals(17L, node.getRightSiblingKey());
    assertEquals(1, node.getChildCount());
    assertEquals(2, node.getAttributeCount());
    assertEquals(2, node.getNamespaceCount());
    assertEquals(17, node.getURIKey());
    assertEquals(18, node.getPrefixKey());
    assertEquals(19, node.getLocalNameKey());
    Assert.assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    Assert.assertEquals(NodeKind.ELEMENT, node.getKind());
    assertTrue(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasLeftSibling());
    assertTrue(node.hasRightSibling());
    assertEquals(97L, node.getAttributeKey(0));
    assertEquals(98L, node.getAttributeKey(1));
    assertEquals(99L, node.getNamespaceKey(0));
    assertEquals(100L, node.getNamespaceKey(1));
  }

}
