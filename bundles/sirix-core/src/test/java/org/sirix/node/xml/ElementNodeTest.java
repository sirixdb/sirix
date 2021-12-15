/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.node.xml;

import com.google.common.collect.HashBiMap;
import com.google.common.hash.Hashing;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.utils.NamePageHash;

import java.io.*;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * Element node test.
 */
public class ElementNodeTest {

  /** {@link Holder} instance. */
  private Holder mHolder;

  /** Sirix {@link PageReadOnlyTrx} instance. */
  private PageReadOnlyTrx mPageReadTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    mHolder = Holder.generateDeweyIDResourceMgr();
    mPageReadTrx = mHolder.getResourceManager().beginPageReadOnlyTrx();
  }

  @After
  public void tearDown() throws SirixException {
    mPageReadTrx.close();
    mHolder.close();
  }

  @Test
  public void testElementNode() throws IOException {
    final NodeDelegate del = new NodeDelegate(13, 14, Hashing.sha256(), null, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(del, 12l, 17l, 16l, 1l, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(del, 17, 18, 19, 1);

    final ElementNode node = new ElementNode(strucDel, nameDel, new ArrayList<>(), HashBiMap.create(),
        new ArrayList<>(), new QNm("ns", "a", "p"));
    node.setHash(node.computeHash());

    // Create empty node.
    node.insertAttribute(97, 100);
    node.insertAttribute(98, 101);
    node.insertNamespace(99);
    node.insertNamespace(100);
    check(node);

    // Serialize and deserialize node.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    node.getKind().serialize(new DataOutputStream(out), node, mPageReadTrx);
    final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    final ElementNode node2 = (ElementNode) NodeKind.ELEMENT.deserialize(new DataInputStream(in), node.getNodeKey(),
        node.getDeweyID(), mPageReadTrx);
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
    assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    assertEquals(NodeKind.ELEMENT, node.getKind());
    assertEquals(true, node.hasFirstChild());
    assertEquals(true, node.hasParent());
    assertEquals(true, node.hasLeftSibling());
    assertEquals(true, node.hasRightSibling());
    assertEquals(97L, node.getAttributeKey(0));
    assertEquals(98L, node.getAttributeKey(1));
    assertEquals(99L, node.getNamespaceKey(0));
    assertEquals(100L, node.getNamespaceKey(1));
  }

}
