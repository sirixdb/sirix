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

package org.sirix.node.xml;

import com.google.common.hash.Hashing;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.settings.Fixed;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * Document root node test.
 */
public class DocumentRootNodeTest {

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
  public void testDocumentRootNode() throws IOException {

    // Create empty node.
    final NodeDelegate nodeDel =
        new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
                         Hashing.sha256(), null, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    final XmlDocumentRootNode node = new XmlDocumentRootNode(nodeDel, strucDel);
    check(node);

    // Serialize and deserialize node.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    node.getKind().serialize(new DataOutputStream(out), node, mPageReadTrx);
    final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    final XmlDocumentRootNode node2 =
        (XmlDocumentRootNode) NodeKind.XML_DOCUMENT.deserialize(new DataInputStream(in), node.getNodeKey(),
            node.getDeweyID(), mPageReadTrx);
    check(node2);
  }

  private final void check(final XmlDocumentRootNode node) {
    // Now compare.
    assertEquals(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), node.getNodeKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getRightSiblingKey());
    assertEquals(0L, node.getChildCount());
    assertEquals(NodeKind.XML_DOCUMENT, node.getKind());
  }

}
