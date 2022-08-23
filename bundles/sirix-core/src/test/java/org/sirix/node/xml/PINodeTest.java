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
import net.openhft.chronicle.bytes.Bytes;
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
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.utils.NamePageHash;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Processing instruction node test.
 */
public class PINodeTest {

  /** {@link Holder} instance. */
  private Holder mHolder;

  /** Sirix {@link PageReadOnlyTrx} instance. */
  private PageReadOnlyTrx pageReadTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    mHolder = Holder.generateDeweyIDResourceMgr();
    pageReadTrx = mHolder.getResourceManager().beginPageReadOnlyTrx();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    mHolder.close();
  }

  @Test
  public void testProcessInstructionNode() throws IOException {
    final byte[] value = { (byte) 17, (byte) 18 };

    final NodeDelegate del = new NodeDelegate(99, 13, Hashing.sha256(), null, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate structDel = new StructNodeDelegate(del, 17, 16, 22, 1, 1);
    final NameNodeDelegate nameDel = new NameNodeDelegate(del, 13, 14, 15, 1);
    final ValueNodeDelegate valDel = new ValueNodeDelegate(del, value, false);

    final PINode node = new PINode(structDel, nameDel, valDel, pageReadTrx);
    node.setHash(node.computeHash());

    // Create empty node.
    check(node);

    // Serialize and deserialize node.
    final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();
    node.getKind().serialize(data, node, pageReadTrx);
    final PINode node2 = (PINode) NodeKind.PROCESSING_INSTRUCTION.deserialize(data,
                                                                              node.getNodeKey(),
                                                                              node.getDeweyID().toBytes(),
                                                                              pageReadTrx);
    check(node2);
  }

  private final void check(final PINode node) {
    // Now compare.
    assertEquals(99L, node.getNodeKey());
    assertEquals(13L, node.getParentKey());

    assertEquals(17L, node.getFirstChildKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(22L, node.getLeftSiblingKey());
    assertEquals(1, node.getDescendantCount());
    assertEquals(1, node.getChildCount());

    assertEquals(13, node.getURIKey());
    assertEquals(14, node.getPrefixKey());
    assertEquals(15, node.getLocalNameKey());

    assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    assertEquals(2, node.getRawValue().length);
    assertEquals(NodeKind.PROCESSING_INSTRUCTION, node.getKind());
    assertEquals(true, node.hasParent());
    assertEquals(SirixDeweyID.newRootID(), node.getDeweyID());
  }

}
