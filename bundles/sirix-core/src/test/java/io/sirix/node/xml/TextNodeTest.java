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
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixException;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Text node test.
 */
public class TextNodeTest {

  /**
   * {@link Holder} instance.
   */
  private Holder holder;

  /**
   * Sirix {@link StorageEngineReader} instance.
   */
  private StorageEngineReader pageReadTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    holder = Holder.generateDeweyIDResourceSession();
    pageReadTrx = holder.getResourceSession().beginPageReadOnlyTrx();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    holder.close();
  }

  @Test
  public void testTextRootNode() {
    // Create empty node.
    final byte[] value = { (byte) 17, (byte) 18 };
    
    // Create node with MemorySegment
    final var data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(14);                              // parentKey - offset 0
    data.writeInt(Constants.NULL_REVISION_NUMBER);   // previousRevision - offset 8
    data.writeInt(0);                                // lastModifiedRevision - offset 12
    
    // Write sibling keys (16 bytes)
    data.writeLong(16L);                             // rightSiblingKey - offset 16
    data.writeLong(15L);                             // leftSiblingKey - offset 24
    
    var segment = (java.lang.foreign.MemorySegment) data.asBytesIn().getUnderlying();
    final TextNode node = new TextNode(segment, 13L, SirixDeweyID.newRootID(), 
                                       LongHashFunction.xx3(), value, false);
    var hashBytes = Bytes.elasticOffHeapByteBuffer();
    node.setHash(node.computeHash(hashBytes));
    check(node);

    // Serialize and deserialize node.
    final BytesOut<?> data2 = Bytes.elasticOffHeapByteBuffer();
    node.getKind().serialize(data2, node, pageReadTrx.getResourceSession().getResourceConfig());
    final TextNode node2 = (TextNode) NodeKind.TEXT.deserialize(data2.asBytesIn(),
                                                                node.getNodeKey(),
                                                                node.getDeweyID().toBytes(),
                                                                pageReadTrx.getResourceSession().getResourceConfig());
    check(node2);
  }

  private void check(final TextNode node) {
    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(15L, node.getLeftSiblingKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    assertEquals(2, node.getRawValue().length);
    assertEquals(NodeKind.TEXT, node.getKind());
    assertFalse(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasLeftSibling());
    assertTrue(node.hasRightSibling());
  }

}
