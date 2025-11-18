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

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.NodeTestHelper;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;

import java.io.IOException;

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
  public void testElementNode() throws IOException {
    // Use a simplified ResourceConfiguration for testing
    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.ROLLING)
        .storeChildCount(true)
        .build();

    // Create data in the correct serialization format with size prefix and padding
    // Format: [NodeKind][4-byte size][3-byte padding][NodeDelegate + StructNode + NameNode fields + attributes + namespaces][end padding]
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    long sizePos = NodeTestHelper.writeHeader(data, NodeKind.ELEMENT);
    long startPos = data.writePosition();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(14);                              // parentKey - offset 0
    data.writeInt(Constants.NULL_REVISION_NUMBER);   // previousRevision - offset 8
    data.writeInt(0);                                // lastModifiedRevision - offset 12
    
    // Write StructNode fields (32 bytes)
    data.writeLong(17L);                             // rightSiblingKey - offset 16
    data.writeLong(16L);                             // leftSiblingKey - offset 24
    data.writeLong(12L);                             // firstChildKey - offset 32
    data.writeLong(12L);                             // lastChildKey - offset 40
    
    // Write NameNode fields (20 bytes)
    data.writeLong(1L);                              // pathNodeKey - offset 48
    data.writeInt(18);                               // prefixKey - offset 56
    data.writeInt(19);                               // localNameKey - offset 60
    data.writeInt(17);                               // uriKey - offset 64
    
    // Write optional fields
    if (config.storeChildCount()) {
      data.writeLong(1L);                            // childCount - offset 68
    }
    if (config.hashType != HashType.NONE) {
      data.writeLong(0);                             // hash placeholder - offset 76
      data.writeLong(0);                             // descendantCount - offset 84
    }
    
    // Write attributes list
    data.writeInt(2);                                // attribute count
    data.writeLong(97);                              // attribute key 1
    data.writeLong(98);                              // attribute key 2
    
    // Write namespaces list
    data.writeInt(2);                                // namespace count
    data.writeLong(99);                              // namespace key 1
    data.writeLong(100);                             // namespace key 2
    
    // Finalize AFTER writing attributes and namespaces
    NodeTestHelper.finalizeSerialization(data, sizePos, startPos);
    
    // Deserialize to create properly initialized node
    var bytesIn = data.asBytesIn();
    bytesIn.readByte(); // Skip NodeKind byte
    final ElementNode node = (ElementNode) NodeKind.ELEMENT.deserialize(
        bytesIn, 13L, SirixDeweyID.newRootID().toBytes(), config);
    
    // Compute and set hash
    var hashBytes = Bytes.elasticOffHeapByteBuffer();
    node.setHash(node.computeHash(hashBytes));
    
    check(node);

    // Serialize and deserialize node.
    final BytesOut<?> data2 = Bytes.elasticOffHeapByteBuffer();
    data2.writeByte(NodeKind.ELEMENT.getId()); // Write NodeKind to ensure proper alignment
    node.getKind().serialize(data2, node, config);
    var bytesIn2 = data2.asBytesIn();
    bytesIn2.readByte(); // Skip NodeKind byte
    final ElementNode node2 = (ElementNode) NodeKind.ELEMENT.deserialize(bytesIn2,
                                                                         node.getNodeKey(),
                                                                         node.getDeweyID().toBytes(),
                                                                         config);
    check(node2);
  }

  private final void check(final ElementNode node) {
    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(12L, node.getFirstChildKey());
    assertEquals(12L, node.getLastChildKey());
    assertEquals(16L, node.getLeftSiblingKey());
    assertEquals(17L, node.getRightSiblingKey());
    assertEquals(1, node.getChildCount());
    assertEquals(2, node.getAttributeCount());
    assertEquals(2, node.getNamespaceCount());
    assertEquals(17, node.getURIKey());
    assertEquals(18, node.getPrefixKey());
    assertEquals(19, node.getLocalNameKey());
    assertEquals(1L, node.getPathNodeKey());
    // typeKey is not persisted, so we don't test it
    Assert.assertEquals(NodeKind.ELEMENT, node.getKind());
    assertTrue(node.hasFirstChild());
    assertTrue(node.hasLastChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasLeftSibling());
    assertTrue(node.hasRightSibling());
    assertEquals(97L, node.getAttributeKey(0));
    assertEquals(98L, node.getAttributeKey(1));
    assertEquals(99L, node.getNamespaceKey(0));
    assertEquals(100L, node.getNamespaceKey(1));
  }

}
