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
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
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

    // Create data in the structural delta/varint format.
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();

    final long nodeKey = 13L;
    data.writeByte(NodeKind.ELEMENT.getId());
    DeltaVarIntCodec.encodeDelta(data, 14L, nodeKey); // parentKey
    DeltaVarIntCodec.encodeDelta(data, 17L, nodeKey); // rightSiblingKey
    DeltaVarIntCodec.encodeDelta(data, 16L, nodeKey); // leftSiblingKey
    DeltaVarIntCodec.encodeDelta(data, 12L, nodeKey); // firstChildKey
    DeltaVarIntCodec.encodeDelta(data, 12L, nodeKey); // lastChildKey
    DeltaVarIntCodec.encodeDelta(data, 1L, nodeKey);  // pathNodeKey
    DeltaVarIntCodec.encodeSigned(data, 18);          // prefixKey
    DeltaVarIntCodec.encodeSigned(data, 19);          // localNameKey
    DeltaVarIntCodec.encodeSigned(data, 17);          // uriKey
    DeltaVarIntCodec.encodeSigned(data, Constants.NULL_REVISION_NUMBER);
    DeltaVarIntCodec.encodeSigned(data, 0);           // lastModifiedRevision

    if (config.storeChildCount()) {
      DeltaVarIntCodec.encodeSigned(data, 1);        // childCount
    }
    if (config.hashType != HashType.NONE) {
      data.writeLong(0);                             // hash
      DeltaVarIntCodec.encodeSigned(data, 0);        // descendantCount
    }
    DeltaVarIntCodec.encodeSigned(data, 2);          // attribute count
    DeltaVarIntCodec.encodeDelta(data, 97L, nodeKey);
    DeltaVarIntCodec.encodeDelta(data, 98L, nodeKey);
    DeltaVarIntCodec.encodeSigned(data, 2);          // namespace count
    DeltaVarIntCodec.encodeDelta(data, 99L, nodeKey);
    DeltaVarIntCodec.encodeDelta(data, 100L, nodeKey);
    
    // Deserialize to create properly initialized node
    var bytesIn = data.asBytesIn();
    bytesIn.readByte(); // Skip NodeKind byte
    final ElementNode node = (ElementNode) NodeKind.ELEMENT.deserialize(
        bytesIn, nodeKey, SirixDeweyID.newRootID().toBytes(), config);
    
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
