/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.PageTrx;
import io.sirix.exception.SirixException;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Null node test.
 */
public class NullNodeTest {

  private PageTrx pageTrx;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.beginResourceSession(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @Test
  public void test() throws IOException {
    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.NONE)
        .storeChildCount(false)
        .build();
    
    // Create data in the correct serialization format with size prefix and padding
    // Format: [4-byte size][3-byte padding][NodeDelegate + siblings][end padding]
    final BytesOut<?> data = Bytes.elasticHeapByteBuffer();
    
    long sizePos = data.writePosition();
    data.writeInt(0); // Size placeholder
    data.writeByte(NodeKind.Null.getId()); // NodeKind byte
    long sizePos = data.writePosition();
    data.writeInt(0); // Size placeholder
    data.writeByte((byte) 0); // 3 bytes padding (total header = 8 bytes with NodeKind)
    data.writeByte((byte) 0);
    data.writeByte((byte) 0);
    
    long startPos = data.writePosition();
    // NodeDelegate fields
    data.writeLong(14); // parentKey
    data.writeInt(Constants.NULL_REVISION_NUMBER); // previousRevision
    data.writeInt(0); // lastModifiedRevision
    // Siblings
    data.writeLong(16L); // rightSibling
    data.writeLong(15L); // leftSibling
    
    // Write end padding to make size multiple of 8
    long nodeDataSize = data.writePosition() - startPos;
    int remainder = (int)(nodeDataSize % 8);
    if (remainder != 0) {
      int padding = 8 - remainder;
      for (int i = 0; i < padding; i++) {
        data.writeByte((byte) 0);
      }
    }
    
    // Update size prefix
    long endPos = data.writePosition();
    nodeDataSize = endPos - startPos;
    long currentPos = data.writePosition();
    data.writePosition(sizePos);
    data.writeInt((int) nodeDataSize);
    data.writePosition(currentPos);
    
    // Deserialize to create properly initialized node
    final NullNode node = (NullNode) NodeKind.NULL_VALUE.deserialize(
        data.asBytesIn(), 13L, null, config);
    check(node);

    // Serialize and deserialize node.
    final BytesOut<?> data2 = Bytes.elasticHeapByteBuffer();
    node.getKind().serialize(data2, node, config);
    final NullNode node2 = (NullNode) NodeKind.NULL_VALUE.deserialize(
        data2.asBytesIn(), node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final NullNode node) {
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(15L, node.getLeftSiblingKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(NodeKind.NULL_VALUE, node.getKind());
    assertFalse(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasLeftSibling());
    assertTrue(node.hasRightSibling());
  }

}