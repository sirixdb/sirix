/*
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

package io.sirix.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.api.Database;
import io.sirix.api.PageTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import static org.junit.Assert.*;

/**
 * Array node test.
 */
public class ArrayNodeTest {

  private PageTrx pageTrx;

  private Database<JsonResourceSession> database;

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.beginResourceSession(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testNode() {
    final long pathNodeKey = 18;
    
    // Use a simplified ResourceConfiguration without optional fields
    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.NONE)
        .storeChildCount(false)
        .build();
    
    // Create data in the correct serialization format to match ArrayNode CORE_LAYOUT
    // Format: NodeDelegate + pathNodeKey + rightSib + leftSib + firstChild + lastChild
    final BytesOut<?> data = Bytes.elasticHeapByteBuffer();
    data.writeLong(14); // parentKey (offset 0)
    data.writeInt(Constants.NULL_REVISION_NUMBER); // previousRevision (offset 8)
    data.writeInt(0); // lastModifiedRevision (offset 12)
    data.writeLong(pathNodeKey); // pathNodeKey (offset 16) - FIRST in ArrayNode!
    data.writeLong(16L); // rightSibling (offset 24)
    data.writeLong(15L); // leftSibling (offset 32)
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty()); // firstChild (offset 40)
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty()); // lastChild (offset 48)
    
    // Deserialize to create properly initialized node
    final ArrayNode node = (ArrayNode) NodeKind.ARRAY.deserialize(
        data.asBytesIn(), 13L, null, config);
    check(node);

    // Serialize and deserialize node.
    final BytesOut<?> data2 = Bytes.elasticHeapByteBuffer();
    node.getKind().serialize(data2, node, config);
    final ArrayNode node2 = (ArrayNode) NodeKind.ARRAY.deserialize(
        data2.asBytesIn(), node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final ArrayNode node) {
    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(18L, node.getPathNodeKey());

    assertEquals(NodeKind.ARRAY, node.getKind());
    assertFalse(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
  }

}
