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
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Object node test.
 */
public class ObjectNodeTest {

  private StorageEngineWriter pageTrx;

  private Database<JsonResourceSession> database;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    pageTrx = database.beginResourceSession(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testNode() throws IOException {
    // Create node directly with primitive constructor
    final var hashFunction = LongHashFunction.xx3();
    final ObjectNode node = new ObjectNode(
        13L, // nodeKey
        14L, // parentKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        16L, // rightSiblingKey
        15L, // leftSiblingKey
        Fixed.NULL_NODE_KEY.getStandardProperty(), // firstChildKey
        Fixed.NULL_NODE_KEY.getStandardProperty(), // lastChildKey
        0, // childCount
        0, // descendantCount
        0, // hash
        hashFunction,
        (byte[]) null // deweyID
    );
    check(node);

    // Use a simplified ResourceConfiguration without optional fields
    final var config = ResourceConfiguration.newBuilder("test")
        .hashKind(HashType.NONE)
        .storeChildCount(false)
        .build();

    // Serialize and deserialize node.
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    NodeKind.OBJECT.serialize(data, node, config);
    
    var bytesIn = data.asBytesIn();
    final ObjectNode node2 = (ObjectNode) NodeKind.OBJECT.deserialize(
        bytesIn, node.getNodeKey(), null, config);
    check(node2);
  }

  private void check(final ObjectNode node) {
    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(16L, node.getRightSiblingKey());

    assertEquals(NodeKind.OBJECT, node.getKind());
    assertFalse(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
  }

}
