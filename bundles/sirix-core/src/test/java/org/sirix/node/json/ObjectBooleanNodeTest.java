/*
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

package org.sirix.node.json;

import com.google.common.hash.Hashing;
import net.openhft.chronicle.bytes.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.api.Database;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.settings.Fixed;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Boolean node test.
 */
public class ObjectBooleanNodeTest {

  private PageTrx pageTrx;

  private Database<JsonResourceManager> database;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.openResourceManager(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    // Create empty node.
    final boolean boolValue = true;
    final NodeDelegate del = new NodeDelegate(13, 14, Hashing.sha256(), null, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(del,
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               0L, 0L);
    final ObjectBooleanNode node = new ObjectBooleanNode(boolValue, strucDel);
    node.setHash(node.computeHash());
    check(node);

    // Serialize and deserialize node.
    final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();
    node.getKind().serialize(data, node, pageTrx);
    final ObjectBooleanNode node2 =
        (ObjectBooleanNode) NodeKind.OBJECT_BOOLEAN_VALUE.deserialize(data,
                                                                      node.getNodeKey(),
                                                                      null,
                                                                      pageTrx);
    check(node2);
  }

  private void check(final ObjectBooleanNode node) {
    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getRightSiblingKey());
    assertTrue(node.getValue());
    assertEquals(NodeKind.OBJECT_BOOLEAN_VALUE, node.getKind());
    assertFalse(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertFalse(node.hasLeftSibling());
    assertFalse(node.hasRightSibling());
  }

}
