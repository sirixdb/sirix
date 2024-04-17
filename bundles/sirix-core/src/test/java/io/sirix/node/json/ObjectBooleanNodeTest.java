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

package io.sirix.node.json;

import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.api.Database;
import io.sirix.api.PageTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Boolean node test.
 */
public class ObjectBooleanNodeTest {

  private PageTrx pageTrx;

  private Database<JsonResourceSession> database;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.beginResourceSession(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    // Create empty node.
    final boolean boolValue = true;
    final NodeDelegate del = new NodeDelegate(13,
                                              14,
                                              LongHashFunction.xx3(), Constants.NULL_REVISION_NUMBER,
                                              0,
                                              SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(del,
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               0L,
                                                               0L);
    final ObjectBooleanNode node = new ObjectBooleanNode(boolValue, strucDel);
    var bytes = Bytes.elasticHeapByteBuffer();
    node.setHash(node.computeHash(bytes));
    check(node);

    // Serialize and deserialize node.
    final Bytes<ByteBuffer> data = Bytes.elasticHeapByteBuffer();
    node.getKind().serialize(data, node, pageTrx.getResourceSession().getResourceConfig());
    final ObjectBooleanNode node2 =
        (ObjectBooleanNode) NodeKind.OBJECT_BOOLEAN_VALUE.deserialize(data, node.getNodeKey(), null, pageTrx.getResourceSession().getResourceConfig());
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
