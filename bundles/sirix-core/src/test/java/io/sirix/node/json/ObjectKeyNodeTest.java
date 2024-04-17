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

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Object record node test.
 */
public class ObjectKeyNodeTest {

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
  public void testNode() {
    // Create empty node.
    final int nameKey = pageTrx.createNameKey("foobar", NodeKind.OBJECT_KEY);
    final String name = "foobar";

    final long pathNodeKey = 12;
    final NodeDelegate del =
        new NodeDelegate(14, 13, LongHashFunction.xx3(), Constants.NULL_REVISION_NUMBER, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(del, 17L, 16L, 15L, 0L, 0L);
    final ObjectKeyNode node = new ObjectKeyNode(strucDel, nameKey, name, pathNodeKey);
    var bytes = Bytes.elasticHeapByteBuffer();
    node.setHash(node.computeHash(bytes));
    check(node, nameKey);

    // Serialize and deserialize node.
    final Bytes<ByteBuffer> data = Bytes.elasticHeapByteBuffer();
    node.getKind().serialize(data, node, pageTrx.getResourceSession().getResourceConfig());
    final ObjectKeyNode node2 = (ObjectKeyNode) NodeKind.OBJECT_KEY.deserialize(data,
                                                                                node.getNodeKey(),
                                                                                null,
                                                                                pageTrx.getResourceSession()
                                                                                       .getResourceConfig());
    check(node2, nameKey);
  }

  private void check(final ObjectKeyNode node, final int nameKey) {
    // Now compare.
    assertEquals(14L, node.getNodeKey());
    assertEquals(13L, node.getParentKey());
    assertEquals(17L, node.getFirstChildKey());
    assertEquals(16L, node.getRightSiblingKey());

    assertEquals(nameKey, node.getNameKey());
   //assertEquals("foobar", node.getName().getLocalName());
    assertEquals(NodeKind.OBJECT_KEY, node.getKind());
    assertTrue(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertTrue(node.hasRightSibling());
  }

}
