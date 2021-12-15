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

package org.sirix.node.json;

import com.google.common.hash.Hashing;
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

import java.io.*;

import static org.junit.Assert.*;

/**
 * Object node test.
 */
public class ObjectNodeTest {

  private PageTrx pageTrx;

  private Database<JsonResourceManager> database;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    pageTrx = database.openResourceManager(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testNode() throws IOException {
    final NodeDelegate del = new NodeDelegate(13, 14, Hashing.sha256(), null, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel =
        new StructNodeDelegate(del, Fixed.NULL_NODE_KEY.getStandardProperty(), 16L, 15L, 0L, 0L);
    final ObjectNode node = new ObjectNode(strucDel);
    node.setHash(node.computeHash());
    check(node);

    // Serialize and deserialize node.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    node.getKind().serialize(new DataOutputStream(out), node, pageTrx);
    final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    final ObjectNode node2 =
        (ObjectNode) NodeKind.OBJECT.deserialize(new DataInputStream(in), node.getNodeKey(), null, pageTrx);
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
