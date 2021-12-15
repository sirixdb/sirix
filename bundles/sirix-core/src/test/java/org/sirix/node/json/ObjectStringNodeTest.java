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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.settings.Fixed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Object strjng node test.
 */
public class ObjectStringNodeTest {

  private PageTrx pageTrx;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.openResourceManager(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    // Create empty node.
    final byte[] value = { (byte) 17, (byte) 18 };
    final NodeDelegate del = new NodeDelegate(13, 14, Hashing.sha256(), null, 0, SirixDeweyID.newRootID());
    final ValueNodeDelegate valDel = new ValueNodeDelegate(del, value, false);
    final StructNodeDelegate strucDel = new StructNodeDelegate(del,
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                               0L,
                                                               0L);
    final ObjectStringNode node = new ObjectStringNode(valDel, strucDel);
    node.setHash(node.computeHash());
    check(node);

    // Serialize and deserialize node.
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    node.getKind().serialize(new DataOutputStream(out), node, pageTrx);
    final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    final ObjectStringNode node2 = (ObjectStringNode) NodeKind.OBJECT_STRING_VALUE.deserialize(new DataInputStream(in),
                                                                                               node.getNodeKey(),
                                                                                               null,
                                                                                               pageTrx);
    check(node2);
  }

  private void check(final ObjectStringNode node) {
    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), node.getRightSiblingKey());
    assertEquals(2, node.getRawValue().length);
    assertEquals(NodeKind.OBJECT_STRING_VALUE, node.getKind());
    assertFalse(node.hasFirstChild());
    assertTrue(node.hasParent());
    assertFalse(node.hasLeftSibling());
    assertFalse(node.hasRightSibling());
  }

}
