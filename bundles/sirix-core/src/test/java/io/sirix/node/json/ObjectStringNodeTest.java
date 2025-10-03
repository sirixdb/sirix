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

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.api.PageTrx;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Object strjng node test.
 */
public class ObjectStringNodeTest {

  private PageTrx pageTrx;

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    pageTrx = database.beginResourceSession(JsonTestHelper.RESOURCE).beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    final byte[] value = { (byte) 17, (byte) 18 };
    
    // Create data - Object value nodes only have parent (no siblings)
    final BytesOut<?> data = Bytes.elasticHeapByteBuffer();
    data.writeLong(14); // parentKey
    data.writeInt(Constants.NULL_REVISION_NUMBER); // previousRevision
    data.writeInt(0); // lastModifiedRevision
    data.writeStopBit(value.length); // string length
    data.write(value); // string value
    
    // Deserialize to create properly initialized node
    final ObjectStringNode node = (ObjectStringNode) NodeKind.OBJECT_STRING_VALUE.deserialize(
        data.asBytesIn(), 13L, null, pageTrx.getResourceSession().getResourceConfig());
    check(node);

    // Serialize and deserialize node.
    final BytesOut<?> data2 = Bytes.elasticHeapByteBuffer();
    node.getKind().serialize(data2, node, pageTrx.getResourceSession().getResourceConfig());
    final ObjectStringNode node2 = (ObjectStringNode) NodeKind.OBJECT_STRING_VALUE.deserialize(
        data2.asBytesIn(), node.getNodeKey(), null, pageTrx.getResourceSession().getResourceConfig());
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
