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

import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.utils.NamePageHash;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixException;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.settings.Constants;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Attribute node test.
 */
public class AttributeNodeTest {

  /**
   * {@link Holder} instance.
   */
  private Holder holder;

  /**
   * Sirix {@link StorageEngineReader} instance.
   */
  private StorageEngineReader pageReadOnlyTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    holder = Holder.generateDeweyIDResourceSession();
    pageReadOnlyTrx = holder.getResourceSession().beginStorageEngineReader();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadOnlyTrx.close();
    holder.close();
  }

  @Test
  public void testAttributeNode() {
    final byte[] value = {(byte) 17, (byte) 18};

    // Create AttributeNode with primitive fields
    final AttributeNode node = new AttributeNode(99L, // nodeKey
        13L, // parentKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        1L, // pathNodeKey
        14, // prefixKey
        15, // localNameKey
        13, // uriKey
        0, // hash
        value, // value
        LongHashFunction.xx3(), // hashFunction
        SirixDeweyID.newRootID(), // deweyID
        new QNm("ns", "a", "p"));
    var hashBytes = Bytes.elasticOffHeapByteBuffer();
    node.setHash(node.computeHash(hashBytes));

    // Create empty node.
    check(node);

    // Serialize and deserialize node.
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    data.writeByte(NodeKind.ATTRIBUTE.getId()); // Write NodeKind byte
    node.getKind().serialize(data, node, pageReadOnlyTrx.getResourceSession().getResourceConfig());
    var bytesIn = data.asBytesIn();
    bytesIn.readByte(); // Skip NodeKind byte
    final AttributeNode node2 = (AttributeNode) NodeKind.ATTRIBUTE.deserialize(bytesIn, node.getNodeKey(),
        node.getDeweyID().toBytes(), pageReadOnlyTrx.getResourceSession().getResourceConfig());
    check(node2);
  }

  private final void check(final AttributeNode node) {
    // Now compare.
    assertEquals(99L, node.getNodeKey());
    assertEquals(13L, node.getParentKey());

    assertEquals(13, node.getURIKey());
    assertEquals(14, node.getPrefixKey());
    assertEquals(15, node.getLocalNameKey());

    assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    assertEquals(2, node.getRawValue().length);
    assertEquals(NodeKind.ATTRIBUTE, node.getKind());
    assertEquals(true, node.hasParent());
    assertEquals(NodeKind.ATTRIBUTE, node.getKind());
    assertEquals(SirixDeweyID.newRootID(), node.getDeweyID());
  }

}
