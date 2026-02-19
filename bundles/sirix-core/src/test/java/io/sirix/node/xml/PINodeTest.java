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

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixException;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.settings.Constants;
import io.sirix.utils.NamePageHash;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Processing instruction node test.
 */
public class PINodeTest {

  /**
   * {@link Holder} instance.
   */
  private Holder mHolder;

  /**
   * Sirix {@link StorageEngineReader} instance.
   */
  private StorageEngineReader pageReadTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    mHolder = Holder.generateDeweyIDResourceSession();
    pageReadTrx = mHolder.getResourceSession().beginStorageEngineReader();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    mHolder.close();
  }

  @Test
  public void testProcessInstructionNode() {
    final byte[] value = {(byte) 17, (byte) 18};
    final var config = pageReadTrx.getResourceSession().getResourceConfig();

    // Create PINode with primitive fields
    final PINode node = new PINode(99L, // nodeKey
        13L, // parentKey
        Constants.NULL_REVISION_NUMBER, // previousRevision
        0, // lastModifiedRevision
        16L, // rightSiblingKey
        22L, // leftSiblingKey
        17L, // firstChildKey
        17L, // lastChildKey
        config.storeChildCount()
            ? 1L
            : 0L, // childCount
        config.hashType != HashType.NONE
            ? 1L
            : 0L, // descendantCount
        0L, // hash
        1L, // pathNodeKey
        14, // prefixKey
        15, // localNameKey
        13, // uriKey
        value, // value
        false, // isCompressed
        LongHashFunction.xx3(), // hashFunction
        SirixDeweyID.newRootID(), // deweyID
        new io.brackit.query.atomic.QNm(""));
    var hashBytes = Bytes.elasticOffHeapByteBuffer();
    node.setHash(node.computeHash(hashBytes));

    // Create empty node.
    check(node);

    // Serialize and deserialize node.
    final BytesOut<?> data2 = Bytes.elasticOffHeapByteBuffer();
    node.getKind().serialize(data2, node, pageReadTrx.getResourceSession().getResourceConfig());
    final PINode node2 = (PINode) NodeKind.PROCESSING_INSTRUCTION.deserialize(data2.asBytesIn(), node.getNodeKey(),
        node.getDeweyID().toBytes(), pageReadTrx.getResourceSession().getResourceConfig());
    check(node2);
  }

  private void check(final PINode node) {
    // Now compare.
    assertEquals(99L, node.getNodeKey());
    assertEquals(13L, node.getParentKey());

    assertEquals(17L, node.getFirstChildKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(22L, node.getLeftSiblingKey());
    assertEquals(1, node.getDescendantCount());
    assertEquals(1, node.getChildCount());

    assertEquals(13, node.getURIKey());
    assertEquals(14, node.getPrefixKey());
    assertEquals(15, node.getLocalNameKey());

    Assert.assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    assertEquals(2, node.getRawValue().length);
    Assert.assertEquals(NodeKind.PROCESSING_INSTRUCTION, node.getKind());
    assertTrue(node.hasParent());
    Assert.assertEquals(SirixDeweyID.newRootID(), node.getDeweyID());
  }

}
