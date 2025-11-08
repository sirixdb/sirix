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
import io.sirix.node.NodeTestHelper;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Namespace node test.
 */
public class NamespaceNodeTest {

  /**
   * {@link Holder} instance.
   */
  private Holder holder;

  /**
   * Sirix {@link PageReadOnlyTrx} instance.
   */
  private PageReadOnlyTrx pageReadTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    holder = Holder.generateDeweyIDResourceSession();
    pageReadTrx = holder.getResourceSession().beginPageReadOnlyTrx();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    holder.close();
  }

  @Test
  public void testNamespaceNode() {
    // Create data in the correct serialization format with size prefix and padding
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    long sizePos = NodeTestHelper.writeHeader(data, NodeKind.NAMESPACE);
    long startPos = data.writePosition();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(13);                              // parentKey - offset 0
    data.writeInt(Constants.NULL_REVISION_NUMBER);   // previousRevision - offset 8
    data.writeInt(0);                                // lastModifiedRevision - offset 12
    
    // Write NameNode fields (20 bytes)
    data.writeLong(1L);                              // pathNodeKey - offset 16
    data.writeInt(14);                               // prefixKey - offset 24
    data.writeInt(15);                               // localNameKey - offset 28
    data.writeInt(13);                               // uriKey - offset 32
    
    // Finalize
    NodeTestHelper.finalizeSerialization(data, sizePos, startPos);
    
    // Deserialize to create properly initialized node
    var bytesIn = data.asBytesIn();
    bytesIn.readByte(); // Skip NodeKind byte
    final NamespaceNode node = (NamespaceNode) NodeKind.NAMESPACE.deserialize(
        bytesIn, 99L, SirixDeweyID.newRootID().toBytes(), 
        pageReadTrx.getResourceSession().getResourceConfig());
    
    // Compute and set hash
    var hashBytes = Bytes.elasticOffHeapByteBuffer();
    node.setHash(node.computeHash(hashBytes));
    
    check(node);

    // Serialize and deserialize node again
    final BytesOut<?> data2 = Bytes.elasticOffHeapByteBuffer();
    data2.writeByte(NodeKind.NAMESPACE.getId()); // Write NodeKind to ensure proper alignment
    node.getKind().serialize(data2, node, pageReadTrx.getResourceSession().getResourceConfig());
    var bytesIn2 = data2.asBytesIn();
    bytesIn2.readByte(); // Skip NodeKind byte
    final NamespaceNode node2 = (NamespaceNode) NodeKind.NAMESPACE.deserialize(bytesIn2,
                                                                           node.getNodeKey(),
                                                                           node.getDeweyID().toBytes(),
                                                                           pageReadTrx.getResourceSession()
                                                                                      .getResourceConfig());
    check(node2);
  }

  private final void check(final NamespaceNode node) {
    // Now compare.
    assertEquals(99L, node.getNodeKey());
    assertEquals(13L, node.getParentKey());

    assertEquals(13, node.getURIKey());
    assertEquals(14, node.getPrefixKey());
    assertEquals(15, node.getLocalNameKey());
    assertEquals(NodeKind.NAMESPACE, node.getKind());
    assertTrue(node.hasParent());
  }

}
