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

package io.sirix.page;

import io.brackit.query.atomic.QNm;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.PageTrx;
import io.sirix.exception.SirixException;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.settings.Constants;
import io.sirix.utils.NamePageHash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.hashing.LongHashFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Node page test.
 */
public final class NodePageTest {

  /**
   * {@link Holder} instance.
   */
  private Holder holder;

  /**
   * Sirix {@link PageTrx} instance.
   */
  private PageTrx pageTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateDeweyIDResourceMgr();
    pageTrx = holder.getResourceManager().beginPageTrx();
  }

  @After
  public void tearDown() throws SirixException {
    pageTrx.close();
    holder.close();
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
  }

  @Test
  public void testSerializeDeserialize() throws IOException {
    final KeyValueLeafPage page1 = new KeyValueLeafPage(0L, IndexType.DOCUMENT, pageTrx);
    assertEquals(0L, page1.getPageKey());

    final NodeDelegate del =
        new NodeDelegate(0, 1, LongHashFunction.xx3(), Constants.NULL_REVISION_NUMBER, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(del, 12L, 4L, 3L, 1L, 0L);
    final NameNodeDelegate nameDel = new NameNodeDelegate(del, 5, 6, 7, 1);
    final ElementNode node1 =
        new ElementNode(strucDel, nameDel, new LongArrayList(), new LongArrayList(), new QNm("a", "b", "c"));
    var bytes = Bytes.elasticHeapByteBuffer();
    node1.setHash(node1.computeHash(bytes));
    node1.insertAttribute(88L);
    node1.insertAttribute(87L);
    node1.insertNamespace(99L);
    node1.insertNamespace(98L);
    assertEquals(0L, node1.getNodeKey());
    page1.setRecord(node1);

    final Bytes<ByteBuffer> data = Bytes.elasticHeapByteBuffer();
    final PagePersister pagePersister = new PagePersister();
    pagePersister.serializePage(pageTrx, data, page1, SerializationType.DATA);

    byte[] deserializedBytes;
    try (final var inputStream = pageTrx.getResourceSession()
                                        .getResourceConfig().byteHandlePipeline.deserialize(new ByteArrayInputStream(
            data.toByteArray()))) {
      deserializedBytes = inputStream.readAllBytes();
    }

    final KeyValueLeafPage page2 = (KeyValueLeafPage) pagePersister.deserializePage(pageTrx,
                                                                                    Bytes.wrapForRead(deserializedBytes),
                                                                                    SerializationType.DATA);
    // assertEquals(position, out.position());
    final ElementNode element = (ElementNode) pageTrx.getValue(page2, 0L);

    assertEquals(0L, pageTrx.getValue(page2, 0L).getNodeKey());
    assertEquals(1L, element.getParentKey());
    assertEquals(12L, element.getFirstChildKey());
    assertEquals(3L, element.getLeftSiblingKey());
    assertEquals(4L, element.getRightSiblingKey());
    assertEquals(1, element.getChildCount());
    assertEquals(2, element.getAttributeCount());
    assertEquals(2, element.getNamespaceCount());
    assertEquals(88L, element.getAttributeKey(0));
    assertEquals(87L, element.getAttributeKey(1));
    assertEquals(99L, element.getNamespaceKey(0));
    assertEquals(98L, element.getNamespaceKey(1));
    assertEquals(5, ((NameNode) pageTrx.getValue(page2, 0L)).getURIKey());
    assertEquals(6, ((NameNode) pageTrx.getValue(page2, 0L)).getPrefixKey());
    assertEquals(7, ((NameNode) pageTrx.getValue(page2, 0L)).getLocalNameKey());
    assertEquals(NamePageHash.generateHashForString("xs:untyped"), element.getTypeKey());
  }
}
