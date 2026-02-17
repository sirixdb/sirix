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

import io.sirix.access.trx.node.HashType;
import io.sirix.node.Bytes;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import io.sirix.node.BytesOut;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixException;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.settings.Constants;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
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
   * Sirix {@link StorageEngineReader} instance.
   */
  private StorageEngineReader pageReadTrx;

  private Arena arena;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateDeweyIDResourceSession();
    pageReadTrx = holder.getResourceSession().beginPageReadOnlyTrx();
    arena = Arena.ofConfined();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    holder.close();
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    arena.close();
  }

  @Test
  public void testSerializeDeserialize() throws IOException {
    final KeyValueLeafPage page1 =
        new KeyValueLeafPage(0L, IndexType.DOCUMENT, pageReadTrx.getResourceSession().getResourceConfig(),
            pageReadTrx.getRevisionNumber(), arena.allocate(SIXTYFOUR_KB), null);
    KeyValueLeafPage page2 = null;
    try {
      assertEquals(0L, page1.getPageKey());

      // Create ResourceConfiguration for testing
      final var config = pageReadTrx.getResourceSession().getResourceConfig();

      // Create ElementNode with primitive fields
      final LongArrayList attributeKeys = new LongArrayList();
      attributeKeys.add(88L);
      attributeKeys.add(87L);
      final LongArrayList namespaceKeys = new LongArrayList();
      namespaceKeys.add(99L);
      namespaceKeys.add(98L);

      final ElementNode node1 = new ElementNode(0L, // nodeKey
          1L, // parentKey
          Constants.NULL_REVISION_NUMBER, // previousRevision
          0, // lastModifiedRevision
          4L, // rightSiblingKey
          3L, // leftSiblingKey
          12L, // firstChildKey
          12L, // lastChildKey
          config.storeChildCount()
              ? 1L
              : 0L, // childCount
          0L, // descendantCount
          0L, // hash
          1L, // pathNodeKey
          6, // prefixKey
          7, // localNameKey
          5, // uriKey
          config.nodeHashFunction, // hashFunction
          SirixDeweyID.newRootID(), // deweyID
          attributeKeys, // attributeKeys
          namespaceKeys, // namespaceKeys
          new QNm("a", "b", "c"));

      // Compute and set hash
      var bytes = Bytes.elasticOffHeapByteBuffer();
      node1.setHash(node1.computeHash(bytes));

      assertEquals(0L, node1.getNodeKey());
      page1.setRecord(node1);

      final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
      final PagePersister pagePersister = new PagePersister();
      pagePersister.serializePage(pageReadTrx.getResourceSession().getResourceConfig(), data, page1,
          SerializationType.DATA);
      page2 = (KeyValueLeafPage) pagePersister.deserializePage(pageReadTrx.getResourceSession().getResourceConfig(),
          Bytes.wrapForRead(data.toByteArray()), SerializationType.DATA);
      // assertEquals(position, out.position());
      final ElementNode element = (ElementNode) pageReadTrx.getValue(page2, 0L);

      assertEquals(0L, pageReadTrx.getValue(page2, 0L).getNodeKey());
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
      assertEquals(5, ((NameNode) pageReadTrx.getValue(page2, 0L)).getURIKey());
      assertEquals(6, ((NameNode) pageReadTrx.getValue(page2, 0L)).getPrefixKey());
      assertEquals(7, ((NameNode) pageReadTrx.getValue(page2, 0L)).getLocalNameKey());
      // typeKey is not persisted, so we don't test it
    } finally {
      // Close pages to prevent page leaks
      page1.close();
      if (page2 != null) {
        page2.close();
      }
    }
  }
}
