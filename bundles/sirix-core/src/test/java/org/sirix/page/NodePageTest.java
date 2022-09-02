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

package org.sirix.page;

import com.google.common.collect.HashBiMap;
import com.google.common.hash.Hashing;
import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixException;
import org.sirix.index.IndexType;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.xml.ElementNode;
import org.sirix.settings.Constants;
import org.sirix.utils.NamePageHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * Node page test.
 */
public final class NodePageTest {

  /**
   * {@link Holder} instance.
   */
  private Holder mHolder;

  /**
   * Sirix {@link PageReadOnlyTrx} instance.
   */
  private PageReadOnlyTrx pageReadTrx;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    mHolder = Holder.generateDeweyIDResourceMgr();
    pageReadTrx = mHolder.getResourceManager().beginPageReadOnlyTrx();
  }

  @After
  public void tearDown() throws SirixException {
    pageReadTrx.close();
    mHolder.close();
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
  }

  @Test
  public void testSerializeDeserialize() throws IOException {
    final UnorderedKeyValuePage page1 = new UnorderedKeyValuePage(0L, IndexType.DOCUMENT, pageReadTrx);
    assertEquals(0L, page1.getPageKey());

    final NodeDelegate del =
        new NodeDelegate(0, 1, Hashing.sha256(), null, Constants.NULL_REVISION_NUMBER, 0, SirixDeweyID.newRootID());
    final StructNodeDelegate strucDel = new StructNodeDelegate(del, 12L, 4L, 3L, 1L, 0L);
    final NameNodeDelegate nameDel = new NameNodeDelegate(del, 5, 6, 7, 1);
    final ElementNode node1 = new ElementNode(strucDel,
                                              nameDel,
                                              new ArrayList<>(),
                                              HashBiMap.create(),
                                              new ArrayList<>(),
                                              new QNm("a", "b", "c"));
    node1.setHash(node1.computeHash());
    node1.insertAttribute(88L, 100);
    node1.insertAttribute(87L, 101);
    node1.insertNamespace(99L);
    node1.insertNamespace(98L);
    assertEquals(0L, node1.getNodeKey());
    page1.setRecord(node1);

    final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();
    final PagePersister pagePersister = new PagePersister();
    pagePersister.serializePage(pageReadTrx, data, page1, SerializationType.DATA);
    final UnorderedKeyValuePage page2 =
        (UnorderedKeyValuePage) pagePersister.deserializePage(pageReadTrx, data, SerializationType.DATA);
    // assertEquals(position, out.position());
    final ElementNode element = (ElementNode) page2.getValue(pageReadTrx, 0L);
    assertEquals(0L, page2.getValue(pageReadTrx, 0L).getNodeKey());
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
    assertEquals(5, ((NameNode) page2.getValue(pageReadTrx, 0L)).getURIKey());
    assertEquals(6, ((NameNode) page2.getValue(pageReadTrx, 0L)).getPrefixKey());
    assertEquals(7, ((NameNode) page2.getValue(pageReadTrx, 0L)).getLocalNameKey());
    assertEquals(NamePageHash.generateHashForString("xs:untyped"), element.getTypeKey());
  }
}
