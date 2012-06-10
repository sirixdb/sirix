/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.shredder;

import java.io.File;
import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.XMLEvent;

import org.custommonkey.xmlunit.XMLTestCase;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.api.INodeWriteTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.AbsTTException;
import org.sirix.node.ENode;
import org.sirix.node.ElementNode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.utils.DocumentCreater;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class XMLShredderTest extends XMLTestCase {

  public static final String XML = "src" + File.separator + "test" + File.separator + "resources"
    + File.separator + "test.xml";

  public static final String XML2 = "src" + File.separator + "test" + File.separator + "resources"
    + File.separator + "test2.xml";

  public static final String XML3 = "src" + File.separator + "test" + File.separator + "resources"
    + File.separator + "test3.xml";

  private Holder holder;

  @Override
  @Before
  public void setUp() throws AbsTTException {
    TestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @Override
  @After
  public void tearDown() throws AbsTTException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testSTAXShredder() throws Exception {

    // Setup parsed session.
    XMLShredder.main(XML, PATHS.PATH2.getFile().getAbsolutePath());
    final INodeReadTrx expectedTrx = holder.getWtx();

    // Verify.
    final IDatabase database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());
    database2.createResource(new ResourceConfiguration.Builder(TestHelper.RESOURCE, PATHS.PATH2.getConfig())
      .build());
    final ISession session =
      database2.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build());
    final INodeReadTrx rtx = session.beginNodeReadTrx();
    rtx.moveToDocumentRoot();
    final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);
    final Iterator<Long> descendants = new DescendantAxis(rtx);

    while (expectedDescendants.hasNext() && descendants.hasNext()) {
      final IStructNode expDesc = expectedTrx.getStructuralNode();
      final IStructNode desc = rtx.getStructuralNode();
      assertEquals(expDesc.getNodeKey(), desc.getNodeKey());
      assertEquals(expDesc.getParentKey(), desc.getParentKey());
      assertEquals(expDesc.getFirstChildKey(), desc.getFirstChildKey());
      assertEquals(expDesc.getLeftSiblingKey(), desc.getLeftSiblingKey());
      assertEquals(expDesc.getRightSiblingKey(), desc.getRightSiblingKey());
      assertEquals(expDesc.getChildCount(), desc.getChildCount());
      if (expDesc.getKind() == ENode.ELEMENT_KIND || desc.getKind() == ENode.ELEMENT_KIND) {

        assertEquals(((ElementNode)expDesc).getAttributeCount(), ((ElementNode)desc).getAttributeCount());
        assertEquals(((ElementNode)expDesc).getNamespaceCount(), ((ElementNode)desc).getNamespaceCount());
      }
      assertEquals(expDesc.getKind(), desc.getKind());
      assertEquals(expectedTrx.getQNameOfCurrentNode(), rtx.getQNameOfCurrentNode());
      assertEquals(expectedTrx.getValueOfCurrentNode(), expectedTrx.getValueOfCurrentNode());
    }

    rtx.close();
    session.close();
    database2.close();
    expectedTrx.close();
  }

  @Test
  public void testShredIntoExisting() throws Exception {

    final INodeWriteTrx wtx = holder.getWtx();
    final XMLShredder shredder =
      new XMLShredder(wtx, XMLShredder.createFileReader(new File(XML)), EInsert.ASFIRSTCHILD);
    shredder.call();
    assertEquals(1, wtx.getRevisionNumber());
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.remove();
    final XMLShredder shredder2 =
      new XMLShredder(wtx, XMLShredder.createFileReader(new File(XML)), EInsert.ASFIRSTCHILD);
    shredder2.call();
    assertEquals(2, wtx.getRevisionNumber());
    wtx.close();

    // Setup expected session.
    final IDatabase database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());
    final ISession expectedSession =
      database2.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build());

    final INodeWriteTrx expectedTrx = expectedSession.beginNodeWriteTrx();
    DocumentCreater.create(expectedTrx);
    expectedTrx.commit();
    expectedTrx.moveToDocumentRoot();

    // Verify.
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();

    final Iterator<Long> descendants = new DescendantAxis(rtx);
    final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);

    while (expectedDescendants.hasNext()) {
      expectedDescendants.next();
      descendants.hasNext();
      descendants.next();
      assertEquals(expectedTrx.getQNameOfCurrentNode(), rtx.getQNameOfCurrentNode());
    }

    // expectedTrx.moveToDocumentRoot();
    // final Iterator<Long> expectedDescendants2 = new DescendantAxis(expectedTrx);
    // while (expectedDescendants2.hasNext()) {
    // expectedDescendants2.next();
    // descendants.hasNext();
    // descendants.next();
    // assertEquals(expectedTrx.getQNameOfCurrentNode(), rtx.getQNameOfCurrentNode());
    // }

    expectedTrx.close();
    expectedSession.close();
    rtx.close();
  }

  @Test
  public void testAttributesNSPrefix() throws Exception {
    // Setup expected session.
    final INodeWriteTrx expectedTrx2 = holder.getWtx();
    DocumentCreater.createWithoutNamespace(expectedTrx2);
    expectedTrx2.commit();

    // Setup parsed session.
    final IDatabase database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());
    final ISession session2 =
      database2.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build());
    final INodeWriteTrx wtx = session2.beginNodeWriteTrx();
    final XMLShredder shredder =
      new XMLShredder(wtx, XMLShredder.createFileReader(new File(XML2)), EInsert.ASFIRSTCHILD);
    shredder.call();
    wtx.commit();
    wtx.close();

    // Verify.
    final INodeReadTrx rtx = session2.beginNodeReadTrx();
    rtx.moveToDocumentRoot();
    final Iterator<Long> expectedAttributes = new DescendantAxis(expectedTrx2);
    final Iterator<Long> attributes = new DescendantAxis(rtx);

    while (expectedAttributes.hasNext() && attributes.hasNext()) {
      expectedAttributes.next();
      attributes.next();
      if (expectedTrx2.getNode().getKind() == ENode.ELEMENT_KIND
        || rtx.getNode().getKind() == ENode.ELEMENT_KIND) {
        assertEquals(((ElementNode)expectedTrx2.getNode()).getNamespaceCount(), ((ElementNode)rtx.getNode())
          .getNamespaceCount());
        assertEquals(((ElementNode)expectedTrx2.getNode()).getAttributeCount(), ((ElementNode)rtx.getNode())
          .getAttributeCount());
        for (int i = 0; i < ((ElementNode)expectedTrx2.getNode()).getAttributeCount(); i++) {
          assertEquals(expectedTrx2.getQNameOfCurrentNode(), rtx.getQNameOfCurrentNode());
        }
      }
    }
    attributes.hasNext();

    assertEquals(expectedAttributes.hasNext(), attributes.hasNext());

    expectedTrx2.close();
    rtx.close();
    session2.close();
  }

  @Test
  public void testShreddingLargeText() throws Exception {
    final IDatabase database = TestHelper.getDatabase(PATHS.PATH2.getFile());
    final ISession session =
      database.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build());
    final INodeWriteTrx wtx = session.beginNodeWriteTrx();
    final XMLShredder shredder =
      new XMLShredder(wtx, XMLShredder.createFileReader(new File(XML3)), EInsert.ASFIRSTCHILD);
    shredder.call();
    wtx.close();

    final INodeReadTrx rtx = session.beginNodeReadTrx();
    assertTrue(rtx.moveToFirstChild());
    assertTrue(rtx.moveToFirstChild());

    final StringBuilder tnkBuilder = new StringBuilder();
    do {
      tnkBuilder.append(rtx.getValueOfCurrentNode());
    } while (rtx.moveToRightSibling());

    final String tnkString = tnkBuilder.toString();

    rtx.close();
    session.close();

    final XMLEventReader validater = XMLShredder.createFileReader(new File(XML3));
    final StringBuilder xmlBuilder = new StringBuilder();
    while (validater.hasNext()) {
      final XMLEvent event = validater.nextEvent();
      switch (event.getEventType()) {
      case XMLStreamConstants.CHARACTERS:
        final String text = event.asCharacters().getData().trim();
        if (text.length() > 0) {
          xmlBuilder.append(text);
        }
        break;
      }
    }

    assertEquals(xmlBuilder.toString(), tnkString);
  }
}
