/**
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

package org.sirix.service.xml.shredder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.XmlTestHelper.PATHS;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.service.InsertPosition;
import org.sirix.utils.XmlDocumentCreator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.XMLEvent;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class XmlShredderTest {

  public static final Path XML = Paths.get("src", "test", "resources", "test.xml");

  public static final Path XML2 = Paths.get("src", "test", "resources", "test2.xml");

  public static final Path XML3 = Paths.get("src", "test", "resources", "test3.xml");

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testSTAXShredder() throws Exception {
    // Setup parsed session.
    XmlShredder.main(XML.toAbsolutePath().toString(), PATHS.PATH2.getFile().toAbsolutePath().toString());
    final XmlNodeReadOnlyTrx expectedTrx = holder.getXdmNodeWriteTrx();

    // Verify.
    final var database2 = XmlTestHelper.getDatabase(PATHS.PATH2.getFile());
    database2.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).build());
    final XmlResourceSession manager = database2.beginResourceSession(XmlTestHelper.RESOURCE);
    final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx();
    rtx.moveToDocumentRoot();
    final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);
    final Iterator<Long> descendants = new DescendantAxis(rtx);

    while (expectedDescendants.hasNext() && descendants.hasNext()) {
      Assert.assertEquals(expectedTrx.getNodeKey(), rtx.getNodeKey());
      Assert.assertEquals(expectedTrx.getParentKey(), rtx.getParentKey());
      Assert.assertEquals(expectedTrx.getFirstChildKey(), rtx.getFirstChildKey());
      Assert.assertEquals(expectedTrx.getLeftSiblingKey(), rtx.getLeftSiblingKey());
      Assert.assertEquals(expectedTrx.getRightSiblingKey(), rtx.getRightSiblingKey());
      Assert.assertEquals(expectedTrx.getChildCount(), rtx.getChildCount());
      if (expectedTrx.getKind() == NodeKind.ELEMENT || rtx.getKind() == NodeKind.ELEMENT) {

        Assert.assertEquals(expectedTrx.getAttributeCount(), rtx.getAttributeCount());
        Assert.assertEquals(expectedTrx.getNamespaceCount(), rtx.getNamespaceCount());
      }
      Assert.assertEquals(expectedTrx.getKind(), rtx.getKind());
      Assert.assertEquals(expectedTrx.getName(), rtx.getName());
      Assert.assertEquals(expectedTrx.getValue(), expectedTrx.getValue());
    }

    rtx.close();
    manager.close();
    database2.close();
    expectedTrx.close();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void testShredIntoExisting() throws Exception {
    try (final XmlNodeTrx wtx = holder.getXdmNodeWriteTrx();
        final FileInputStream fis1 = new FileInputStream(XML.toFile());
        final FileInputStream fis2 = new FileInputStream(XML.toFile())) {
      final XmlShredder shredder = new XmlShredder.Builder(wtx, XmlShredder.createFileReader(fis1),
          InsertPosition.AS_FIRST_CHILD).includeComments(true).commitAfterwards().build();
      shredder.call();
      Assert.assertEquals(2, wtx.getRevisionNumber());
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.remove();
      final XmlShredder shredder2 = new XmlShredder.Builder(wtx, XmlShredder.createFileReader(fis2),
          InsertPosition.AS_FIRST_CHILD).includeComments(true).commitAfterwards().build();
      shredder2.call();
      Assert.assertEquals(3, wtx.getRevisionNumber());
    }

    // Setup expected.
    final var database2 = XmlTestHelper.getDatabase(PATHS.PATH2.getFile());

    try (final XmlResourceSession manager = database2.beginResourceSession(XmlTestHelper.RESOURCE);
         final XmlNodeTrx expectedTrx = manager.beginNodeTrx()) {
      XmlDocumentCreator.create(expectedTrx);
      expectedTrx.commit();
      expectedTrx.moveToDocumentRoot();

      // Verify.
      try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx()) {

        final Iterator<Long> descendants = new DescendantAxis(rtx);
        final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);

        while (expectedDescendants.hasNext()) {
          expectedDescendants.next();
          descendants.hasNext();
          descendants.next();
          Assert.assertEquals(expectedTrx.getName(), rtx.getName());
          Assert.assertEquals(expectedTrx.getValue(), rtx.getValue());
        }
      }
    }
  }

  @Test
  public void testAttributesNSPrefix() throws Exception {
    // Setup expected.
    final XmlNodeTrx expectedTrx2 = holder.getXdmNodeWriteTrx();
    XmlDocumentCreator.createWithoutNamespace(expectedTrx2);
    expectedTrx2.commit();

    // Setup parsed session.
    final var database2 = XmlTestHelper.getDatabase(PATHS.PATH2.getFile());

    try (final var manager2 = database2.beginResourceSession(XmlTestHelper.RESOURCE);
         final XmlNodeTrx wtx = manager2.beginNodeTrx();
         final FileInputStream fis = new FileInputStream(XML2.toFile())) {
      final XmlShredder shredder = new XmlShredder.Builder(wtx, XmlShredder.createFileReader(fis),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
      wtx.commit();

      // Verify.
      try (final XmlNodeReadOnlyTrx rtx = manager2.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        final Iterator<Long> expectedAttributes = new DescendantAxis(expectedTrx2);
        final Iterator<Long> attributes = new DescendantAxis(rtx);

        while (expectedAttributes.hasNext() && attributes.hasNext()) {
          expectedAttributes.next();
          attributes.next();
          if (expectedTrx2.getKind() == NodeKind.ELEMENT || rtx.getKind() == NodeKind.ELEMENT) {
            Assert.assertEquals(expectedTrx2.getNamespaceCount(), rtx.getNamespaceCount());
            Assert.assertEquals(expectedTrx2.getAttributeCount(), rtx.getAttributeCount());
            for (int i = 0; i < expectedTrx2.getAttributeCount(); i++) {
              Assert.assertEquals(expectedTrx2.getName(), rtx.getName());
            }
          }
        }
        //noinspection ResultOfMethodCallIgnored
        attributes.hasNext();

        Assert.assertEquals(expectedAttributes.hasNext(), attributes.hasNext());

        expectedTrx2.close();
      }
    }
  }

  @Test
  public void testShreddingLargeText() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH2.getFile());
    try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final FileInputStream fis1 = new FileInputStream(XML3.toFile());
         final FileInputStream fis2 = new FileInputStream(XML3.toFile())) {
      try (final XmlNodeTrx wtx = manager.beginNodeTrx()) {
        final XmlShredder shredder = new XmlShredder.Builder(wtx, XmlShredder.createFileReader(fis1),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }

      final StringBuilder tnkBuilder = new StringBuilder();

      try (final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        Assert.assertTrue(rtx.moveToFirstChild());
        Assert.assertTrue(rtx.moveToFirstChild());


        do {
          tnkBuilder.append(rtx.getValue());
        } while (rtx.moveToRightSibling());
      }

      final String tnkString = tnkBuilder.toString();

      final XMLEventReader validater = XmlShredder.createFileReader(fis2);
      final StringBuilder xmlBuilder = new StringBuilder();
      while (validater.hasNext()) {
        final XMLEvent event = validater.nextEvent();
        if (event.getEventType() == XMLStreamConstants.CHARACTERS) {
          final String text = event.asCharacters().getData().trim();
          if (text.length() > 0) {
            xmlBuilder.append(text);
          }
        }
      }

      Assert.assertEquals(xmlBuilder.toString(), tnkString);
    }
  }
}
