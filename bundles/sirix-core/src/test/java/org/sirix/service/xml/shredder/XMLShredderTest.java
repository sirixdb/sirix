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

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.XMLEvent;
import org.custommonkey.xmlunit.XMLTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.utils.DocumentCreator;

public class XMLShredderTest extends XMLTestCase {

  public static final Path XML = Paths.get("src", "test", "resources", "test.xml");

  public static final Path XML2 = Paths.get("src", "test", "resources", "test2.xml");

  public static final Path XML3 = Paths.get("src", "test", "resources", "test3.xml");

  private Holder holder;

  @Override
  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @Override
  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testSTAXShredder() throws Exception {
    // Setup parsed session.
    XMLShredder.main(XML.toAbsolutePath().toString(), PATHS.PATH2.getFile().toAbsolutePath().toString());
    final XdmNodeReadTrx expectedTrx = holder.getXdmNodeWriteTrx();

    // Verify.
    final Database database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());
    database2.createResource(new ResourceConfiguration.Builder(TestHelper.RESOURCE, PATHS.PATH2.getConfig()).build());
    final XdmResourceManager manager = database2.getXdmResourceManager(TestHelper.RESOURCE);
    final XdmNodeReadTrx rtx = manager.beginNodeReadTrx();
    rtx.moveToDocumentRoot();
    final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);
    final Iterator<Long> descendants = new DescendantAxis(rtx);

    while (expectedDescendants.hasNext() && descendants.hasNext()) {
      assertEquals(expectedTrx.getNodeKey(), rtx.getNodeKey());
      assertEquals(expectedTrx.getParentKey(), rtx.getParentKey());
      assertEquals(expectedTrx.getFirstChildKey(), rtx.getFirstChildKey());
      assertEquals(expectedTrx.getLeftSiblingKey(), rtx.getLeftSiblingKey());
      assertEquals(expectedTrx.getRightSiblingKey(), rtx.getRightSiblingKey());
      assertEquals(expectedTrx.getChildCount(), rtx.getChildCount());
      if (expectedTrx.getKind() == Kind.ELEMENT || rtx.getKind() == Kind.ELEMENT) {

        assertEquals(expectedTrx.getAttributeCount(), rtx.getAttributeCount());
        assertEquals(expectedTrx.getNamespaceCount(), rtx.getNamespaceCount());
      }
      assertEquals(expectedTrx.getKind(), rtx.getKind());
      assertEquals(expectedTrx.getName(), rtx.getName());
      assertEquals(expectedTrx.getValue(), expectedTrx.getValue());
    }

    rtx.close();
    manager.close();
    database2.close();
    expectedTrx.close();
  }

  @Test
  public void testShredIntoExisting() throws Exception {
    try (final XdmNodeWriteTrx wtx = holder.getXdmNodeWriteTrx();
        final FileInputStream fis1 = new FileInputStream(XML.toFile());
        final FileInputStream fis2 = new FileInputStream(XML.toFile())) {
      final XMLShredder shredder =
          new XMLShredder.Builder(wtx, XMLShredder.createFileReader(fis1), Insert.ASFIRSTCHILD).includeComments(true)
                                                                                               .commitAfterwards()
                                                                                               .build();
      shredder.call();
      assertEquals(2, wtx.getRevisionNumber());
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.remove();
      final XMLShredder shredder2 =
          new XMLShredder.Builder(wtx, XMLShredder.createFileReader(fis2), Insert.ASFIRSTCHILD).includeComments(true)
                                                                                               .commitAfterwards()
                                                                                               .build();
      shredder2.call();
      assertEquals(3, wtx.getRevisionNumber());
    }

    // Setup expected.
    final Database database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());

    try (final XdmResourceManager manager = database2.getXdmResourceManager(TestHelper.RESOURCE);
        final XdmNodeWriteTrx expectedTrx = manager.beginNodeWriteTrx()) {
      DocumentCreator.create(expectedTrx);
      expectedTrx.commit();
      expectedTrx.moveToDocumentRoot();

      // Verify.
      try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx()) {

        final Iterator<Long> descendants = new DescendantAxis(rtx);
        final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);

        while (expectedDescendants.hasNext()) {
          expectedDescendants.next();
          descendants.hasNext();
          descendants.next();
          assertEquals(expectedTrx.getName(), rtx.getName());
          assertEquals(expectedTrx.getValue(), rtx.getValue());
        }
      }
    }
  }

  @Test
  public void testAttributesNSPrefix() throws Exception {
    // Setup expected.
    final XdmNodeWriteTrx expectedTrx2 = holder.getXdmNodeWriteTrx();
    DocumentCreator.createWithoutNamespace(expectedTrx2);
    expectedTrx2.commit();

    // Setup parsed session.
    final Database database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());

    try (final XdmResourceManager manager2 = database2.getXdmResourceManager(TestHelper.RESOURCE);
        final XdmNodeWriteTrx wtx = manager2.beginNodeWriteTrx();
        final FileInputStream fis = new FileInputStream(XML2.toFile())) {
      final XMLShredder shredder =
          new XMLShredder.Builder(wtx, XMLShredder.createFileReader(fis), Insert.ASFIRSTCHILD).commitAfterwards()
                                                                                              .build();
      shredder.call();
      wtx.commit();

      // Verify.
      try (final XdmNodeReadTrx rtx = manager2.beginNodeReadTrx()) {
        rtx.moveToDocumentRoot();
        final Iterator<Long> expectedAttributes = new DescendantAxis(expectedTrx2);
        final Iterator<Long> attributes = new DescendantAxis(rtx);

        while (expectedAttributes.hasNext() && attributes.hasNext()) {
          expectedAttributes.next();
          attributes.next();
          if (expectedTrx2.getKind() == Kind.ELEMENT || rtx.getKind() == Kind.ELEMENT) {
            assertEquals(expectedTrx2.getNamespaceCount(), rtx.getNamespaceCount());
            assertEquals(expectedTrx2.getAttributeCount(), rtx.getAttributeCount());
            for (int i = 0; i < expectedTrx2.getAttributeCount(); i++) {
              assertEquals(expectedTrx2.getName(), rtx.getName());
            }
          }
        }
        attributes.hasNext();

        assertEquals(expectedAttributes.hasNext(), attributes.hasNext());

        expectedTrx2.close();
      }
    }
  }

  @Test
  public void testShreddingLargeText() throws Exception {
    final Database database = TestHelper.getDatabase(PATHS.PATH2.getFile());
    try (final XdmResourceManager manager = database.getXdmResourceManager(TestHelper.RESOURCE);
        final FileInputStream fis1 = new FileInputStream(XML3.toFile());
        final FileInputStream fis2 = new FileInputStream(XML3.toFile())) {
      try (final XdmNodeWriteTrx wtx = manager.beginNodeWriteTrx()) {
        final XMLShredder shredder =
            new XMLShredder.Builder(wtx, XMLShredder.createFileReader(fis1), Insert.ASFIRSTCHILD).commitAfterwards()
                                                                                                 .build();
        shredder.call();
      }

      final StringBuilder tnkBuilder = new StringBuilder();

      try (final XdmNodeReadTrx rtx = manager.beginNodeReadTrx()) {
        assertTrue(rtx.moveToFirstChild().hasMoved());
        assertTrue(rtx.moveToFirstChild().hasMoved());


        do {
          tnkBuilder.append(rtx.getValue());
        } while (rtx.moveToRightSibling().hasMoved());
      }

      final String tnkString = tnkBuilder.toString();

      final XMLEventReader validater = XMLShredder.createFileReader(fis2);
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
          default:
            break;
        }
      }

      assertEquals(xmlBuilder.toString(), tnkString);
    }
  }
}
