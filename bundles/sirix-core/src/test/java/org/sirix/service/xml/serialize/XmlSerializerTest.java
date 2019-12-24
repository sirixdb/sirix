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

package org.sirix.service.xml.serialize;

import static org.junit.Assert.assertEquals;
import java.io.ByteArrayOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.XmlTestHelper;
import org.sirix.XmlTestHelper.PATHS;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XmlSerializer.XmlSerializerBuilder;
import org.sirix.settings.Constants;
import org.sirix.utils.XmlDocumentCreator;

public class XmlSerializerTest {

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testXMLSerializerWithInitialIndent() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceManager manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();

      // Generate from this session.
      final XmlSerializer serializer = new XmlSerializerBuilder(manager, out).prettyPrint().withInitialIndent().build();
      serializer.call();

      System.out.println(out.toString(Constants.DEFAULT_ENCODING.toString()));

      // assertEquals(XmlDocumentCreator.XML, out.toString(Constants.DEFAULT_ENCODING.toString()));
    }
  }

  @Test
  public void testXMLSerializerWithMaxLevel() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceManager manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();

      // Generate from this session.
      final XmlSerializer serializer = new XmlSerializerBuilder(manager, out).emitXMLDeclaration().maxLevel(2).build();
      serializer.call();
      assertEquals(XmlDocumentCreator.PRUNED, out.toString(Constants.DEFAULT_ENCODING.toString()));
    }
  }

  @Test
  public void testXMLSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceManager manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();

      // Generate from this session.
      final XmlSerializer serializer = new XmlSerializerBuilder(manager, out).emitXMLDeclaration().build();
      serializer.call();
      assertEquals(XmlDocumentCreator.XML, out.toString(Constants.DEFAULT_ENCODING.toString()));
    }
  }

  @Test
  public void testRestSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceManager manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();
      wtx.close();

      // Generate from this session.
      final XmlSerializer serializer = XmlSerializer.newBuilder(manager, out)
                                                    .emitRESTful()
                                                    .emitRESTSequence()
                                                    .emitIDs()
                                                    .emitXMLDeclaration()
                                                    .build();
      serializer.call();
      assertEquals(XmlDocumentCreator.REST, out.toString(Constants.DEFAULT_ENCODING.toString()));
      assertEquals(XmlDocumentCreator.REST, out.toString(Constants.DEFAULT_ENCODING.toString()));
    }
  }

  @Test
  public void testIDSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceManager manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();
      wtx.close();

      // Generate from this session.
      final XmlSerializer serializer = new XmlSerializerBuilder(manager, out).emitIDs().emitXMLDeclaration().build();
      serializer.call();
      assertEquals(XmlDocumentCreator.ID, out.toString(Constants.DEFAULT_ENCODING.toString()));
    }
  }

  @Test
  public void testSampleCompleteSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceManager manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {

      // generate serialize all from this session
      XmlDocumentCreator.createVersioned(wtx);

      XmlSerializer serializerall =
          new XmlSerializerBuilder(manager, out, -1).emitXMLDeclaration().serializeTimestamp(false).build();
      serializerall.call();
      assertEquals(XmlDocumentCreator.VERSIONEDXML, out.toString(Constants.DEFAULT_ENCODING.toString()));
      out.reset();

      serializerall =
          new XmlSerializerBuilder(manager, out, 1, 2, 3).emitXMLDeclaration().serializeTimestamp(false).build();
      serializerall.call();
      assertEquals(XmlDocumentCreator.VERSIONEDXML, out.toString());
    }
  }

  /**
   * This test check the XPath //books expression and expects 6 books as result. But the failure is,
   * that only the children of the books will be serialized and NOT the book node itself.
   */
  @Test
  public void testKeyStart() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceManager manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {

      // generate serialize all from this session
      XmlDocumentCreator.createVersioned(wtx);
      wtx.commit();

      XmlSerializer serializerall =
          new XmlSerializerBuilder(manager, 5l, out, new XmlSerializerProperties()).emitXMLDeclaration().build();
      serializerall.call();
      final String result = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><b>foo<c/></b>";

      assertEquals(result, out.toString());
      out.reset();

      serializerall =
          new XmlSerializerBuilder(manager, out, 1, 2, 3).emitXMLDeclaration().serializeTimestamp(false).build();
      serializerall.call();
      assertEquals(XmlDocumentCreator.VERSIONEDXML, out.toString(Constants.DEFAULT_ENCODING.toString()));
    }
  }
}
