package io.sirix.service.xml.serialize;

import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

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
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();

      // Generate from this session.
      final XmlSerializer serializer =
          new XmlSerializer.XmlSerializerBuilder(session, out).prettyPrint().withInitialIndent().build();
      serializer.call();

      System.out.println(out.toString(Constants.DEFAULT_ENCODING));

      // assertEquals(XmlDocumentCreator.XML, out.toString(Constants.DEFAULT_ENCODING.toString()));
    }
  }

  @Test
  public void testXMLSerializerWithMaxLevel() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();

      // Generate from this session.
      final XmlSerializer serializer =
          new XmlSerializer.XmlSerializerBuilder(session, out).emitXMLDeclaration().maxLevel(2).build();
      serializer.call();
      assertEquals(XmlDocumentCreator.PRUNED, out.toString(Constants.DEFAULT_ENCODING));
    }
  }

  @Test
  public void testXMLSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = manager.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();

      // Generate from this session.
      final XmlSerializer serializer =
          new XmlSerializer.XmlSerializerBuilder(manager, out).emitXMLDeclaration().build();
      serializer.call();
      assertEquals(XmlDocumentCreator.XML, out.toString(Constants.DEFAULT_ENCODING));
    }
  }

  @Test
  public void testRestSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();
      wtx.close();

      // Generate from this session.
      final XmlSerializer serializer = XmlSerializer.newBuilder(session, out)
                                                    .emitRESTful()
                                                    .emitRESTSequence()
                                                    .emitIDs()
                                                    .emitXMLDeclaration()
                                                    .build();
      serializer.call();
      assertEquals(XmlDocumentCreator.REST, out.toString(Constants.DEFAULT_ENCODING));
      assertEquals(XmlDocumentCreator.REST, out.toString(Constants.DEFAULT_ENCODING));
    }
  }

  @Test
  public void testIDSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();
      wtx.close();

      // Generate from this session.
      final XmlSerializer serializer =
          new XmlSerializer.XmlSerializerBuilder(session, out).emitIDs().emitXMLDeclaration().build();
      serializer.call();
      assertEquals(XmlDocumentCreator.ID, out.toString(Constants.DEFAULT_ENCODING));
    }
  }

  @Test
  public void testSampleCompleteSerializer() throws Exception {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {

      // generate serialize all from this session
      XmlDocumentCreator.createVersioned(wtx);

      XmlSerializer serializerall = new XmlSerializer.XmlSerializerBuilder(session, out, -1).emitXMLDeclaration()
                                                                                            .serializeTimestamp(false)
                                                                                            .build();
      serializerall.call();
      assertEquals(XmlDocumentCreator.VERSIONEDXML, out.toString(Constants.DEFAULT_ENCODING));
      out.reset();

      serializerall = new XmlSerializer.XmlSerializerBuilder(session, out, 1, 2, 3).emitXMLDeclaration()
                                                                                   .serializeTimestamp(false)
                                                                                   .build();
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
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {

      // generate serialize all from this session
      XmlDocumentCreator.createVersioned(wtx);
      wtx.commit();

      XmlSerializer serializerall =
          new XmlSerializer.XmlSerializerBuilder(session, 5L, out, new XmlSerializerProperties()).emitXMLDeclaration()
                                                                                                 .build();
      serializerall.call();
      final String result = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><b>foo<c/></b>";

      assertEquals(result, out.toString());
      out.reset();

      serializerall = new XmlSerializer.XmlSerializerBuilder(session, out, 1, 2, 3).emitXMLDeclaration()
                                                                                   .serializeTimestamp(false)
                                                                                   .build();
      serializerall.call();
      assertEquals(XmlDocumentCreator.VERSIONEDXML, out.toString(Constants.DEFAULT_ENCODING));
    }
  }
}
