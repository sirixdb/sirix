package io.sirix.service.xml.serialize;

import io.brackit.query.atomic.QNm;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.api.Movement;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
   * Literal tab/LF/CR in attribute values are normalized to spaces by conforming parsers, and
   * literal CR/CRLF in content to LF — so they must be serialized as character references
   * ({@code &#x9;}/{@code &#xA;}/{@code &#xD;}) to survive a serialize→reparse round-trip
   * unchanged.
   */
  @Test
  public void testAttributeAndContentWhitespaceRoundTrip() throws Exception {
    final String attributeValue = "tab\there\nand\rthere";
    final String textValue = "line1\r\nline2\rline3";

    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final XmlResourceSession session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final XmlNodeTrx wtx = session.beginNodeTrx();
        final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      wtx.insertElementAsFirstChild(new QNm("root"));
      wtx.insertAttribute(new QNm("value"), attributeValue, Movement.TOPARENT);
      wtx.insertTextAsFirstChild(textValue);
      wtx.commit();

      final XmlSerializer serializer = new XmlSerializer.XmlSerializerBuilder(session, out).build();
      serializer.call();

      final String serialized = out.toString(Constants.DEFAULT_ENCODING);
      assertTrue("attribute whitespace must be emitted as character references: " + serialized,
          serialized.contains("tab&#x9;here&#xA;and&#xD;there"));
      assertTrue("CR in content must be emitted as a character reference: " + serialized,
          serialized.contains("line1&#xD;\nline2&#xD;line3"));

      // Reparse with StAX: the exact original characters must survive the round-trip.
      final XMLInputFactory factory = XMLInputFactory.newInstance();
      factory.setProperty(XMLInputFactory.IS_COALESCING, true);
      final XMLStreamReader reader = factory.createXMLStreamReader(new ByteArrayInputStream(out.toByteArray()));
      String parsedAttributeValue = null;
      final StringBuilder parsedText = new StringBuilder();
      while (reader.hasNext()) {
        reader.next();
        if (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
          parsedAttributeValue = reader.getAttributeValue(null, "value");
        } else if (reader.getEventType() == XMLStreamConstants.CHARACTERS) {
          parsedText.append(reader.getText());
        }
      }
      assertEquals(attributeValue, parsedAttributeValue);
      assertEquals(textValue, parsedText.toString());
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
