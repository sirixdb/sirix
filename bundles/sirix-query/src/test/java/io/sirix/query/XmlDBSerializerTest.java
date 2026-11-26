package io.sirix.query;

import io.brackit.query.ErrorCode;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class XmlDBSerializerTest {
  @TempDir
  Path directory;

  // REST-203: standalone stored attributes/namespaces use the REST representation only.
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void storedNonStructuralNodesFollowTheRestRepresentation(final boolean namespace) throws Exception {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      final var collection =
          store.create("nodes", new DocumentParser("<root xmlns:p='urn:example' name='&quot;&lt;&amp;水'/>"));
      for (final boolean rest : new boolean[] {false, true}) {
        final var document = collection.getDocument();
        final var cursor = document.getTrx();
        assertTrue(cursor.moveToFirstChild());
        assertTrue(namespace
            ? cursor.moveToNamespace(0)
            : cursor.moveToAttribute(0));
        final var item = new XmlDBNode(cursor, collection);
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (final PrintStream writer = new PrintStream(output, false, StandardCharsets.UTF_8);
            final XmlDBSerializer serializer = new XmlDBSerializer(writer, rest, false)) {
          if (!rest) {
            assertEquals(ErrorCode.ERR_SERIALIZE_ATTRIBUTE_OR_NAMESPACE_NODE,
                assertThrows(QueryException.class, () -> serializer.serialize(item)).getCode());
            continue;
          }
          serializer.serialize(item);
        }
        final var factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        final Element envelope =
            factory.newDocumentBuilder().parse(new ByteArrayInputStream(output.toByteArray())).getDocumentElement();
        final String local = namespace
            ? "namespace"
            : "attribute";
        assertEquals(1, envelope.getElementsByTagNameNS("https://sirix.io/rest", local).getLength());
        final Element node = (Element) envelope.getElementsByTagNameNS("https://sirix.io/rest", local).item(0);
        if (namespace) {
          assertEquals("p", node.getAttribute("prefix"));
          assertEquals("urn:example", node.getAttribute("uri"));
        } else {
          assertEquals("name", node.getAttribute("name"));
          assertEquals("", node.getAttribute("namespace"));
          assertEquals("\"<&水", node.getAttribute("value"));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"()", "xn:diff('diff','resource1',1,1)", "1"})
  void restQueryResultsHaveACompleteXmlEnvelope(final String expression) throws Exception {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      store.create("diff", new DocumentParser("<root xml:lang='en'/>"));
      for (final boolean prettyPrint : new boolean[] {false, true}) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (final PrintStream writer = new PrintStream(output, false, StandardCharsets.UTF_8);
            final XmlDBSerializer serializer = new XmlDBSerializer(writer, true, prettyPrint)) {
          serializer.serialize(new Query(chain, expression).evaluate(context));
        }
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        final Element envelope =
            factory.newDocumentBuilder().parse(new ByteArrayInputStream(output.toByteArray())).getDocumentElement();
        assertEquals("https://sirix.io/rest", envelope.getNamespaceURI());
        assertEquals("sequence", envelope.getLocalName());
        assertEquals(0, envelope.getElementsByTagName("*").getLength());
        if (expression.equals("1")) {
          assertEquals("1", envelope.getTextContent().trim());
        } else {
          assertTrue(envelope.getTextContent().isBlank());
        }
      }
    }
  }
}
