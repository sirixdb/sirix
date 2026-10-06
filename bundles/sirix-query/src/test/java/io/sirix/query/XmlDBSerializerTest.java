package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.node.BasicXmlDBStore;
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

final class XmlDBSerializerTest {
  @TempDir
  Path directory;

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
        final Element envelope = factory.newDocumentBuilder()
                                        .parse(new ByteArrayInputStream(output.toByteArray()))
                                        .getDocumentElement();
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
