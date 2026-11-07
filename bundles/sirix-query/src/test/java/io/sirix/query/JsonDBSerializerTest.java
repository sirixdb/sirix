package io.sirix.query;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.node.BasicXmlDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class JsonDBSerializerTest {
  @TempDir
  Path directory;

  /** W3C Serialization 3.1 section 9 represents XML nodes as serialized XML in JSON strings. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void xmlNodesAreEscapedStringsWithinMixedJsonResults(final boolean prettyPrint) throws Exception {
    final StringBuilder output = new StringBuilder();
    try (final JsonDBSerializer serializer = new JsonDBSerializer(output, prettyPrint)) {
      serializer.serialize(new Query(
          "({\"n\":0}, <index note='&quot;\\&amp;'>{codepoints-to-string(10)}</index>, 7, {\"n\":1})")
              .evaluate(new BrackitQueryContext()));
    }

    final JsonArray results = JsonParser.parseString(output.toString()).getAsJsonObject().getAsJsonArray("rest");
    assertEquals(4, results.size());
    assertEquals(0, results.get(0).getAsJsonObject().get("n").getAsInt());
    final Element index = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(new InputSource(new StringReader(results.get(1).getAsString()))).getDocumentElement();
    assertEquals("index", index.getTagName());
    assertEquals("\"\\&", index.getAttribute("note"));
    assertEquals("\n", index.getTextContent());
    assertEquals(7, results.get(2).getAsInt());
    assertEquals(1, results.get(3).getAsJsonObject().get("n").getAsInt());
  }

  @Test
  void storedXmlNodesUseXmlSerialization() throws Exception {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      store.create("xml", new DocumentParser("<root>stored &amp; escaped</root>"));
      final StringBuilder output = new StringBuilder();
      try (final JsonDBSerializer serializer = new JsonDBSerializer(output, false)) {
        serializer.serialize(new Query(chain, "xn:doc('xml','resource1')").evaluate(context));
      }
      final JsonArray results = JsonParser.parseString(output.toString()).getAsJsonObject().getAsJsonArray("rest");
      assertEquals(1, results.size());
      final Element root = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .parse(new InputSource(new StringReader(results.get(0).getAsString()))).getDocumentElement();
      assertEquals("root", root.getTagName());
      assertEquals("stored & escaped", root.getTextContent());
    }
  }
}
