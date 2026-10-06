package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.XmlDBSerializer;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.service.xml.serialize.XmlSerializationAssertions;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.w3c.dom.Element;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.sirix.service.xml.serialize.XmlSerializationAssertions.assertElement;
import static io.sirix.service.xml.serialize.XmlSerializationAssertions.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class XmlQuerySerializationEscapingTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void nativeQuerySerializerRoundTripsDecodedValues(final VersioningType versioning) throws Exception {
    for (int i = 0; i < XmlSerializationAssertions.ENCODED_VALUES.size(); i++) {
      final String input = XmlSerializationAssertions.document(XmlSerializationAssertions.ENCODED_VALUES.get(i));
      final String value = XmlSerializationAssertions.DECODED_VALUES.get(i);
      final String collection = "escaped" + i;
      try (final var store = store(versioning)) {
        store.create(collection, "resource1", new DocumentParser(input));
      }
      assertDecoded(versioning, collection, value);
      final Element expectedRoot = parse(input).getDocumentElement();
      for (final boolean detached : new boolean[] {false, true}) {
        final Element expected = detached
            ? (Element) expectedRoot.getFirstChild()
            : expectedRoot;
        for (final boolean rest : new boolean[] {false, true}) {
          final String output = serializeNative(versioning, expression(collection, detached), rest);
          final Element actual =
              (Element) parse(output).getElementsByTagNameNS(expected.getNamespaceURI(), expected.getLocalName())
                                     .item(0);
          assertElement(expected, actual, rest);
          assertDecoded(versioning, collection, value);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void brackitQuerySerializerRoundTripsDecodedValues(final VersioningType versioning) throws Exception {
    for (int i = 0; i < XmlSerializationAssertions.ENCODED_VALUES.size(); i++) {
      final String input = XmlSerializationAssertions.document(XmlSerializationAssertions.ENCODED_VALUES.get(i));
      final String value = XmlSerializationAssertions.DECODED_VALUES.get(i);
      final String collection = "brackit-escaped" + i;
      try (final var store = store(versioning)) {
        store.create(collection, "resource1", new DocumentParser(input));
      }
      assertDecoded(versioning, collection, value);
      final String output = serializeBrackit(versioning, expression(collection, false));
      assertElement(parse(input).getDocumentElement(), parse(output).getDocumentElement(), false);
      assertDecoded(versioning, collection, value);
      final String reparsed = "brackit-reparsed" + i;
      try (final var store = store(versioning)) {
        store.create(reparsed, "resource1", new DocumentParser(output));
      }
      assertDecoded(versioning, reparsed, value);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void reportedStoreQueryKeepsDecodedNamespaceUriOnReparse(final VersioningType versioning) throws Exception {
    final String input = "<root xmlns='https://example.test/ns?a=1&amp;b=2'/>";
    serializeBrackit(versioning, "xml:store('reported',()," + input + ")");
    final String output = serializeBrackit(versioning, "xml:doc('reported','resource1')");
    assertElement(parse(input).getDocumentElement(), parse(output).getDocumentElement(), false);
    try (final var store = store(versioning)) {
      final var root = store.lookup("reported").getDocument("resource1").getFirstChild();
      assertEquals("https://example.test/ns?a=1&b=2", root.getName().getNamespaceURI());
      assertEquals("https://example.test/ns?a=1&b=2", root.getScope().defaultNS());
      final var reparsed = store.create("reported-reparsed", "resource1", new DocumentParser(output))
                                .getDocument("resource1")
                                .getFirstChild();
      assertEquals(root.getName(), reparsed.getName());
      assertEquals(root.getScope().defaultNS(), reparsed.getScope().defaultNS());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void fileImportedSubtreeKeepsInheritedNamespaces(final VersioningType versioning) throws Exception {
    assertFileImportedSubtree(versioning, "inherited",
        "<root xmlns='urn:outer?a=1&amp;b=2' xmlns:p='urn:prefix?a=1&amp;b=2'>"
            + "<p:child value='plain'><leaf p:value='plain'/><reset xmlns=''/></p:child></root>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void fileImportedSubtreeKeepsShadowedNamespaces(final VersioningType versioning) throws Exception {
    assertFileImportedSubtree(versioning, "shadowed",
        "<root xmlns='urn:outer?a=1&amp;b=2' xmlns:p='urn:prefix?a=1&amp;b=2'>"
            + "<p:child xmlns:p='urn:local?a=1&amp;b=2' value='plain'><leaf/>"
            + "<reset xmlns='' xmlns:p='urn:nested'><p:leaf/></reset><p:leaf/></p:child></root>");
  }

  private void assertFileImportedSubtree(final VersioningType versioning, final String collection,
      final String input) throws Exception {
    final Path file = directory.resolve(collection + ".xml");
    Files.writeString(file, input, StandardCharsets.UTF_8);
    serializeBrackit(versioning, "xml:load('" + collection + "','resource1','" + file.toUri() + "')");
    final Element expected = (Element) parse(input).getDocumentElement().getFirstChild();
    final String output = serializeBrackit(versioning, expression(collection, true));
    assertElement(expected, parse(output).getDocumentElement(), false);
    try (final var store = store(versioning)) {
      final var child = store.lookup(collection).getDocument("resource1").getFirstChild().getFirstChild();
      assertEquals(expected.getNamespaceURI(), child.getName().getNamespaceURI());
      assertEquals(expected.lookupNamespaceURI(null), child.getScope().defaultNS());
      assertEquals(expected.lookupNamespaceURI("p"), child.getScope().resolvePrefix("p"));
      store.create(collection + "-reparsed", "resource1", new DocumentParser(output));
    }
    final String reparsed = serializeBrackit(versioning, expression(collection + "-reparsed", false));
    assertElement(expected, parse(reparsed).getDocumentElement(), false);
    assertElement(parse(input).getDocumentElement(),
        parse(serializeBrackit(versioning, expression(collection, false))).getDocumentElement(), false);
  }

  private String serializeBrackit(final VersioningType versioning, final String expression) {
    try (final var store = store(versioning);
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        new Query(chain, expression).serialize(context, writer);
      }
      return output.toString();
    }
  }

  private static String expression(final String collection, final boolean detached) {
    return "xml:doc('" + collection + "','resource1')" + (detached
        ? "/*/child::*:child"
        : "");
  }

  private String serializeNative(final VersioningType versioning, final String expression, final boolean rest) {
    try (final var store = store(versioning);
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      try (final PrintStream writer = new PrintStream(output, false, StandardCharsets.UTF_8);
          final XmlDBSerializer serializer = new XmlDBSerializer(writer, rest, false)) {
        serializer.serialize(new Query(chain, expression).execute(context));
      }
      return output.toString(StandardCharsets.UTF_8);
    }
  }

  private void assertDecoded(final VersioningType versioning, final String collection, final String value) {
    try (final var store = store(versioning)) {
      final var root = store.lookup(collection).getDocument("resource1").getFirstChild();
      final String defaultUri = "https://example.test/default?" + value;
      final String prefixUri = "https://example.test/prefix?" + value;
      assertEquals(new QNm(defaultUri, "", "root"), root.getName());
      assertEquals(defaultUri, root.getScope().defaultNS());
      assertEquals(prefixUri, root.getScope().resolvePrefix("p"));
      assertEquals(value, root.getAttribute(new QNm("value")).getValue().stringValue());
      assertEquals(value, root.getAttribute(new QNm(prefixUri, "p", "value")).getValue().stringValue());
      final var child = root.getFirstChild();
      assertEquals(new QNm(prefixUri, "p", "child"), child.getName());
      assertEquals(defaultUri, child.getScope().defaultNS());
      assertEquals(prefixUri, child.getScope().resolvePrefix("p"));
      assertEquals(value, child.getFirstChild().getValue().stringValue());
    }
  }

  private BasicXmlDBStore store(final VersioningType versioning) {
    return BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }
}
