package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ReservedXmlPrefixTest {
  private static final String XML_NAMESPACE = "http://www.w3.org/XML/1998/namespace";
  private static final String DOCUMENT = "<leaf xml:lang='en' xml:space='preserve' xml:id='leaf-id'/>";

  @TempDir
  Path directory;

  @Test
  void xmlPrefixResolvesToW3cAndDoesNotAliasSirixFunctions() {
    assertEquals(XML_NAMESPACE,
        run(VersioningType.SLIDING_SNAPSHOT, "string(namespace-uri-from-QName(xs:QName('xml:lang')))"));
    final QueryException exception = assertThrows(QueryException.class,
        () -> run(VersioningType.SLIDING_SNAPSHOT, "xml:doc('reserved','resource1')"));
    assertEquals("XPST0017", exception.getCode().getLocalName());
  }

  @ParameterizedTest
  @ValueSource(strings = {"lang", "space", "id"})
  void constructedAttributesCanBeQueriedBeforeStorage(final String name) {
    final String expected = switch (name) {
      case "lang" -> "en";
      case "space" -> "preserve";
      case "id" -> "leaf-id";
      default -> throw new AssertionError(name);
    };
    assertEquals(expected, run(VersioningType.SLIDING_SNAPSHOT, "string((" + DOCUMENT + ")/@xml:" + name + ")"));
    assertEquals("3", run(VersioningType.SLIDING_SNAPSHOT, "count((" + DOCUMENT + ")/@*)"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void constructedReservedAttributesSurviveStoreSerializeAndColdRoundTrip(final VersioningType versioningType)
      throws Exception {
    run(versioningType, "xn:store('reserved','resource1'," + DOCUMENT + ")");
    final String serialized = run(versioningType, "xn:doc('reserved','resource1')/leaf");
    assertReservedAttributes(serialized);
    assertStoredAttributes(versioningType, "resource1");

    run(versioningType, "xn:store('roundtrip','resource1'," + serialized + ")");
    assertReservedAttributes(run(versioningType, "xn:doc('roundtrip','resource1')/leaf"));
    assertEquals("en", run(versioningType, "string(xn:doc('roundtrip','resource1')/leaf/@xml:lang)"));
  }

  private void assertStoredAttributes(final VersioningType versioningType, final String resource) {
    final String leaf = "xn:doc('reserved','" + resource + "')/leaf";
    assertEquals("en", run(versioningType, "string(" + leaf + "/@xml:lang)"));
    assertEquals("preserve", run(versioningType, "string(" + leaf + "/@xml:space)"));
    assertEquals("leaf-id", run(versioningType, "string(" + leaf + "/@xml:id)"));
    assertEquals("3", run(versioningType, "count(" + leaf + "/@*)"));
    assertEquals(XML_NAMESPACE, run(versioningType, "string(namespace-uri(" + leaf + "/@xml:lang))"));
    assertEquals(XML_NAMESPACE, run(versioningType, "string(namespace-uri(" + leaf + "/@xml:space))"));
    assertEquals(XML_NAMESPACE, run(versioningType, "string(namespace-uri(" + leaf + "/@xml:id))"));
  }

  private static void assertReservedAttributes(final String serialized) throws Exception {
    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    final Element element =
        factory.newDocumentBuilder().parse(new InputSource(new StringReader(serialized))).getDocumentElement();
    assertEquals("en", element.getAttributeNS(XML_NAMESPACE, "lang"));
    assertEquals("preserve", element.getAttributeNS(XML_NAMESPACE, "space"));
    assertEquals("leaf-id", element.getAttributeNS(XML_NAMESPACE, "id"));
  }

  private String run(final VersioningType versioningType, final String expression) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioningType).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        new Query(chain, "xquery version \"1.0\"; " + expression).serialize(context, writer);
      }
      return output.toString();
    }
  }
}
