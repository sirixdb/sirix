package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  @CsvSource(value = {"@xml:*|1", "@*:lang|1", "@*:missing|0", "@xml:lang|1", "@other|1", "@missing|0", "@*|2",
      "attribute::*|2", "attribute::attribute()|2", "attribute::node()|2", "attribute::text()|0",
      "attribute::element()|0", "attribute::attribute(*,xs:integer)|0", "attribute::attribute(xml:lang,xs:integer)|0",
      "attribute::attribute(*,xs:untypedAtomic)|2", "attribute::attribute(xml:lang,xs:untypedAtomic)|1"},
      delimiter = '|')
  void attributeStepsHonorCompleteNodeTests(final String step, final String expected) {
    final VersioningType versioningType = VersioningType.SLIDING_SNAPSHOT;
    final String document = "<leaf xml:lang='en' other='x'/>";
    assertEquals(expected, run(versioningType, "count((" + document + ")/" + step + ")"), "constructed " + step);
    run(versioningType, "xn:store('reserved','resource1'," + document + ")");
    assertEquals(expected, run(versioningType, "count(xn:doc('reserved','resource1')/leaf/" + step + ")"),
        "stored " + step);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void storedAttributeCountDiffersFromNamespaceCount(final VersioningType versioningType) {
    run(versioningType,
        "xn:store('reserved','resource1',<leaf xmlns:p='urn:attributes' p:first='a' second='b'/>)");
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioningType).build();
        final var session = store.lookup("reserved").getDatabase().beginResourceSession("resource1");
        final var rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild());
      assertEquals(2, rtx.getAttributeCount());
      assertEquals(1, rtx.getNamespaceCount());
    }
    final String leaf = "xn:doc('reserved','resource1')/leaf";
    assertEquals("1", run(versioningType, "xn:namespace-count(" + leaf + ")"));
    assertEquals("2", run(versioningType, "xn:attribute-count(" + leaf + ")"));
  }

  @Test
  void storedExactAttributeNamesMatchExpandedNamesAndDictionaryKeys() {
    final VersioningType versioningType = VersioningType.SLIDING_SNAPSHOT;
    run(versioningType, "xn:store('reserved','resource1',"
        + "<leaf xmlns:p='urn:a' xmlns:q='urn:b' p:lang='a' q:lang='b' lang='plain' Aa='first' BB='second'/>)");
    final String leaf = "xn:doc('reserved','resource1')/leaf";
    assertEquals("a", run(versioningType, "declare namespace alias='urn:a'; string(" + leaf + "/@alias:lang)"));
    assertEquals("b", run(versioningType, "declare namespace p='urn:b'; string(" + leaf + "/@p:lang)"));
    assertEquals("plain", run(versioningType, "string(" + leaf + "/@lang)"));
    assertEquals("first", run(versioningType, "string(" + leaf + "/@Aa)"));
    assertEquals("second", run(versioningType, "string(" + leaf + "/@BB)"));
    assertEquals("0", run(versioningType, "declare namespace p='urn:missing'; count(" + leaf + "/@p:lang)"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void storedAttributeNamesSurviveCommittedRename(final VersioningType versioningType) {
    run(versioningType, "xn:store('reserved','resource1',<leaf old='x'/>)");
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioningType).build();
        final var session = store.lookup("reserved").getDatabase().beginResourceSession("resource1");
        final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToAttributeByName(new QNm("old")));
      wtx.setName(new QNm("new"));
      wtx.commit();
    }
    final String leaf = "xn:doc('reserved','resource1')/leaf";
    assertEquals("1", run(versioningType, "count(" + leaf + "/@new)"));
    assertEquals("x", run(versioningType, "string(" + leaf + "/attribute::attribute(new))"));
    assertEquals("0", run(versioningType, "count(" + leaf + "/@old)"));
    assertEquals("1", run(versioningType, "count(" + leaf + "/attribute::attribute(new,xs:untypedAtomic))"));
    assertEquals("1", run(versioningType, "count(" + leaf + "/attribute::node())"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void storedAttributeNamesSurviveCommittedCollisionHeadDeletion(final VersioningType versioningType) {
    run(versioningType, "xn:store('reserved','resource1',<leaf Aa='first' BB='second'/>)");
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioningType).build();
        final var session = store.lookup("reserved").getDatabase().beginResourceSession("resource1");
        final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToAttributeByName(new QNm("Aa")));
      wtx.remove();
      wtx.commit();
    }
    final String leaf = "xn:doc('reserved','resource1')/leaf";
    assertEquals("second", run(versioningType, "string(" + leaf + "/@BB)"));
    assertEquals("second", run(versioningType, "string(" + leaf + "/attribute::attribute(BB))"));
    assertEquals("0", run(versioningType, "count(" + leaf + "/@Aa)"));
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioningType).build();
        final var session = store.lookup("reserved").getDatabase().beginResourceSession("resource1");
        final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveToFirstChild());
      wtx.insertElementAsFirstChild(new QNm("child"));
      wtx.insertAttribute(new QNm("BB"), "third");
      wtx.commit();
    }
    assertEquals("second", run(versioningType, "string(" + leaf + "/@BB)"));
    assertEquals("third", run(versioningType, "string(" + leaf + "/child/@BB)"));
    assertEquals("0", run(versioningType, "count(" + leaf + "/child/@Aa)"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void storedAttributeNamespacesSurviveCommittedCollisionHeadDeletion(final VersioningType versioningType) {
    run(versioningType, "xn:store('reserved','resource1',<leaf>"
        + "<drop xmlns:a='urn:Aa' a:gone='first'/><keep xmlns:b='urn:BB' b:lang='second'/></leaf>)");
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioningType).build();
        final var session = store.lookup("reserved").getDatabase().beginResourceSession("resource1");
        final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      wtx.remove();
      wtx.commit();
    }
    final String prolog = "declare namespace alias='urn:BB'; ";
    final String leaf = "xn:doc('reserved','resource1')/leaf";
    assertEquals("second", run(versioningType, prolog + "string(" + leaf + "/keep/@alias:lang)"));
    assertEquals("second", run(versioningType, prolog + "string(" + leaf + "/keep/attribute::attribute(alias:lang))"));
    assertEquals("0", run(versioningType, "declare namespace alias='urn:Aa'; count(" + leaf + "/keep/@alias:lang)"));
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioningType).build();
        final var session = store.lookup("reserved").getDatabase().beginResourceSession("resource1");
        final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveToFirstChild());
      wtx.insertElementAsFirstChild(new QNm("child"));
      wtx.insertNamespace(new QNm("urn:BB", "b", ""));
      assertTrue(wtx.moveToParent());
      wtx.insertAttribute(new QNm("urn:BB", "b", "lang"), "third");
      wtx.commit();
    }
    assertEquals("second", run(versioningType, prolog + "string(" + leaf + "/keep/@alias:lang)"));
    assertEquals("third", run(versioningType, prolog + "string(" + leaf + "/child/@alias:lang)"));
    assertEquals("0", run(versioningType, "declare namespace alias='urn:Aa'; count(" + leaf + "/child/@alias:lang)"));
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
