package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Stream;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.axis.DescendantAxis;
import io.sirix.node.NodeKind;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

final class ConstructedXmlNamespaceTest {
  private static final String ITEMS = "<item xmlns='urn:a' xmlns:p='urn:attr' p:flag='a' flag='plain'>a</item>"
      + "<item xmlns='urn:b'>b</item><p:item xmlns:p='urn:a' p:flag='alias'>alias</p:item><item>plain</item>";
  private static final String ITEM_SEQUENCE =
      ITEMS.replace("</item><", "</item>,<").replace("</p:item><", "</p:item>,<");
  private static final String XML = "<root>" + ITEMS + "</root>";
  private static final List<String> EXPECTED_NAMES =
      List.of("ELEMENT||root|", "ELEMENT|urn:a|item|", "ATTRIBUTE|urn:attr|flag|p", "ATTRIBUTE||flag|",
          "ELEMENT|urn:b|item|", "ELEMENT|urn:a|item|p", "ATTRIBUTE|urn:a|flag|p", "ELEMENT||item|");

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void reportedStoreExpressionKeepsElementNamespaces(final VersioningType versioning) {
    final String xml = "<root><item xmlns='urn:a'>a</item><item xmlns='urn:b'>b</item>"
        + "<p:item xmlns:p='urn:a'>alias</p:item><item>plain</item></root>";
    try (final var store = store(versioning)) {
      store.create("oracle", "resource1", new DocumentParser(xml));
    }
    run(versioning, "xml:store('names',()," + xml + ")");
    assertEquals(List.of("ELEMENT||root|", "ELEMENT|urn:a|item|", "ELEMENT|urn:b|item|", "ELEMENT|urn:a|item|p",
        "ELEMENT||item|"), names(versioning, "oracle", "resource1"));
    assertEquals(names(versioning, "oracle", "resource1"), names(versioning, "names", "resource1"));
    final String serialized = run(versioning, "xml:doc('names','resource1')");
    try (final var store = store(versioning)) {
      store.create("reported-round-trip", "resource1", new DocumentParser(serialized));
    }
    assertEquals(names(versioning, "oracle", "resource1"), names(versioning, "reported-round-trip", "resource1"),
        serialized);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void serializationRoundTripKeepsNamespaces(final VersioningType versioning) {
    final String nested = "<root xmlns='urn:a' xmlns:p='urn:a'><item p:flag='a'/>"
        + "<branch xmlns='urn:b' xmlns:p='urn:b'><p:item p:flag='b'/><item xmlns=''/></branch>"
        + "<p:item/><item xmlns=''/></root>";
    final List<String> nestedNames = List.of("ELEMENT|urn:a|root|", "ELEMENT|urn:a|item|",
        "ATTRIBUTE|urn:a|flag|p",
        "ELEMENT|urn:b|branch|", "ELEMENT|urn:b|item|p", "ATTRIBUTE|urn:b|flag|p", "ELEMENT||item|",
        "ELEMENT|urn:a|item|p", "ELEMENT||item|");
    for (final String input : List.of(XML, nested)) {
      run(versioning, "xml:store('serialized',()," + input + ")");
      final String serialized = run(versioning, "xml:doc('serialized','resource1')");
      try (final var store = store(versioning)) {
        store.create("reparsed", "resource1", new DocumentParser(serialized));
      }
      assertEquals(input.equals(XML) ? EXPECTED_NAMES : nestedNames, names(versioning, "reparsed", "resource1"),
          serialized);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void pathNameTestsMatchExpandedNames(final VersioningType versioning) {
    run(versioning, "xml:store('paths',()," + XML + ")");
    final String document = "xml:doc('paths','resource1')";
    assertAll(() -> assertEquals("a alias", run(versioning,
        "declare namespace a='urn:a'; " + document + "/root/a:item/string()")),
        () -> assertEquals("", run(versioning,
            "declare namespace p='urn:not-a'; " + document + "/root/p:item/string()")),
        () -> assertEquals("plain", run(versioning, document + "/root/item/string()")),
        () -> assertEquals("a alias", run(versioning,
            "declare namespace a='urn:a'; " + document + "//a:item/string()")),
        () -> assertEquals("a alias", run(versioning,
            "declare default element namespace 'urn:a'; " + document + "/*/item/string()")));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void wildcardAttributesKeepExpandedNames(final VersioningType versioning) {
    final String xml = "<root xmlns:p='urn:a' p:flag='a' flag='plain'/>";
    run(versioning, "xml:store('constructed-attributes',()," + xml + ")");
    try (final var store = store(versioning)) {
      store.create("parsed-attributes", "resource1", new DocumentParser(xml));
    }
    for (final String collection : List.of("constructed-attributes", "parsed-attributes")) {
      final String attributes = "xml:doc('" + collection + "','resource1')/root/@*";
      assertEquals("|flag|plain urn:a|flag|a", run(versioning, "for $a in " + attributes
          + " order by namespace-uri($a) return concat(namespace-uri($a),'|',local-name($a),'|',string($a))"));
      assertEquals("a", run(versioning, "for $a in " + attributes
          + " where node-name($a) eq fn:QName('urn:a','flag') return string($a)"));
      assertEquals("a", run(versioning, "declare namespace a='urn:a'; xml:doc('" + collection
          + "','resource1')/root/@a:flag/string()"));
      assertEquals("plain", run(versioning, "xml:doc('" + collection + "','resource1')/root/@flag/string()"));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void namespaceScopesResolveBindingsWithoutMovingTheCursor(final VersioningType versioning) {
    final String xml = "<root xmlns='urn:a' xmlns:p='urn:a' xmlns:q='urn:q'>"
        + "<branch xmlns='' xmlns:p='urn:b'><leaf xml:lang='en'/></branch><p:item/></root>";
    try (final var store = store(versioning)) {
      final var document = store.create("scopes", "resource1", new DocumentParser(xml)).getDocument("resource1");
      final var root = document.getFirstChild();
      final var rootScope = root.getScope();
      final var leaf = root.getFirstChild().getFirstChild();
      final var leafScope = leaf.getScope();
      final var trx = leaf.getTrx();
      final long cursor = trx.getNodeKey();
      assertAll(() -> assertEquals("urn:a", rootScope.defaultNS()),
          () -> assertEquals("urn:a", rootScope.resolvePrefix("p")),
          () -> assertEquals("", leafScope.defaultNS()),
          () -> assertEquals("", leafScope.resolvePrefix(null)),
          () -> assertEquals("urn:b", leafScope.resolvePrefix("p")),
          () -> assertEquals("urn:q", leafScope.resolvePrefix("q")),
          () -> assertEquals("http://www.w3.org/XML/1998/namespace", leafScope.resolvePrefix("xml")),
          () -> assertNull(leafScope.resolvePrefix("missing")));
      assertEquals(cursor, trx.getNodeKey());
      final List<String> prefixes = new ArrayList<>(3);
      try (final Stream<String> stream = rootScope.localPrefixes()) {
        String prefix;
        while ((prefix = stream.next()) != null) {
          prefixes.add(prefix);
          assertEquals(cursor, trx.getNodeKey());
        }
      }
      prefixes.sort(null);
      assertEquals(List.of("", "p", "q"), prefixes);
      assertEquals(cursor, trx.getNodeKey());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void elementStoresMatchDocumentParser(final VersioningType versioning) {
    final List<String> oracle = oracle(versioning);
    run(versioning, "xml:store('element',()," + XML + ")");
    assertEquals(oracle, names(versioning, "element", "resource1"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void documentStoresMatchDocumentParser(final VersioningType versioning) {
    final List<String> oracle = oracle(versioning);
    run(versioning, "xml:store('document',(),document { " + XML + " })");
    assertEquals(oracle, names(versioning, "document", "resource1"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void addedResourcesAndSequencesMatchDocumentParser(final VersioningType versioning) {
    final List<String> oracle = oracle(versioning);
    run(versioning, "xml:store('added','seed',<seed/>)");
    run(versioning, "xml:store('added','element'," + XML + ",false())");
    run(versioning, "xml:store('added','document',document { " + XML + " },false())");
    run(versioning, "xml:store('added-sequence','seed',<seed/>)");
    run(versioning, "xml:store('added-sequence',(),(" + XML + ",document { " + XML + " }),false())");
    assertAll(() -> assertEquals(oracle, names(versioning, "added", "element")),
        () -> assertEquals(oracle, names(versioning, "added", "document")),
        () -> assertEquals(oracle, names(versioning, "added-sequence", "resource2")),
        () -> assertEquals(oracle, names(versioning, "added-sequence", "resource3")));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void insertedElementsMatchDocumentParser(final VersioningType versioning) {
    final List<String> oracle = oracle(versioning);
    final List<String> positions = List.of("into $doc/root", "as first into $doc/root", "as last into $doc/root",
        "before $doc/root/anchor", "after $doc/root/anchor");
    final List<Executable> assertions = new ArrayList<>(positions.size());
    for (int i = 0; i < positions.size(); i++) {
      final String collection = "insert" + i;
      run(versioning, "xml:store('" + collection + "',(),<root><anchor/></root>)");
      run(versioning, "let $doc := xml:doc('" + collection + "','resource1') return insert nodes (" + ITEM_SEQUENCE
          + ") " + positions.get(i));
      run(versioning, "let $doc := xml:doc('" + collection + "','resource1') return delete nodes $doc/root/anchor");
      final String position = positions.get(i);
      // The XML insert sequence-order follow-up owns first/after ordering; verify every expanded name
      // here.
      final boolean compareMultiset = position.startsWith("as first") || position.startsWith("after");
      final List<String> expected = compareMultiset
          ? oracle.stream().sorted().toList()
          : oracle;
      assertions.add(() -> {
        final List<String> actual = names(versioning, collection, "resource1");
        if (compareMultiset) {
          actual.sort(null);
        }
        assertEquals(expected, actual, position);
      });
    }
    assertAll(assertions);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void insertedNamespacedAttributesMatchDocumentParser(final VersioningType versioning) {
    try (final var store = store(versioning)) {
      store.create("oracle", "resource1", new DocumentParser("<root xmlns:p='urn:a' p:flag='a' flag='plain'/>"));
    }
    run(versioning, "xml:store('attributes',(),<root/>)");
    run(versioning, "declare namespace p='urn:a'; let $doc := xml:doc('attributes','resource1')"
        + " return insert nodes (attribute p:flag {'a'},attribute flag {'plain'}) into $doc/root");
    assertEquals(names(versioning, "oracle", "resource1"), names(versioning, "attributes", "resource1"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void nameIndexBulkBuildAndIncrementalMaintenanceKeepConstructedNamespaces(final VersioningType versioning) {
    run(versioning, "xml:store('bulk',()," + XML + ")");
    run(versioning, "xml:store('incremental',(),<root/>)");
    for (final String collection : List.of("bulk", "incremental")) {
      run(versioning, "let $doc := xml:doc('" + collection + "','resource1')"
          + " let $idx := xml:create-name-index($doc,()) return sdb:commit($doc)");
    }
    run(versioning,
        "let $doc := xml:doc('incremental','resource1') return insert nodes (" + ITEM_SEQUENCE + ") into $doc/root");
    final List<Executable> assertions = new ArrayList<>(3);
    for (final String collection : List.of("bulk", "incremental")) {
      assertions.add(
          () -> assertAll(collection, () -> assertEquals(EXPECTED_NAMES, names(versioning, collection, "resource1")),
              () -> assertEquals("a alias", scan(versioning, collection, "fn:QName('urn:a','item')")),
              () -> assertEquals("b", scan(versioning, collection, "fn:QName('urn:b','item')")),
              () -> assertEquals("plain", scan(versioning, collection, "fn:QName('','item')"))));
      // The XML bulk attribute NAME index follow-up owns bulk attribute postings; keep incremental
      // checks.
      if (collection.equals("incremental")) {
        assertions.add(() -> assertAll(collection,
            () -> assertEquals("a", scan(versioning, collection, "fn:QName('urn:attr','flag')")),
            () -> assertEquals("alias", scan(versioning, collection, "fn:QName('urn:a','flag')")),
            () -> assertEquals("plain", scan(versioning, collection, "fn:QName('','flag')"))));
      }
    }
    assertAll(assertions);
  }

  private List<String> oracle(final VersioningType versioning) {
    try (final var store = store(versioning)) {
      store.create("oracle", "resource1", new DocumentParser(XML));
    }
    final List<String> names = names(versioning, "oracle", "resource1");
    assertEquals(EXPECTED_NAMES, names);
    return names;
  }

  private List<String> names(final VersioningType versioning, final String collection, final String resource) {
    try (final var store = store(versioning)) {
      final var trx = store.lookup(collection).getDocument(resource).getTrx();
      final List<String> result = new ArrayList<>();
      final DescendantAxis axis = new DescendantAxis(trx);
      while (axis.hasNext()) {
        axis.nextLong();
        if (trx.getKind() == NodeKind.ELEMENT) {
          addName(result, NodeKind.ELEMENT, trx.getName());
          final long element = trx.getNodeKey();
          final int attributes = trx.getAttributeCount();
          for (int i = 0; i < attributes; i++) {
            trx.moveToAttribute(i);
            addName(result, NodeKind.ATTRIBUTE, trx.getName());
            trx.moveTo(element);
          }
        }
      }
      return result;
    }
  }

  private static void addName(final List<String> names, final NodeKind kind, final QNm name) {
    names.add(kind + "|" + name.getNamespaceURI() + "|" + name.getLocalName() + "|" + name.getPrefix());
  }

  private String scan(final VersioningType versioning, final String collection, final String name) {
    return run(versioning,
        "let $doc := xml:doc('" + collection + "','resource1') return for $node in"
            + " xml:scan-name-index($doc,xml:find-name-index($doc," + name + ")," + name + ")"
            + " order by sdb:nodekey($node) return string($node)");
  }

  private BasicXmlDBStore store(final VersioningType versioning) {
    return BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }

  private String run(final VersioningType versioning, final String expression) {
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
}
