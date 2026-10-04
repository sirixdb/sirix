package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
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
      assertions.add(() -> assertEquals(oracle, names(versioning, collection, "resource1"), position));
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
    final List<Executable> assertions = new ArrayList<>(2);
    for (final String collection : List.of("bulk", "incremental")) {
      assertions.add(
          () -> assertAll(collection, () -> assertEquals(EXPECTED_NAMES, names(versioning, collection, "resource1")),
              () -> assertEquals("a alias", scan(versioning, collection, "fn:QName('urn:a','item')")),
              () -> assertEquals("b", scan(versioning, collection, "fn:QName('urn:b','item')")),
              () -> assertEquals("plain", scan(versioning, collection, "fn:QName('','item')")),
              () -> assertEquals("a", scan(versioning, collection, "fn:QName('urn:attr','flag')")),
              () -> assertEquals("alias", scan(versioning, collection, "fn:QName('urn:a','flag')")),
              () -> assertEquals("plain", scan(versioning, collection, "fn:QName('','flag')"))));
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
