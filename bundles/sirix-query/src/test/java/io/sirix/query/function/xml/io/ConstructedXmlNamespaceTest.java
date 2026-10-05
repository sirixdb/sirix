package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Stream;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.filter.xml.XmlNameFilter;
import io.sirix.node.NodeKind;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.settings.VersioningType;
import io.sirix.service.xml.serialize.XmlSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        + "<branch xmlns='urn:b' xmlns:p='urn:b'><p:item p:flag='b'/><item xmlns=''/>"
        + "<inner xmlns:p='urn:a'><p:item/><deep xmlns:p='urn:b'><p:item/>"
        + "<repeat xmlns:p='urn:b'><p:item/></repeat></deep><p:item/></inner><p:item/></branch>"
        + "<p:item/><item xmlns=''/></root>";
    final List<String> nestedNames = List.of("ELEMENT|urn:a|root|", "ELEMENT|urn:a|item|", "ATTRIBUTE|urn:a|flag|p",
        "ELEMENT|urn:b|branch|", "ELEMENT|urn:b|item|p", "ATTRIBUTE|urn:b|flag|p", "ELEMENT||item|",
        "ELEMENT|urn:b|inner|", "ELEMENT|urn:a|item|p", "ELEMENT|urn:b|deep|", "ELEMENT|urn:b|item|p",
        "ELEMENT|urn:b|repeat|", "ELEMENT|urn:b|item|p", "ELEMENT|urn:a|item|p", "ELEMENT|urn:b|item|p",
        "ELEMENT|urn:a|item|p", "ELEMENT||item|");
    for (final String input : List.of(XML, nested)) {
      run(versioning, "xml:store('serialized',()," + input + ")");
      final List<String> expectedNames = input.equals(XML)
          ? EXPECTED_NAMES
          : nestedNames;
      assertEquals(expectedNames, names(versioning, "serialized", "resource1"));
      final String serialized = run(versioning, "xml:doc('serialized','resource1')");
      try (final var store = store(versioning)) {
        store.create("reparsed", "resource1", new DocumentParser(serialized));
      }
      assertEquals(expectedNames, names(versioning, "reparsed", "resource1"), serialized);
      assertEquals(expectedNames, names(versioning, "serialized", "resource1"));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void detachedSubtreeKeepsInheritedNamespaceBindings(final VersioningType versioning) {
    final String xml = "<root xmlns='urn:outer' xmlns:p='urn:outer-prefix' xmlns:q='urn:attribute'>"
        + "<branch xmlns='urn:inner' xmlns:p='urn:inner-prefix'><item q:flag='a'><p:child/>"
        + "<reset xmlns='' xmlns:p='urn:local'><p:child/></reset><p:child/></item></branch></root>";
    final String serialized;
    try (final var store = store(versioning)) {
      final var document = store.create("detached", "resource1", new DocumentParser(xml)).getDocument("resource1");
      final var item = document.getFirstChild().getFirstChild().getFirstChild();
      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      XmlSerializer.newBuilder(item.getTrx().getResourceSession(), output)
                   .startNodeKey(item.getNodeKey())
                   .build()
                   .call();
      serialized = output.toString(StandardCharsets.UTF_8);
      store.create("detached-round-trip", "resource1", new DocumentParser(serialized));
    }
    assertEquals(
        List.of("ELEMENT|urn:inner|item|", "ATTRIBUTE|urn:attribute|flag|q", "ELEMENT|urn:inner-prefix|child|p",
            "ELEMENT||reset|", "ELEMENT|urn:local|child|p", "ELEMENT|urn:inner-prefix|child|p"),
        names(versioning, "detached-round-trip", "resource1"), serialized);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void diffReplayKeepsInheritedBindingsFromTheSelectedRevision(final VersioningType versioning) {
    for (final boolean replace : new boolean[] {false, true}) {
      final String collection = replace
          ? "diff-replace"
          : "diff-insert";
      try (final var store = store(versioning)) {
        final var document = store.create(collection, "resource1",
            new DocumentParser("<root xmlns='urn:outer' xmlns:p='urn:outer-prefix' xmlns:q='urn:attribute'>"
                + "<branch xmlns='urn:inner' xmlns:p='urn:inner-prefix'><old/></branch></root>"))
                                  .getDocument("resource1");
        final long oldKey = document.getFirstChild().getFirstChild().getFirstChild().getNodeKey();
        try (final var writer = document.getTrx().getResourceSession().beginNodeTrx()) {
          writer.moveTo(oldKey);
          writer.insertElementAsLeftSibling(new QNm("urn:inner", "", "added"));
          writer.insertAttribute(new QNm("urn:attribute", "q", "flag"), "a").moveToParent();
          writer.insertElementAsFirstChild(new QNm("urn:inner-prefix", "p", "child"));
          final long childKey = writer.getNodeKey();
          if (replace) {
            writer.moveTo(oldKey);
            writer.remove();
          }
          writer.commit();
          writer.moveTo(childKey);
          writer.setName(new QNm("urn:inner-prefix", "p", "newest"));
          writer.commit();
        }
      }
      final String diff = run(versioning, "xml:diff('" + collection + "','resource1',1,2)");
      run(versioning, diff);
      final List<String> expected = new ArrayList<>(List.of("ELEMENT|urn:outer|root|", "ELEMENT|urn:inner|branch|",
          "ELEMENT|urn:inner|added|", "ATTRIBUTE|urn:attribute|flag|q", "ELEMENT|urn:inner-prefix|child|p"));
      if (!replace) {
        expected.add("ELEMENT|urn:inner|old|");
      }
      assertEquals(expected, names(versioning, collection, "resource1"), diff);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void collidingNamespacePrefixKeepsItsBindingOnRoundTrip(final VersioningType versioning) {
    run(versioning, "xml:store('collision-scope',(),<Aa:item xmlns:Aa='BB'/>)");
    try (final var store = store(versioning)) {
      final var element = store.lookup("collision-scope").getDocument("resource1").getFirstChild();
      final var scope = element.getScope();
      final var trx = element.getTrx();
      final long cursor = trx.getNodeKey();
      assertEquals("BB", scope.resolvePrefix("Aa"));
      assertNull(scope.resolvePrefix("BB"));
      assertEquals(cursor, trx.getNodeKey());
    }
    final String serialized = run(versioning, "xml:doc('collision-scope','resource1')");
    try (final var store = store(versioning)) {
      store.create("collision-round-trip", "resource1", new DocumentParser(serialized));
    }
    final List<String> expected = List.of("ELEMENT|BB|item|Aa");
    assertEquals(expected, names(versioning, "collision-scope", "resource1"));
    assertEquals(expected, names(versioning, "collision-round-trip", "resource1"), serialized);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void collidingLocalNamesAndLexicalPrefixesMatchExactly(final VersioningType versioning) {
    run(versioning, "xml:store('reported-name-collision',(),<root><Aa/><BB/></root>)");
    assertEquals("BB", run(versioning, "xml:doc('reported-name-collision','resource1')/root/BB/local-name()"));
    run(versioning,
        "xml:store('collision-names',(),<root xmlns:Aa='urn:a' xmlns:BB='urn:b'"
            + " Aa:flag='a' BB:flag='b' Aa='local-a' BB='local-b'>"
            + "<Aa:item/><BB:item/><Aa>local-a</Aa><BB>local-b</BB></root>)");
    final String document = "xml:doc('collision-names','resource1')";
    assertEquals("local-b", run(versioning, document + "/root/BB/string()"));
    assertEquals("local-b", run(versioning, document + "//BB/string()"));
    assertEquals("local-b", run(versioning, document + "/root/@BB/string()"));
    try (final var store = store(versioning)) {
      final var root = store.lookup("collision-names").getDocument("resource1").getFirstChild();
      final var trx = root.getTrx();
      final DescendantAxis axis = new DescendantAxis(trx);
      while (axis.hasNext()) {
        axis.nextLong();
        final var name = trx.getName();
        if (name == null) {
          continue;
        }
        assertTrue(new XmlNameFilter(trx, name).filter());
        if (name.getLocalName().equals("item")) {
          assertTrue(new XmlNameFilter(trx, name.getPrefix() + ":item").filter());
          assertFalse(new XmlNameFilter(trx, (name.getPrefix().equals("Aa")
              ? "BB"
              : "Aa") + ":item").filter());
        } else {
          assertTrue(new XmlNameFilter(trx, name.getLocalName()).filter());
          assertFalse(new XmlNameFilter(trx, name.getLocalName().equals("Aa")
              ? "BB"
              : "Aa").filter());
        }
      }
      trx.moveTo(root.getNodeKey());
      final int attributes = trx.getAttributeCount();
      for (int i = 0; i < attributes; i++) {
        trx.moveToAttribute(i);
        final var name = trx.getName();
        assertTrue(new XmlNameFilter(trx, name).filter());
        if (!name.getPrefix().isEmpty()) {
          assertTrue(new XmlNameFilter(trx, name.getPrefix() + ":flag").filter());
          assertFalse(new XmlNameFilter(trx, (name.getPrefix().equals("Aa")
              ? "BB"
              : "Aa") + ":flag").filter());
        } else {
          assertTrue(new XmlNameFilter(trx, name.getLocalName()).filter());
          assertFalse(new XmlNameFilter(trx, name.getLocalName().equals("Aa")
              ? "BB"
              : "Aa").filter());
        }
        trx.moveTo(root.getNodeKey());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void partialAttributeWildcardsSelectAndDeleteOnlyMatchingAttributes(final VersioningType versioning) {
    run(versioning, "xml:store('partial-attributes',(),<root xmlns:p='urn:a' p:flag='a' flag='plain' other='x'/>)");
    final String root = "xml:doc('partial-attributes','resource1')/root";
    final String declarations = "declare namespace a='urn:a'; ";
    assertEquals("1 2 3 0 a plain",
        run(versioning, declarations + "(count(" + root + "/@a:*),count(" + root + "/@*:flag),count(" + root
            + "/@*),count(" + root + "/@*:missing)," + root + "/@a:*/string()," + root + "/@flag/string())"));
    run(versioning, declarations + "delete nodes " + root + "/@a:*");
    assertEquals("2 1 0 plain x", run(versioning, declarations + "(count(" + root + "/@*),count(" + root
        + "/@*:flag),count(" + root + "/@a:*)," + root + "/@flag/string()," + root + "/@other/string())"));
    assertEquals(List.of("ELEMENT||root|", "ATTRIBUTE||flag|", "ATTRIBUTE||other|"),
        names(versioning, "partial-attributes", "resource1"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void spatialNodeTestsKeepWildcardTypeAndDocumentConstraints(final VersioningType versioning) {
    run(versioning, "xml:store('node-tests',(),<root xmlns:p='urn:a'>"
        + "<p:item p:flag='a' flag='plain' other='x'/><item/><p:other/></root>)");
    final String declarations = "declare namespace a='urn:a'; ";
    final String document = "xml:doc('node-tests','resource1')";
    final String root = document + "/root";
    final String item = root + "/a:item";
    for (final String axis : List.of("child", "descendant", "descendant-or-self")) {
      final String path = root + "/" + axis + "::";
      final String expected = axis.equals("descendant-or-self")
          ? "2 2 0 1 4"
          : "2 2 0 1 3";
      assertEquals(expected,
          run(versioning,
              declarations + "(count(" + path + "a:*),count(" + path + "*:item),count(" + path
                  + "element(a:item,xs:string)),count(" + path + "element(a:item,xs:untyped)),count(" + path
                  + "element()))"),
          axis);
    }
    for (final String axis : List.of("parent", "ancestor", "ancestor-or-self")) {
      final String path = item + "/" + axis + "::";
      assertEquals("0 1", run(versioning,
          declarations + "(count(" + path + "element(root,xs:string)),count(" + path + "element(root,xs:untyped)))"),
          axis);
    }
    final String documentPath = document + "/ancestor-or-self::";
    assertEquals("0 1 0",
        run(versioning, declarations + "(count(" + documentPath + "document-node(element(other))),count(" + documentPath
            + "document-node(element(root))),count(" + documentPath + "document-node(element(root,xs:string))))"));
    for (final String axis : List.of("following", "following-sibling", "preceding", "preceding-sibling")) {
      final String start = axis.startsWith("preceding")
          ? root + "/a:other"
          : item;
      final String path = start + "/" + axis + "::";
      final String expected = axis.startsWith("preceding")
          ? "1 2 0 1"
          : "1 1 0 1";
      assertEquals(expected, run(versioning, declarations + "(count(" + path + "a:*),count(" + path + "*:item),count("
          + path + "element(*,xs:string)),count(" + path + "element(item,xs:untyped)))"), axis);
    }
    assertEquals("0 1 0 3",
        run(versioning,
            declarations + "(count(" + item + "/attribute::attribute(a:flag,xs:string)),count(" + item
                + "/attribute::attribute(a:flag,xs:untypedAtomic)),count(" + item
                + "/attribute::attribute(*,xs:string)),count(" + item + "/attribute::attribute(*,xs:untypedAtomic)))"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void temporalNodeTestsKeepWildcardTypeAndDocumentConstraints(final VersioningType versioning) {
    run(versioning, "xml:store('temporal-tests',(),<root xmlns:p='urn:a' p:flag='a'>" + "<p:item>one</p:item></root>)");
    run(versioning, "declare namespace a='urn:a'; replace value of node"
        + " xml:doc('temporal-tests','resource1')/root/a:item/text() with 'two'");
    final String declarations = "declare namespace a='urn:a'; declare namespace b='urn:b'; ";
    for (final String axis : List.of("first", "last", "next", "previous", "past", "past-or-self", "future",
        "future-or-self", "all-times")) {
      final boolean backwards = axis.equals("first") || axis.equals("previous") || axis.startsWith("past");
      final String document = "xml:doc('temporal-tests','resource1'," + (backwards
          ? "2"
          : "1") + ")";
      final String elementPath = document + "/root/a:item/" + axis + "::";
      final String attributePath = document + "/root/@a:flag/" + axis + "::";
      final String documentPath = document + "/" + axis + "::";
      final int count = axis.endsWith("or-self") || axis.equals("all-times")
          ? 2
          : 1;
      final String expected =
          count + " 0 " + count + " 0 0 " + count + " 0 " + count + " 0 " + count + " 0 " + count + " 0";
      assertEquals(expected,
          run(versioning, declarations + "(count(" + elementPath + "a:*),count(" + elementPath + "b:*),count("
              + elementPath + "*:item),count(" + elementPath + "*:other),count(" + elementPath
              + "element(a:item,xs:string)),count(" + elementPath + "element(a:item,xs:untyped)),count(" + attributePath
              + "attribute(a:flag,xs:string)),count(" + attributePath + "attribute(a:flag,xs:untypedAtomic)),count("
              + attributePath + "attribute(*,xs:string)),count(" + attributePath + "attribute()),count(" + documentPath
              + "document-node(element(other))),count(" + documentPath + "document-node(element(root))),count("
              + documentPath + "document-node(element(root,xs:string))))"),
          axis);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void pathNameTestsMatchExpandedNames(final VersioningType versioning) {
    run(versioning, "xml:store('paths',()," + XML + ")");
    final String document = "xml:doc('paths','resource1')";
    assertAll(
        () -> assertEquals("a alias",
            run(versioning, "declare namespace a='urn:a'; " + document + "/root/a:item/string()")),
        () -> assertEquals("",
            run(versioning, "declare namespace p='urn:not-a'; " + document + "/root/p:item/string()")),
        () -> assertEquals("plain", run(versioning, document + "/root/item/string()")),
        () -> assertEquals("a alias",
            run(versioning, "declare namespace a='urn:a'; " + document + "//a:item/string()")),
        () -> assertEquals("a alias",
            run(versioning, "declare default element namespace 'urn:a'; " + document + "/*/item/string()")));
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
      assertEquals("a", run(versioning,
          "for $a in " + attributes + " where node-name($a) eq fn:QName('urn:a','flag') return string($a)"));
      assertEquals("a", run(versioning,
          "declare namespace a='urn:a'; xml:doc('" + collection + "','resource1')/root/@a:flag/string()"));
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
          () -> assertEquals("urn:a", rootScope.resolvePrefix("p")), () -> assertEquals("", leafScope.defaultNS()),
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
    // The collection.add versioning follow-up extends added-resource coverage beyond SLIDING_SNAPSHOT.
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
