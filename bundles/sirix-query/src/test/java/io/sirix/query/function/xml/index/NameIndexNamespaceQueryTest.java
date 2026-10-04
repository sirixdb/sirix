package io.sirix.query.function.xml.index;

import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.axis.DescendantAxis;
import io.sirix.node.NodeKind;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class NameIndexNamespaceQueryTest {
  @TempDir
  Path directory;

  @Test
  void publicQNameScansKeepNamespaceAndAcceptMultipleNames() {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      store.create("names", "resource1",
          new DocumentParser("<root><item xmlns='urn:a'>a</item><item xmlns='urn:b'>b</item>"
              + "<p:item xmlns:p='urn:a'>alias</p:item><item>plain</item></root>"));
    }
    run("let $doc := xn:doc('names','resource1') let $idx := xn:create-name-index($doc,())"
        + " return sdb:commit($doc)");
    assertEquals("urn:a",
        run("declare namespace a='urn:a';" + " string(namespace-uri-from-QName(xs:QName('a:item')))"));
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      final var trx = store.lookup("names").getDocument("resource1").getTrx();
      final List<String> uris = new ArrayList<>();
      final DescendantAxis axis = new DescendantAxis(trx);
      while (axis.hasNext()) {
        axis.nextLong();
        if (trx.getKind() == NodeKind.ELEMENT) {
          uris.add(trx.getName().getNamespaceURI());
        }
      }
      assertEquals(List.of("", "urn:a", "urn:b", "urn:a", ""), uris);
    }
    assertEquals("a alias", scan("fn:QName('urn:a','item')"));
    assertEquals("a alias", scan("xs:QName('a:item')"));
    assertEquals("b", scan("xs:QName('b:item')"));
    assertEquals("plain", scan("xs:QName('item')"));
    assertEquals("a b alias", scan("(xs:QName('a:item'),xs:QName('b:item'))"));
    assertEquals("4", run("let $doc := xn:doc('names','resource1') return count("
        + "xn:scan-name-index($doc,xn:find-name-index($doc,xs:QName('item')),())/text())"));
  }

  private String scan(final String names) {
    return run("declare namespace a='urn:a'; declare namespace b='urn:b';"
        + " let $doc := xn:doc('names','resource1') return for $n in xn:scan-name-index($doc,"
        + "xn:find-name-index($doc,xs:QName('a:item'))," + names + ") order by sdb:nodekey($n) return string($n)");
  }

  private String run(final String expression) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
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
