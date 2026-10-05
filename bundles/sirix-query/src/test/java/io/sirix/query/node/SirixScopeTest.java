package io.sirix.query.node;

import io.brackit.query.jdm.Scope;
import io.brackit.query.jdm.Stream;
import io.brackit.query.node.parser.DocumentParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SirixScopeTest {
  @TempDir
  Path directory;

  @Test
  void resolvesLocalAndInheritedNamespacesDespiteSharedCursorMoves() {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      final var collection =
          store.create("scope", new DocumentParser("<root xmlns='urn:default' xmlns:p='urn:parent' xmlns:q='urn:other'>"
              + "<child xmlns:p='urn:child'><leaf xmlns=''/></child></root>"));
      final XmlDBNode root = collection.getDocument("resource1").getFirstChild();
      final Scope rootScope = root.getScope();
      final XmlDBNode child = root.getFirstChild();
      final Scope childScope = child.getScope();
      final XmlDBNode leaf = child.getFirstChild();
      final Scope leafScope = leaf.getScope();

      assertEquals("urn:parent", rootScope.resolvePrefix("p"));
      assertEquals("urn:child", childScope.resolvePrefix("p"));
      assertEquals("urn:child", leafScope.resolvePrefix("p"));
      assertEquals("urn:other", leafScope.resolvePrefix("q"));
      assertEquals("urn:default", childScope.defaultNS());
      assertEquals("", leafScope.defaultNS());
      assertEquals("", leafScope.resolvePrefix(null));
      assertEquals("http://www.w3.org/XML/1998/namespace", leafScope.resolvePrefix("xml"));
      assertNull(leafScope.resolvePrefix("missing"));

      final Set<String> prefixes = new HashSet<>(3);
      try (final Stream<String> stream = rootScope.localPrefixes()) {
        String prefix;
        while ((prefix = stream.next()) != null) {
          assertTrue(prefixes.add(prefix), "Each local namespace prefix must be emitted once");
          // Scope lookup and node access both move the same cursor between stream reads.
          assertEquals("urn:default", rootScope.defaultNS());
          leaf.getName();
        }
      }
      assertEquals(Set.of("", "p", "q"), prefixes);
      assertEquals("urn:parent", rootScope.resolvePrefix("p"));
    }
  }
}
