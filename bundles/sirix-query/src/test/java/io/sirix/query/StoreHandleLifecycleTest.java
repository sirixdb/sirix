package io.sirix.query;

import com.google.gson.stream.JsonReader;
import io.brackit.query.atomic.Str;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.node.stream.ArrayStream;
import io.sirix.access.Databases;
import io.sirix.access.DatabasesInternals;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StoreHandleLifecycleTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void jsonLookupAndDropHandleClosedCacheEntries(final boolean closeBeforeDrop) {
    final Path databasePath = directory.resolve("collection");
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final JsonDBCollection original = store.create("collection", "resource", "[\"before\"]");
      assertSame(original, store.lookup("collection"));
      original.close();
      final JsonDBCollection reopened = requireNonNull(store.lookup("collection"));
      assertNotSame(original, reopened);
      assertFalse(original.getDatabase().isOpen());
      assertTrue(reopened.getDatabase().isOpen());
      assertSame(reopened, store.lookup("collection"));
      try (final JsonResourceSession session = reopened.getDatabase().beginResourceSession("resource");
          final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertTrue(reader.moveToFirstChild());
        assertEquals("before", reader.getValue());
      }
      if (closeBeforeDrop) {
        reopened.close();
      }
      store.drop("collection");
      assertFalse(reopened.getDatabase().isOpen());
      assertFalse(Files.exists(databasePath));
      assertNull(store.lookup("collection"));
      final JsonDBCollection replacement = store.create("collection", "resource", "[\"after\"]");
      assertSame(replacement, store.lookup("collection"));
      try (final JsonResourceSession session = replacement.getDatabase().beginResourceSession("resource");
          final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertTrue(reader.moveToFirstChild());
        assertEquals("after", reader.getValue());
      }
    } finally {
      Databases.removeDatabase(databasePath);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void xmlLookupAndDropHandleClosedCacheEntries(final boolean closeBeforeDrop) {
    final Path databasePath = directory.resolve("collection");
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory).build()) {
      final XmlDBCollection original = store.create("collection", xml("<before/>"));
      assertSame(original, store.lookup("collection"));
      original.close();
      final XmlDBCollection reopened = requireNonNull(store.lookup("collection"));
      assertNotSame(original, reopened);
      assertFalse(original.getDatabase().isOpen());
      assertTrue(reopened.getDatabase().isOpen());
      assertSame(reopened, store.lookup("collection"));
      try (final XmlResourceSession session = reopened.getDatabase().beginResourceSession("resource1");
          final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertEquals("before", requireNonNull(reader.getName()).getLocalName());
      }
      if (closeBeforeDrop) {
        reopened.close();
      }
      store.drop("collection");
      assertFalse(reopened.getDatabase().isOpen());
      assertFalse(Files.exists(databasePath));
      assertNull(store.lookup("collection"));
      final XmlDBCollection replacement = store.create("collection", xml("<after/>"));
      assertSame(replacement, store.lookup("collection"));
      try (final XmlResourceSession session = replacement.getDatabase().beginResourceSession("resource1");
          final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertEquals("after", requireNonNull(reader.getName()).getLocalName());
      }
    } finally {
      Databases.removeDatabase(databasePath);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"loader", "readers", "strings", "paths"})
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void jsonSymbolicAliasesKeepCollectionAndDatabaseIdentity(final String creationPath) throws Exception {
    jsonAliasesKeepCollectionAndDatabaseIdentity(true, creationPath);
  }

  @Test
  void jsonLexicalAliasesKeepCollectionAndDatabaseIdentity() throws Exception {
    jsonAliasesKeepCollectionAndDatabaseIdentity(false, "loader");
  }

  private void jsonAliasesKeepCollectionAndDatabaseIdentity(final boolean symbolic, final String creationPath)
      throws Exception {
    final Path storePath = directory.resolve("store");
    final Path otherPath = directory.resolve("other");
    final Path localDatabasePath = storePath.resolve("orders");
    final Path targetDatabasePath = otherPath.resolve("orders");
    final String aliasName = symbolic
        ? "alias"
        : "../other/./orders";
    final Path aliasPath = storePath.resolve(aliasName);
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storePath).build();
        final BasicJsonDBStore other = BasicJsonDBStore.newBuilder().location(otherPath).build()) {
      store.create("orders", "resource1", "[\"local\"]").close();
      other.create("orders", "resource1", "[\"target\"]").close();
      final Path canonicalTarget = targetDatabasePath.toRealPath();
      if (symbolic) {
        Files.createSymbolicLink(aliasPath, canonicalTarget);
      }

      final JsonDBCollection alias = requireNonNull(store.lookup(aliasName));
      assertEquals(aliasName, alias.getName());
      assertEquals("target", jsonValue(alias));
      final JsonDBCollection orders = requireNonNull(store.lookup("orders"));
      assertNotSame(alias, orders);
      assertEquals("orders", orders.getName());
      assertEquals("local", jsonValue(orders));
      for (int i = 0; i < 3; i++) {
        assertSame(alias, store.lookup(aliasName));
      }
      assertEquals(1, requireNonNull(DatabasesInternals.getOpenDatabases().get(canonicalTarget)).size());

      if (symbolic) {
        Files.delete(aliasPath);
        Files.createSymbolicLink(aliasPath, localDatabasePath.toRealPath());
        final JsonDBCollection redirected = requireNonNull(store.lookup(aliasName));
        assertNotSame(alias, redirected);
        assertEquals(aliasName, redirected.getName());
        assertEquals("local", jsonValue(redirected));
        Files.delete(aliasPath);
        Files.createSymbolicLink(aliasPath, canonicalTarget);
        assertSame(alias, store.lookup(aliasName));
      }

      alias.close();
      final JsonDBCollection reopened = requireNonNull(store.lookup(aliasName));
      assertNotSame(alias, reopened);
      assertEquals("target", jsonValue(reopened));
      assertSame(reopened, store.lookup(aliasName));
      final JsonDBCollection canonical = requireNonNull(store.lookup(canonicalTarget.toString()));
      assertNotSame(reopened, canonical);
      assertEquals(2, requireNonNull(DatabasesInternals.getOpenDatabases().get(canonicalTarget)).size());

      final JsonDBCollection replacement = switch (creationPath) {
        case "readers" -> store.create(aliasName, Set.of(new JsonReader(new StringReader("[\"replacement\"]"))));
        case "strings" ->
          store.createFromJsonStrings(aliasName, new ArrayStream<>(new Str[] {new Str("[\"replacement\"]")}));
        case "paths" -> store.createFromPaths(aliasName, new ArrayStream<>(
            new Path[] {Files.writeString(directory.resolve("replacement.json"), "[\"replacement\"]")}));
        default -> store.create(aliasName, "resource1", "[\"replacement\"]");
      };
      assertFalse(reopened.getDatabase().isOpen());
      assertFalse(canonical.getDatabase().isOpen());
      assertEquals(aliasName, replacement.getName());
      assertSame(replacement, store.lookup(aliasName));
      assertEquals("replacement", jsonValue(replacement));
      assertEquals(1, requireNonNull(DatabasesInternals.getOpenDatabases().get(canonicalTarget)).size());
      assertSame(orders, store.lookup("orders"));
      assertEquals("local", jsonValue(orders));

      store.drop(aliasName);
      assertFalse(replacement.getDatabase().isOpen());
      assertFalse(Files.exists(targetDatabasePath));
      assertFalse(DatabasesInternals.getOpenDatabases().containsKey(canonicalTarget));
      assertNull(store.lookup(aliasName));
      other.create("orders", "resource1", "[\"after-drop\"]").close();
      final JsonDBCollection afterDrop = requireNonNull(store.lookup(aliasName));
      assertNotSame(replacement, afterDrop);
      assertEquals(aliasName, afterDrop.getName());
      assertEquals("after-drop", jsonValue(afterDrop));
      assertSame(afterDrop, store.lookup(aliasName));
      assertSame(orders, store.lookup("orders"));
      assertEquals("local", jsonValue(orders));
    } finally {
      Databases.removeDatabase(localDatabasePath);
      Databases.removeDatabase(targetDatabasePath);
    }
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void xmlSymbolicAliasesKeepCollectionAndDatabaseIdentity() throws Exception {
    xmlAliasesKeepCollectionAndDatabaseIdentity(true);
  }

  @Test
  void xmlLexicalAliasesKeepCollectionAndDatabaseIdentity() throws Exception {
    xmlAliasesKeepCollectionAndDatabaseIdentity(false);
  }

  private void xmlAliasesKeepCollectionAndDatabaseIdentity(final boolean symbolic) throws Exception {
    final Path storePath = directory.resolve("store");
    final Path otherPath = directory.resolve("other");
    final Path localDatabasePath = storePath.resolve("orders");
    final Path targetDatabasePath = otherPath.resolve("orders");
    final String aliasName = symbolic
        ? "alias"
        : "../other/./orders";
    final Path aliasPath = storePath.resolve(aliasName);
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(storePath).build();
        final BasicXmlDBStore other = BasicXmlDBStore.newBuilder().location(otherPath).build()) {
      store.create("orders", xml("<local/>")).close();
      other.create("orders", xml("<target/>")).close();
      final Path canonicalTarget = targetDatabasePath.toRealPath();
      if (symbolic) {
        Files.createSymbolicLink(aliasPath, canonicalTarget);
      }

      final XmlDBCollection alias = requireNonNull(store.lookup(aliasName));
      assertEquals(aliasName, alias.getName());
      assertEquals("target", xmlValue(alias));
      final XmlDBCollection orders = requireNonNull(store.lookup("orders"));
      assertNotSame(alias, orders);
      assertEquals("orders", orders.getName());
      assertEquals("local", xmlValue(orders));
      for (int i = 0; i < 3; i++) {
        assertSame(alias, store.lookup(aliasName));
      }
      assertEquals(1, requireNonNull(DatabasesInternals.getOpenDatabases().get(canonicalTarget)).size());

      if (symbolic) {
        Files.delete(aliasPath);
        Files.createSymbolicLink(aliasPath, localDatabasePath.toRealPath());
        final XmlDBCollection redirected = requireNonNull(store.lookup(aliasName));
        assertNotSame(alias, redirected);
        assertEquals(aliasName, redirected.getName());
        assertEquals("local", xmlValue(redirected));
        Files.delete(aliasPath);
        Files.createSymbolicLink(aliasPath, canonicalTarget);
        assertSame(alias, store.lookup(aliasName));
      }

      alias.close();
      final XmlDBCollection reopened = requireNonNull(store.lookup(aliasName));
      assertNotSame(alias, reopened);
      assertEquals("target", xmlValue(reopened));
      assertSame(reopened, store.lookup(aliasName));
      final XmlDBCollection canonical = requireNonNull(store.lookup(canonicalTarget.toString()));
      assertNotSame(reopened, canonical);
      assertEquals(2, requireNonNull(DatabasesInternals.getOpenDatabases().get(canonicalTarget)).size());

      final XmlDBCollection replacement = store.create(aliasName, xml("<replacement/>"));
      assertFalse(reopened.getDatabase().isOpen());
      assertFalse(canonical.getDatabase().isOpen());
      assertEquals(aliasName, replacement.getName());
      assertSame(replacement, store.lookup(aliasName));
      assertEquals("replacement", xmlValue(replacement));
      assertEquals(1, requireNonNull(DatabasesInternals.getOpenDatabases().get(canonicalTarget)).size());
      assertSame(orders, store.lookup("orders"));
      assertEquals("local", xmlValue(orders));

      store.drop(aliasName);
      assertFalse(replacement.getDatabase().isOpen());
      assertFalse(Files.exists(targetDatabasePath));
      assertFalse(DatabasesInternals.getOpenDatabases().containsKey(canonicalTarget));
      assertNull(store.lookup(aliasName));
      other.create("orders", xml("<after-drop/>")).close();
      final XmlDBCollection afterDrop = requireNonNull(store.lookup(aliasName));
      assertNotSame(replacement, afterDrop);
      assertEquals(aliasName, afterDrop.getName());
      assertEquals("after-drop", xmlValue(afterDrop));
      assertSame(afterDrop, store.lookup(aliasName));
      assertSame(orders, store.lookup("orders"));
      assertEquals("local", xmlValue(orders));
    } finally {
      Databases.removeDatabase(localDatabasePath);
      Databases.removeDatabase(targetDatabasePath);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void jsonRecreatesThroughDanglingAlias(final boolean relativeTarget) throws Exception {
    final Path storePath = directory.resolve("store");
    final Path targetDatabasePath = directory.resolve("other/orders");
    final Path plainDatabasePath = storePath.resolve("plain");
    final Path aliasPath = storePath.resolve("alias");
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storePath).build();
        final BasicJsonDBStore other = BasicJsonDBStore.newBuilder().location(targetDatabasePath.getParent()).build()) {
      final JsonDBCollection plain = store.create("plain", "resource1", "[\"plain\"]");
      assertEquals(plainDatabasePath.toRealPath(), plain.getDatabase().getDatabaseConfig().getDatabaseFile());
      assertFalse(Files.isSymbolicLink(plainDatabasePath));
      other.create("orders", "resource1", "[\"before\"]").close();
      final Path linkTarget = relativeTarget
          ? storePath.relativize(targetDatabasePath)
          : targetDatabasePath;
      Files.createSymbolicLink(aliasPath, linkTarget);
      final JsonDBCollection original = requireNonNull(store.lookup("alias"));
      store.drop("alias");
      assertFalse(original.getDatabase().isOpen());
      assertTrue(Files.isSymbolicLink(aliasPath));
      assertFalse(Files.exists(aliasPath));
      final JsonDBCollection replacement = store.create("alias", "resource1", "[\"after\"]");
      assertEquals("alias", replacement.getName());
      assertEquals(targetDatabasePath.toRealPath(), replacement.getDatabase().getDatabaseConfig().getDatabaseFile());
      assertEquals(linkTarget, Files.readSymbolicLink(aliasPath));
      assertEquals("after", jsonValue(replacement));
      replacement.close();
      final JsonDBCollection reopened = requireNonNull(store.lookup("alias"));
      assertNotSame(replacement, reopened);
      assertSame(reopened, store.lookup("alias"));
      assertEquals("after", jsonValue(reopened));
      assertEquals("plain", jsonValue(requireNonNull(store.lookup("plain"))));
    } finally {
      Databases.removeDatabase(plainDatabasePath);
      Databases.removeDatabase(targetDatabasePath);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void xmlRecreatesThroughDanglingAlias(final boolean relativeTarget) throws Exception {
    final Path storePath = directory.resolve("store");
    final Path targetDatabasePath = directory.resolve("other/orders");
    final Path plainDatabasePath = storePath.resolve("plain");
    final Path aliasPath = storePath.resolve("alias");
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(storePath).build();
        final BasicXmlDBStore other = BasicXmlDBStore.newBuilder().location(targetDatabasePath.getParent()).build()) {
      final XmlDBCollection plain = store.create("plain", xml("<plain/>"));
      assertEquals(plainDatabasePath.toRealPath(), plain.getDatabase().getDatabaseConfig().getDatabaseFile());
      assertFalse(Files.isSymbolicLink(plainDatabasePath));
      other.create("orders", xml("<before/>")).close();
      final Path linkTarget = relativeTarget
          ? storePath.relativize(targetDatabasePath)
          : targetDatabasePath;
      Files.createSymbolicLink(aliasPath, linkTarget);
      final XmlDBCollection original = requireNonNull(store.lookup("alias"));
      store.drop("alias");
      assertFalse(original.getDatabase().isOpen());
      assertTrue(Files.isSymbolicLink(aliasPath));
      assertFalse(Files.exists(aliasPath));
      final XmlDBCollection replacement = store.create("alias", xml("<after/>"));
      assertEquals("alias", replacement.getName());
      assertEquals(targetDatabasePath.toRealPath(), replacement.getDatabase().getDatabaseConfig().getDatabaseFile());
      assertEquals(linkTarget, Files.readSymbolicLink(aliasPath));
      assertEquals("after", xmlValue(replacement));
      replacement.close();
      final XmlDBCollection reopened = requireNonNull(store.lookup("alias"));
      assertNotSame(replacement, reopened);
      assertSame(reopened, store.lookup("alias"));
      assertEquals("after", xmlValue(reopened));
      assertEquals("plain", xmlValue(requireNonNull(store.lookup("plain"))));
    } finally {
      Databases.removeDatabase(plainDatabasePath);
      Databases.removeDatabase(targetDatabasePath);
    }
  }

  private static String jsonValue(final JsonDBCollection collection) {
    try (final JsonResourceSession session = collection.getDatabase().beginResourceSession("resource1");
        final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertTrue(reader.moveToFirstChild());
      assertTrue(reader.moveToFirstChild());
      return reader.getValue();
    }
  }

  private static String xmlValue(final XmlDBCollection collection) {
    try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("resource1");
        final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertTrue(reader.moveToFirstChild());
      return requireNonNull(reader.getName()).getLocalName();
    }
  }

  private static DocumentParser xml(final String document) {
    return new DocumentParser(new ByteArrayInputStream(document.getBytes(StandardCharsets.UTF_8)));
  }
}
