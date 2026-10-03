package io.sirix.query;

import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

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

  private static DocumentParser xml(final String document) {
    return new DocumentParser(new ByteArrayInputStream(document.getBytes(StandardCharsets.UTF_8)));
  }
}
