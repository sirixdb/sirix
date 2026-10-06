package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class JsonCollectionResourceConfigurationTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4})
  void pathImportsCloseTheirReadersOnSuccessAndFailure(final int operation) throws Exception {
    try (final BasicJsonDBStore store = configuredStore(directory, VersioningType.SLIDING_SNAPSHOT, false)) {
      for (final boolean malformed : new boolean[] {false, true}) {
        final String name = "collection" + malformed;
        final JsonDBCollection collection = store.create(name);
        final Path file = Files.writeString(directory.resolve("input.json"), malformed
            ? "[\"one\""
            : "[\"one\"]");
        try (final JsonReader reader = spy(JsonShredder.createFileReader(file));
            final MockedStatic<JsonShredder> factory = mockStatic(JsonShredder.class)) {
          factory.when(() -> JsonShredder.createFileReader(file)).thenReturn(reader);
          if (malformed) {
            assertThrows(SirixIOException.class, () -> {
              if (operation == 0) {
                collection.add(file);
              } else {
                createFromPath(store, name, file, operation);
              }
            });
          } else if (operation == 0) {
            assertNotNull(collection.add(file));
          } else {
            assertEquals(1, createFromPath(store, name, file, operation).getDocumentCount());
          }
          verify(reader).close();
          Files.delete(file);
        }
      }
    }
  }

  private static JsonDBCollection createFromPath(final BasicJsonDBStore store, final String name, final Path file,
      final int operation) {
    final ArrayObject options = new ArrayObject(new QNm[0], new Sequence[0]);
    return switch (operation) {
      case 1 -> store.create(name, file);
      case 2 -> store.create(name, file, options);
      case 3 -> store.create(name, "resource", file);
      case 4 -> store.create(name, "resource", file, options);
      default -> throw new IllegalArgumentException("Unknown path import operation: " + operation);
    };
  }

  @Test
  void callerSuppliedReadersRemainOpen() throws Exception {
    try (final BasicJsonDBStore store = configuredStore(directory, VersioningType.SLIDING_SNAPSHOT, false);
        final JsonReader initial = spy(new JsonReader(new StringReader("[\"one\"]")));
        final JsonReader added = spy(new JsonReader(new StringReader("[\"two\"]")))) {
      final JsonDBCollection collection = store.create("collection", "seed", initial);
      assertNotNull(collection.add("added", added));
      verify(initial, never()).close();
      verify(added, never()).close();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void everyAddPathUsesStoreSettingsAndPreservesHistory(final VersioningType versioning) throws Exception {
    for (final boolean pathSummary : new boolean[] {false, true}) {
      final Path location = directory.resolve(Boolean.toString(pathSummary));
      try (final BasicJsonDBStore store = configuredStore(location, versioning, pathSummary);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        final JsonDBCollection collection = store.create("collection", "seed", "[\"one\"]");
        assertConfiguration(collection, "seed", versioning, pathSummary, false);
        assertNotNull(collection.add("[\"one\"]"));
        final Path file = Files.writeString(directory.resolve("input.json"), "[\"one\"]");
        assertNotNull(collection.add(file));
        try (final JsonReader reader = new JsonReader(new StringReader("[\"one\"]"))) {
          assertNotNull(collection.add("reader", reader));
        }
        try (final JsonReader reader = new JsonReader(new StringReader("[\"one\"]"))) {
          assertNotNull(collection.add("timestamp", reader,
              new ArrayObject(new QNm[] {new QNm("commitMessage"), new QNm("commitTimestamp")},
                  new Sequence[] {new Str("initial"), new Str("2020-01-01T00:00:00Z")})));
        }
        new Query(chain, "jn:store('collection','single','[\"one\"]',false())").evaluate(context);
        new Query(chain, "jn:store('collection',(),('[\"one\"]','[\"one\"]'),false())").evaluate(context);
        assertEquals(8, collection.getDocumentCount());
        for (final Path resource : collection.getDatabase().listResources()) {
          final String name = resource.getFileName().toString();
          assertConfiguration(collection, name, versioning, pathSummary, name.equals("timestamp"));
          writeHistory(collection, name);
        }
      }
      try (final BasicJsonDBStore store = configuredStore(location, versioning, pathSummary)) {
        final JsonDBCollection collection = store.lookup("collection");
        assertNotNull(collection);
        for (final Path resource : collection.getDatabase().listResources()) {
          final String name = resource.getFileName().toString();
          assertConfiguration(collection, name, versioning, pathSummary, name.equals("timestamp"));
          assertHistory(collection, name);
        }
        try (final JsonReader reader = new JsonReader(new StringReader("[\"one\"]"))) {
          assertNotNull(collection.add("reopened", reader));
        }
        assertConfiguration(collection, "reopened", versioning, pathSummary, false);
        writeHistory(collection, "reopened");
        assertHistory(collection, "reopened");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void emptyCollectionsPreserveSettingsAndHistoryWhenDuplicatesAreRejected(final VersioningType versioning)
      throws Exception {
    for (final boolean pathSummary : new boolean[] {false, true}) {
      final Path location = directory.resolve(Boolean.toString(pathSummary));
      try (final BasicJsonDBStore store = configuredStore(location, versioning, pathSummary)) {
        final JsonDBCollection collection = store.create("collection");
        assertNotNull(collection);
        assertEquals(0, collection.getDocumentCount());
        assertDuplicateRejected(store);
        assertSame(collection, store.lookup("collection"));
        try (final JsonReader reader = new JsonReader(new StringReader("[\"one\"]"))) {
          assertNotNull(collection.add("added", reader));
        }
        assertConfiguration(collection, "added", versioning, pathSummary, false);
        writeHistory(collection, "added");
        assertDuplicateRejected(store);
        assertSame(collection, store.lookup("collection"));
        assertEquals(1, collection.getDocumentCount());
        assertHistory(collection, "added");
      }
      try (final BasicJsonDBStore store = configuredStore(location, versioning, pathSummary)) {
        assertDuplicateRejected(store);
        final JsonDBCollection collection = store.lookup("collection");
        assertNotNull(collection);
        assertEquals(1, collection.getDocumentCount());
        assertConfiguration(collection, "added", versioning, pathSummary, false);
        assertHistory(collection, "added");
      }
    }
  }

  @Test
  void filesystemCreationFailuresAreNotReportedAsDuplicates() throws Exception {
    final Path blocker = Files.writeString(directory.resolve("blocked"), "preserved");
    try (final BasicJsonDBStore store = configuredStore(directory, VersioningType.SLIDING_SNAPSHOT, true)) {
      for (final String name : new String[] {"blocked", "blocked/collection"}) {
        final DocumentException failure = assertThrows(DocumentException.class, () -> store.create(name));
        final String message = failure.getMessage();
        assertNotNull(message);
        assertTrue(message.contains("Could not create document with name " + name));
        assertFalse(message.contains("exists"));
        assertEquals("preserved", Files.readString(blocker));
      }
      assertNotNull(store.create("available"));
    }
  }

  private static void assertDuplicateRejected(final BasicJsonDBStore store) {
    final DocumentException failure = assertThrows(DocumentException.class, () -> store.create("collection"));
    final String message = failure.getMessage();
    assertNotNull(message);
    assertTrue(message.contains("Document with name collection exists!"));
  }

  private static BasicJsonDBStore configuredStore(final Path location, final VersioningType versioning,
      final boolean pathSummary) {
    return BasicJsonDBStore.newBuilder()
                           .location(location)
                           .versioningType(versioning)
                           .storageType(pathSummary
                               ? StorageType.MEMORY_MAPPED
                               : StorageType.FILE_CHANNEL)
                           .hashType(HashType.NONE)
                           .storeDeweyIds(false)
                           .storeNodeHistory(false)
                           .buildPathSummary(pathSummary)
                           .buildPathStatistics(pathSummary)
                           .build();
  }

  private static void assertConfiguration(final JsonDBCollection collection, final String name,
      final VersioningType versioning, final boolean pathSummary, final boolean customTimestamp) {
    try (final JsonResourceSession session = collection.getDatabase().beginResourceSession(name)) {
      final ResourceConfiguration configuration = session.getResourceConfig();
      assertEquals(versioning, configuration.versioningType, name);
      assertEquals(pathSummary
          ? StorageType.MEMORY_MAPPED
          : StorageType.FILE_CHANNEL, configuration.storageType, name);
      assertEquals(HashType.NONE, configuration.hashType, name);
      assertEquals(pathSummary, configuration.withPathSummary, name);
      assertEquals(pathSummary, configuration.withPathStatistics, name);
      assertFalse(configuration.areDeweyIDsStored, name);
      assertFalse(configuration.storeNodeHistory(), name);
      assertFalse(configuration.useTextCompression, name);
      assertEquals(customTimestamp, configuration.customCommitTimestamps(), name);
    }
  }

  private static void writeHistory(final JsonDBCollection collection, final String name) {
    try (final JsonResourceSession session = collection.getDatabase().beginResourceSession(name);
        final var trx = session.beginNodeTrx()) {
      for (int revision = 2; revision <= 5; revision++) {
        trx.moveToDocumentRoot();
        assertTrue(trx.moveToFirstChild());
        assertTrue(trx.moveToFirstChild());
        trx.setStringValue("revision" + revision);
        trx.commit();
      }
    }
  }

  private static void assertHistory(final JsonDBCollection collection, final String name) {
    try (final JsonResourceSession session = collection.getDatabase().beginResourceSession(name)) {
      assertEquals(5, session.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= 5; revision++) {
        try (final var trx = session.beginNodeReadOnlyTrx(revision)) {
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          assertEquals(revision == 1
              ? "one"
              : "revision" + revision, trx.getValue(), name);
        }
      }
    }
  }
}
