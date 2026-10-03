/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class WriterCatalogueRecoveryTest {

  enum Recovery {
    REVERT, ROLLBACK, CLOSE
  }

  @AfterEach
  void clearFault() {
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
  }

  @ParameterizedTest
  @CsvSource({"KEEP_OPEN, REVERT", "KEEP_OPEN_ASYNC_COMMIT, REVERT", "KEEP_OPEN, ROLLBACK",
      "KEEP_OPEN_ASYNC_COMMIT, ROLLBACK", "KEEP_OPEN, CLOSE", "KEEP_OPEN_ASYNC_COMMIT, CLOSE"})
  void jsonRecoveryInheritsSkippedCatalogueAndMaintainsLookups(final AfterCommitState mode, final Recovery recovery,
      @TempDir final Path directory) {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final IndexDef cas = jsonCas(0);
    final long valueKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(resource());
      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"id\":1}]"), JsonNodeTrx.Commit.NO);
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(cas), trx);
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          trx.moveToFirstChild();
          valueKey = trx.getNodeKey();
          trx.commit();
        }
        try (final JsonNodeTrx trx = session.beginNodeTrx(1, mode)) {
          assertTrue(trx.moveTo(valueKey));
          trx.setNumberValue(2);
          if (recovery == Recovery.REVERT) {
            trx.setNumberValue(4);
          } else {
            final OutOfMemoryError failure = failNextSuccessor();
            try {
              assertSame(failure, assertThrows(OutOfMemoryError.class, () -> trx.setNumberValue(4)));
            } finally {
              NodeStorageEngineWriter.asyncFlushFaultHook = null;
            }
          }
          trx.awaitPendingAsyncCommit();
          assertSkippedCatalogue(session, 2);
          if (recovery == Recovery.REVERT) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(jsonCas(1)), trx);
            trx.revertTo(1);
          } else if (recovery == Recovery.ROLLBACK) {
            trx.rollback();
          } else {
            trx.close();
          }
          if (recovery != Recovery.CLOSE) {
            assertJsonRecovery(session, trx, cas, valueKey, recovery == Recovery.REVERT ? 1 : 2);
          }
        }
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession("data")) {
      if (recovery == Recovery.CLOSE) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          assertJsonRecovery(session, trx, cas, valueKey, 2);
        }
      }
      try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(2)) {
        assertJsonLookup(session, reader, cas, valueKey, 2);
      }
      try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertJsonLookup(session, reader, cas, valueKey, 7);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"KEEP_OPEN, REVERT", "KEEP_OPEN_ASYNC_COMMIT, REVERT", "KEEP_OPEN, ROLLBACK",
      "KEEP_OPEN_ASYNC_COMMIT, ROLLBACK", "KEEP_OPEN, CLOSE", "KEEP_OPEN_ASYNC_COMMIT, CLOSE"})
  void xmlRecoveryInheritsSkippedCatalogueAndMaintainsLookups(final AfterCommitState mode, final Recovery recovery,
      @TempDir final Path directory) {
    final Path databasePath = directory.resolve("database");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    final IndexDef cas = xmlCas(0);
    final long valueKey;
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(resource());
      try (final XmlResourceSession session = database.beginResourceSession("data")) {
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(cas), trx);
          trx.insertElementAsFirstChild(new QNm("root"));
          trx.insertElementAsFirstChild(new QNm("value"));
          valueKey = trx.insertTextAsFirstChild("1").getNodeKey();
          trx.commit();
        }
        try (final XmlNodeTrx trx = session.beginNodeTrx(1, mode)) {
          assertTrue(trx.moveTo(valueKey));
          trx.setValue("2");
          if (recovery == Recovery.REVERT) {
            trx.setValue("4");
          } else {
            final OutOfMemoryError failure = failNextSuccessor();
            try {
              assertSame(failure, assertThrows(OutOfMemoryError.class, () -> trx.setValue("4")));
            } finally {
              NodeStorageEngineWriter.asyncFlushFaultHook = null;
            }
          }
          trx.awaitPendingAsyncCommit();
          assertSkippedCatalogue(session, 2);
          if (recovery == Recovery.REVERT) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(xmlCas(1)), trx);
            trx.revertTo(1);
          } else if (recovery == Recovery.ROLLBACK) {
            trx.rollback();
          } else {
            trx.close();
          }
          if (recovery != Recovery.CLOSE) {
            assertXmlRecovery(session, trx, cas, valueKey, recovery == Recovery.REVERT ? "1" : "2");
          }
        }
      }
    }
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        final XmlResourceSession session = database.beginResourceSession("data")) {
      if (recovery == Recovery.CLOSE) {
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          assertXmlRecovery(session, trx, cas, valueKey, "2");
        }
      }
      try (final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(2)) {
        assertXmlLookup(session, reader, cas, valueKey, "2");
      }
      try (final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertXmlLookup(session, reader, cas, valueKey, "7");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_COMMIT"})
  void emptyPersistedCatalogueOverridesOlderDefinitionsAndUncommittedChanges(final AfterCommitState mode,
      @TempDir final Path directory) {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final IndexDef cas = jsonCas(0);
    final long valueKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(resource());
      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          valueKey = trx.insertNumberValueAsFirstChild(1).getNodeKey();
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(cas), trx);
          trx.commit();
          session.getWtxIndexController(trx.getRevisionNumber()).dropIndexes(Set.of(cas), trx);
          trx.commit();
          assertTrue(session.getRtxIndexController(2).getIndexes().getIndexDefs().isEmpty());
        }
        try (final JsonNodeTrx trx = session.beginNodeTrx(1, mode)) {
          assertTrue(trx.moveTo(valueKey));
          trx.setNumberValue(3);
          trx.setNumberValue(4);
          trx.awaitPendingAsyncCommit();
          assertSkippedCatalogue(session, 3);
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(jsonCas(1)), trx);
          trx.rollback();
          assertTrue(session.getWtxIndexController(trx.getRevisionNumber()).getIndexes().getIndexDefs().isEmpty());
          trx.revertTo(1);
          assertTrue(session.getWtxIndexController(trx.getRevisionNumber()).getIndexes().getIndexDefs().isEmpty());
        }
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession("data");
        final JsonNodeTrx trx = session.beginNodeTrx()) {
      assertTrue(session.getWtxIndexController(trx.getRevisionNumber()).getIndexes().getIndexDefs().isEmpty());
      assertEquals(1, session.getRtxIndexController(1).getIndexes().getIndexDefs().size());
      assertTrue(session.getRtxIndexController(3).getIndexes().getIndexDefs().isEmpty());
    }
  }

  private static void assertJsonRecovery(final JsonResourceSession session, final JsonNodeTrx trx,
      final IndexDef cas, final long valueKey, final int oldValue) {
    assertEquals(1, session.getWtxIndexController(trx.getRevisionNumber()).getIndexes().getIndexDefs().size());
    assertNotNull(session.getWtxIndexController(trx.getRevisionNumber())
                         .getIndexes().getIndexDef(cas.getID(), cas.getType()));
    assertTrue(trx.moveTo(valueKey));
    assertEquals(oldValue, trx.getNumberValue().intValue());
    trx.setNumberValue(7);
    trx.commit();
    try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertJsonLookup(session, reader, cas, valueKey, 7);
      final var controller = session.getRtxIndexController(reader.getRevisionNumber());
      assertFalse(controller.openCASIndex(reader.getStorageEngineReader(), cas,
          controller.createCASFilter(Set.of("/[]/id"), new Int32(oldValue), SearchMode.EQUAL,
              new JsonPCRCollector(reader))).hasNext());
    }
  }

  private static void assertXmlRecovery(final XmlResourceSession session, final XmlNodeTrx trx,
      final IndexDef cas, final long valueKey, final String oldValue) {
    assertEquals(1, session.getWtxIndexController(trx.getRevisionNumber()).getIndexes().getIndexDefs().size());
    assertNotNull(session.getWtxIndexController(trx.getRevisionNumber())
                         .getIndexes().getIndexDef(cas.getID(), cas.getType()));
    assertTrue(trx.moveTo(valueKey));
    assertEquals(oldValue, trx.getValue());
    trx.setValue("7");
    trx.commit();
    try (final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertXmlLookup(session, reader, cas, valueKey, "7");
      final var controller = session.getRtxIndexController(reader.getRevisionNumber());
      assertFalse(controller.openCASIndex(reader.getStorageEngineReader(), cas,
          controller.createCASFilter(Set.of("/root/value"), new Str(oldValue), SearchMode.EQUAL,
              new XmlPCRCollector(reader))).hasNext());
    }
  }

  private static void assertJsonLookup(final JsonResourceSession session, final JsonNodeReadOnlyTrx reader,
      final IndexDef cas, final long valueKey, final int value) {
    assertTrue(reader.moveTo(valueKey));
    assertEquals(value, reader.getNumberValue().intValue());
    final var controller = session.getRtxIndexController(reader.getRevisionNumber());
    assertEquals(1, controller.getIndexes().getIndexDefs().size());
    final var lookup = controller.openCASIndex(reader.getStorageEngineReader(), cas,
        controller.createCASFilter(Set.of("/[]/id"), new Int32(value), SearchMode.EQUAL, new JsonPCRCollector(reader)));
    assertTrue(lookup.hasNext());
    final var references = lookup.next();
    assertEquals(1, references.getNodeKeys().getLongCardinality());
    assertTrue(references.contains(valueKey));
    assertFalse(lookup.hasNext());
  }

  private static void assertXmlLookup(final XmlResourceSession session, final XmlNodeReadOnlyTrx reader,
      final IndexDef cas, final long valueKey, final String value) {
    assertTrue(reader.moveTo(valueKey));
    assertEquals(value, reader.getValue());
    final var controller = session.getRtxIndexController(reader.getRevisionNumber());
    assertEquals(1, controller.getIndexes().getIndexDefs().size());
    final var lookup = controller.openCASIndex(reader.getStorageEngineReader(), cas,
        controller.createCASFilter(Set.of("/root/value"), new Str(value), SearchMode.EQUAL, new XmlPCRCollector(reader)));
    assertTrue(lookup.hasNext());
    final var references = lookup.next();
    assertEquals(1, references.getNodeKeys().getLongCardinality());
    assertTrue(references.contains(valueKey));
    assertFalse(lookup.hasNext());
  }

  private static void assertSkippedCatalogue(final ResourceSession<?, ?> session, final int revision) {
    final Path indexes = session.getResourceConfig().getResource()
                                .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
    assertEquals(revision, session.getMostRecentRevisionNumber());
    assertTrue(Files.exists(indexes.resolve((revision - 1) + ".xml")));
    assertTrue(Files.notExists(indexes.resolve(revision + ".xml")));
  }

  private static OutOfMemoryError failNextSuccessor() {
    final OutOfMemoryError failure = new OutOfMemoryError("injected after skipped catalogue revision");
    NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
      if ("constructor-before-local-caches".equals(site)) {
        throw failure;
      }
    };
    return failure;
  }

  private static ResourceConfiguration resource() {
    return ResourceConfiguration.newBuilder("data").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build();
  }

  private static IndexDef jsonCas(final int id) {
    return IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), id,
        IndexDef.DbType.JSON);
  }

  private static IndexDef xmlCas(final int id) {
    return IndexDefs.createCASIdxDef(false, Type.STR, Set.of(parse("/root/value")), id, IndexDef.DbType.XML);
  }
}
