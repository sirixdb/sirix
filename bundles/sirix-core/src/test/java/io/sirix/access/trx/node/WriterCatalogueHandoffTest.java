/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class WriterCatalogueHandoffTest {

  enum Drop {
    ALL, CAS, NAME
  }

  private @Nullable CountDownLatch releaseHarden;

  @AfterEach
  void clearHook() {
    if (releaseHarden != null) {
      releaseHarden.countDown();
    }
    AbstractNodeTrxImpl.asyncCommitTestHook = null;
  }

  @ParameterizedTest
  @EnumSource(Drop.class)
  // blockHarden initializes releaseHarden before this test uses it.
  @SuppressWarnings("NullAway")
  void jsonPendingDropsTransferTheCompleteCatalogue(final Drop drop, @TempDir final Path directory) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final IndexDef cas = IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
    final IndexDef sibling = IndexDefs.createCASIdxDef(false, Type.INR, cas.getPaths(), 1, IndexDef.DbType.JSON);
    final IndexDef name = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    final Set<IndexDef> definitions = Set.of(cas, sibling, name);
    final Set<IndexDef> dropped = dropped(drop, definitions, cas, name);
    final Set<IndexDef> retained = new HashSet<>(definitions);
    retained.removeAll(dropped);
    final long valueKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(resource());
      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"id\":1}]"), JsonNodeTrx.Commit.NO);
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          trx.moveToFirstChild();
          valueKey = trx.getNodeKey();
          trx.commit();
        }
        try (final JsonNodeTrx trx = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          final CountDownLatch hardenEntered = blockHarden();
          final Set<IndexDef> pendingDefinitions;
          try {
            session.getWtxIndexController(trx.getRevisionNumber()).dropIndexes(dropped, trx);
            assertTrue(trx.moveTo(valueKey));
            trx.setNumberValue(2);
            trx.setNumberValue(3);
            hardenEntered.await();
            assertPendingRevision(session, trx);
            pendingDefinitions = session.getWtxIndexController(trx.getRevisionNumber()).getIndexes().getIndexDefs();
          } finally {
            releaseHarden.countDown();
            AbstractNodeTrxImpl.asyncCommitTestHook = null;
          }
          trx.awaitPendingAsyncCommit();
          trx.commit();
          assertEquals(retained.size(), pendingDefinitions.size(), "pending successor catalogue size");
          assertEquals(retained, pendingDefinitions);
          assertCatalogue(session.getWtxIndexController(trx.getRevisionNumber()), retained, dropped);
          try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
            assertJsonLookups(session, reader, retained, dropped, valueKey, 3);
          }
        }
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession("data")) {
      for (final int revision : new int[] {1, 2, 3}) {
        try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          assertJsonLookups(session, reader, revision == 1
              ? definitions
              : retained,
              revision == 1
                  ? Set.of()
                  : dropped,
              valueKey, revision);
        }
      }
      try (final JsonNodeTrx trx = session.beginNodeTrx()) {
        assertCatalogue(session.getWtxIndexController(trx.getRevisionNumber()), retained, dropped);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Drop.class)
  // blockHarden initializes releaseHarden before this test uses it.
  @SuppressWarnings("NullAway")
  void xmlPendingDropsTransferTheCompleteCatalogue(final Drop drop, @TempDir final Path directory) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    final IndexDef cas =
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(parse("/root/value")), 0, IndexDef.DbType.XML);
    final IndexDef sibling = IndexDefs.createCASIdxDef(false, Type.STR, cas.getPaths(), 1, IndexDef.DbType.XML);
    final IndexDef name = IndexDefs.createNameIdxDef(0, IndexDef.DbType.XML);
    final Set<IndexDef> definitions = Set.of(cas, sibling, name);
    final Set<IndexDef> dropped = dropped(drop, definitions, cas, name);
    final Set<IndexDef> retained = new HashSet<>(definitions);
    retained.removeAll(dropped);
    final long valueKey;
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(resource());
      try (final XmlResourceSession session = database.beginResourceSession("data")) {
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          trx.insertElementAsFirstChild(new QNm("root"));
          trx.insertElementAsFirstChild(new QNm("value"));
          valueKey = trx.insertTextAsFirstChild("1").getNodeKey();
          trx.commit();
        }
        try (final XmlNodeTrx trx = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          final CountDownLatch hardenEntered = blockHarden();
          final Set<IndexDef> pendingDefinitions;
          try {
            session.getWtxIndexController(trx.getRevisionNumber()).dropIndexes(dropped, trx);
            assertTrue(trx.moveTo(valueKey));
            trx.setValue("2");
            trx.setValue("3");
            hardenEntered.await();
            assertPendingRevision(session, trx);
            pendingDefinitions = session.getWtxIndexController(trx.getRevisionNumber()).getIndexes().getIndexDefs();
          } finally {
            releaseHarden.countDown();
            AbstractNodeTrxImpl.asyncCommitTestHook = null;
          }
          trx.awaitPendingAsyncCommit();
          trx.commit();
          assertEquals(retained.size(), pendingDefinitions.size(), "pending successor catalogue size");
          assertEquals(retained, pendingDefinitions);
          assertCatalogue(session.getWtxIndexController(trx.getRevisionNumber()), retained, dropped);
          try (final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
            assertXmlLookups(session, reader, retained, dropped, valueKey, 3);
          }
        }
      }
    }
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        final XmlResourceSession session = database.beginResourceSession("data")) {
      for (final int revision : new int[] {1, 2, 3}) {
        try (final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          assertXmlLookups(session, reader, revision == 1
              ? definitions
              : retained,
              revision == 1
                  ? Set.of()
                  : dropped,
              valueKey, revision);
        }
      }
      try (final XmlNodeTrx trx = session.beginNodeTrx()) {
        assertCatalogue(session.getWtxIndexController(trx.getRevisionNumber()), retained, dropped);
      }
    }
  }

  // The callback runs only after releaseHarden has been initialized below.
  @SuppressWarnings("NullAway")
  private CountDownLatch blockHarden() {
    final CountDownLatch entered = new CountDownLatch(1);
    releaseHarden = new CountDownLatch(1);
    AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
      if ("before-harden".equals(stage)) {
        entered.countDown();
        try {
          releaseHarden.await();
        } catch (final InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(interrupted);
        }
      }
    };
    return entered;
  }

  private static void assertPendingRevision(final ResourceSession<?, ?> session, final NodeReadOnlyTrx trx) {
    assertEquals(3, trx.getRevisionNumber());
    assertEquals(1, session.getMostRecentRevisionNumber());
    final Path indexes =
        session.getResourceConfig().getResource().resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath());
    assertTrue(Files.exists(indexes.resolve("1.xml")));
    assertTrue(Files.notExists(indexes.resolve("2.xml")));
  }

  private static void assertCatalogue(final IndexController<?, ?> controller, final Set<IndexDef> retained,
      final Set<IndexDef> dropped) {
    assertEquals(retained, controller.getIndexes().getIndexDefs());
    for (final IndexDef definition : retained) {
      assertNotNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
    }
    for (final IndexDef definition : dropped) {
      assertNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
    }
    assertEquals(retained.stream().anyMatch(IndexDef::isCasIndex), controller.containsIndex(IndexType.CAS));
    assertEquals(retained.stream().anyMatch(IndexDef::isNameIndex), controller.containsIndex(IndexType.NAME));
  }

  private static void assertJsonLookups(final JsonResourceSession session, final JsonNodeReadOnlyTrx reader,
      final Set<IndexDef> retained, final Set<IndexDef> dropped, final long valueKey, final int value) {
    assertTrue(reader.moveTo(valueKey));
    assertEquals(value, reader.getNumberValue().intValue());
    final var controller = session.getRtxIndexController(reader.getRevisionNumber());
    assertCatalogue(controller, retained, dropped);
    for (final IndexDef definition : retained) {
      if (definition.isCasIndex()) {
        final var lookup =
            controller.openCASIndex(reader.getStorageEngineReader(), definition, controller.createCASFilter(
                Set.of("/[]/id"), new Int32(value), SearchMode.EQUAL, new JsonPCRCollector(reader)));
        assertTrue(lookup.hasNext());
        assertTrue(lookup.next().contains(valueKey));
        assertFalse(lookup.hasNext());
        assertFalse(controller
                              .openCASIndex(reader.getStorageEngineReader(), definition,
                                  controller.createCASFilter(Set.of("/[]/id"), new Int32(value - 1), SearchMode.EQUAL,
                                      new JsonPCRCollector(reader)))
                              .hasNext());
      }
    }
  }

  private static void assertXmlLookups(final XmlResourceSession session, final XmlNodeReadOnlyTrx reader,
      final Set<IndexDef> retained, final Set<IndexDef> dropped, final long valueKey, final int value) {
    assertTrue(reader.moveTo(valueKey));
    assertEquals(Integer.toString(value), reader.getValue());
    final var controller = session.getRtxIndexController(reader.getRevisionNumber());
    assertCatalogue(controller, retained, dropped);
    for (final IndexDef definition : retained) {
      if (definition.isCasIndex()) {
        final var lookup = controller.openCASIndex(reader.getStorageEngineReader(), definition,
            controller.createCASFilter(Set.of("/root/value"), new Str(Integer.toString(value)), SearchMode.EQUAL,
                new XmlPCRCollector(reader)));
        assertTrue(lookup.hasNext());
        assertTrue(lookup.next().contains(valueKey));
        assertFalse(lookup.hasNext());
        assertFalse(controller
                              .openCASIndex(reader.getStorageEngineReader(), definition,
                                  controller.createCASFilter(Set.of("/root/value"),
                                      new Str(Integer.toString(value - 1)), SearchMode.EQUAL,
                                      new XmlPCRCollector(reader)))
                              .hasNext());
      }
    }
  }

  private static Set<IndexDef> dropped(final Drop drop, final Set<IndexDef> definitions, final IndexDef cas,
      final IndexDef name) {
    return switch (drop) {
      case ALL -> definitions;
      case CAS -> Set.of(cas);
      case NAME -> Set.of(name);
    };
  }

  private static ResourceConfiguration resource() {
    return ResourceConfiguration.newBuilder("data")
                                .storageType(StorageType.FILE_CHANNEL)
                                .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                .maxNumberOfRevisionsToRestore(3)
                                .storeDiffs(false)
                                .build();
  }
}
