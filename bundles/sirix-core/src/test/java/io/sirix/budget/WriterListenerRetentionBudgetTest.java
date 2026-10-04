/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractIndexController;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.reflect.Field;
import java.nio.file.Path;

import java.util.Map;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards revision-cached listeners retaining superseded storage engines. Counts the cache's actual
 * roots without GC scheduling or a wall-clock bound: baseline retained 130 listeners at 64 commits;
 * fixed code retains two at both 64 and 256 commits, and zero after writer close.
 */
@Isolated
final class WriterListenerRetentionBudgetTest {

  @Test
  void cachedListenersAreBoundedByActiveWritersAcrossManyCommits(@TempDir final Path directory) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .validTimePaths("validFrom", "validTo")
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        final long valueKey;
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(
                  "[{\"id\":0,\"validFrom\":\"2024-01-01T00:00:00Z\",\"validTo\":\"2025-01-01T00:00:00Z\"}]"),
              JsonNodeTrx.Commit.NO);
          final IndexDef cas = IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)),
              0, IndexDef.DbType.JSON);
          final IndexDef validTime = IndexDefs.createValidTimeIdxDef(
              Set.of(parse("/[]/validFrom", PathParser.Type.JSON), parse("/[]/validTo", PathParser.Type.JSON)), 0,
              IndexDef.DbType.JSON);
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(cas, validTime), trx);
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          trx.moveToFirstChild(); // A fused named number carries its value on the key itself.
          valueKey = trx.getNodeKey();
          for (int commit = 1; commit <= 256; commit++) {
            assertTrue(trx.moveTo(valueKey));
            trx.setNumberValue(commit);
            trx.commit();
            if (commit == 64 || commit == 256) {
              assertEquals(2, retainedListeners(session),
                  "only the active writer's CAS and VALIDTIME listeners may survive; commits=" + commit);
            }
          }
        }
        assertEquals(0, retainedListeners(session), "closing the last writer must retire every listener root");
        for (final int revision : new int[] {1, 64, 256}) {
          try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
            assertTrue(reader.moveTo(valueKey));
            assertEquals(revision, reader.getNumberValue().intValue());
            assertEquals(2, session.getRtxIndexController(revision).getIndexes().getIndexDefs().size());
          }
        }
      }
    }
  }

  @Test
  void jsonIndexedRevertsRetireListenersAndPreserveCatalogueAndLookups(@TempDir final Path directory) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final IndexDef cas = IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
    final IndexDef validTime = IndexDefs.createValidTimeIdxDef(
        Set.of(parse("/[]/validFrom", PathParser.Type.JSON), parse("/[]/validTo", PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
    final long valueKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .validTimePaths("validFrom", "validTo")
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("data")) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(
                  "[{\"id\":0,\"validFrom\":\"2024-01-01T00:00:00Z\",\"validTo\":\"2025-01-01T00:00:00Z\"}]"),
              JsonNodeTrx.Commit.NO);
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(cas, validTime), trx);
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          assertTrue(trx.moveToFirstChild());
          trx.moveToFirstChild();
          valueKey = trx.getNodeKey();
          for (int revision = 1; revision <= 26; revision++) {
            if (revision > 10) {
              trx.revertTo(1);
              assertEquals(revision, trx.getRevisionNumber());
              assertEquals(2, session.getWtxIndexController(revision).getIndexes().getIndexDefs().size());
              assertTrue(trx.moveTo(valueKey));
              assertEquals(1, trx.getNumberValue().intValue());
            }
            assertTrue(trx.moveTo(valueKey));
            trx.setNumberValue(revision);
            trx.commit();
            assertEquals(2, retainedListeners(session), "only the active writer may retain indexed listeners");
          }
        }
        assertEquals(0, retainedListeners(session));
      }
    }
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession("data")) {
      for (final int revision : new int[] {1, 10, 11, 26}) {
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          assertTrue(reader.moveTo(valueKey));
          assertEquals(revision, reader.getNumberValue().intValue());
          final var controller = session.getRtxIndexController(revision);
          assertEquals(2, controller.getIndexes().getIndexDefs().size());
          assertNotNull(controller.getIndexes().getIndexDef(validTime.getID(), validTime.getType()));
          final var lookup =
              controller.openCASIndex(reader.getStorageEngineReader(), cas, controller.createCASFilter(Set.of("/[]/id"),
                  new Int32(revision), SearchMode.EQUAL, new JsonPCRCollector(reader)));
          assertTrue(lookup.hasNext());
          final var references = lookup.next();
          assertEquals(1, references.getNodeKeys().getLongCardinality());
          assertTrue(references.contains(valueKey));
          assertFalse(lookup.hasNext());
        }
      }
    }
  }

  @Test
  void xmlIndexedRevertsRetireListenersAndPreserveCatalogueAndLookups(@TempDir final Path directory) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    final IndexDef cas =
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(parse("/root/value")), 0, IndexDef.DbType.XML);
    final IndexDef name = IndexDefs.createNameIdxDef(0, IndexDef.DbType.XML);
    final long valueKey;
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (final XmlResourceSession session = database.beginResourceSession("data")) {
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(cas, name), trx);
          trx.insertElementAsFirstChild(new QNm("root"));
          trx.insertElementAsFirstChild(new QNm("value"));
          valueKey = trx.insertTextAsFirstChild("0").getNodeKey();
          for (int revision = 1; revision <= 26; revision++) {
            if (revision > 10) {
              trx.revertTo(1);
              assertEquals(revision, trx.getRevisionNumber());
              assertEquals(2, session.getWtxIndexController(revision).getIndexes().getIndexDefs().size());
              assertTrue(trx.moveTo(valueKey));
              assertEquals("1", trx.getValue());
            }
            assertTrue(trx.moveTo(valueKey));
            trx.setValue(Integer.toString(revision));
            trx.commit();
            assertEquals(2, retainedListeners(session), "only the active writer may retain indexed listeners");
          }
        }
        assertEquals(0, retainedListeners(session));
      }
    }
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        final XmlResourceSession session = database.beginResourceSession("data")) {
      for (final int revision : new int[] {1, 10, 11, 26}) {
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          assertTrue(reader.moveTo(valueKey));
          assertEquals(Integer.toString(revision), reader.getValue());
          final var controller = session.getRtxIndexController(revision);
          assertEquals(2, controller.getIndexes().getIndexDefs().size());
          assertNotNull(controller.getIndexes().getIndexDef(name.getID(), name.getType()));
          final var lookup = controller.openCASIndex(reader.getStorageEngineReader(), cas,
              controller.createCASFilter(Set.of("/root/value"), new Str(Integer.toString(revision)), SearchMode.EQUAL,
                  new XmlPCRCollector(reader)));
          assertTrue(lookup.hasNext());
          final var references = lookup.next();
          assertEquals(1, references.getNodeKeys().getLongCardinality());
          assertTrue(references.contains(valueKey));
          assertFalse(lookup.hasNext());
        }
      }
    }
  }

  private static int retainedListeners(final ResourceSession<?, ?> session) throws ReflectiveOperationException {
    final Field cacheField = session.getClass().getDeclaredField("wtxIndexControllers");
    cacheField.setAccessible(true);
    final Map<?, ?> controllers = (Map<?, ?>) cacheField.get(session);
    final Field listenersField = AbstractIndexController.class.getDeclaredField("listenerSnapshot");
    listenersField.setAccessible(true);
    int count = 0;
    for (final Object controller : controllers.values()) {
      count += ((ChangeListener[]) listenersField.get(controller)).length;
    }
    return count;
  }
}
