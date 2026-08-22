/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionDefaultResourceLifecycleTest {

  private static final String RESOURCE = "default-projection-lifecycle";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0} maintains a default Dewey-disabled projection")
  @EnumSource(value = VersioningType.class,
      names = {"FULL", "DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void insertUpdateDeleteMoveAndColdReopenWorkWithoutStoredDeweyIds(
      final VersioningType versioningType) throws Exception {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        parse("/records/[]", PathParser.Type.JSON),
        List.of(parse("/records/[]/score", PathParser.Type.JSON)),
        List.of(Type.LON), 0, IndexDef.DbType.JSON);

    final int indexRevision;
    final int insertRevision;
    final int updateRevision;
    final int deleteRevision;
    final int moveRevision;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storageType(StorageType.FILE_CHANNEL)
          .hashKind(HashType.NONE)
          .storeDiffs(false)
          .buildPathSummary(true)
          .versioningApproach(versioningType)
          .maxNumberOfRevisionsToRestore(10)
          .build()));

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx();
           var parser = JacksonJsonShredder.createStringParser(
               "{\"records\":[{\"score\":1},{\"score\":2}]}")) {
        assertFalse(session.getResourceConfig().areDeweyIDsStored);
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD)
            .commitAfterwards()
            .build()
            .call();
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx()) {
        assertFalse(session.getResourceConfig().areDeweyIDsStored);
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
        wtx.commit();
      }
      indexRevision = mostRecentRevision(database);
      assertProjectionState(database, versioningType, definition, indexRevision, 1L, 2L);

      final long recordsKey;
      final long firstRecordKey;
      final long secondRecordKey;
      final long firstScoreKey;
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(indexRevision)) {
        recordsKey = namedArrayKey(rtx, "records");
        assertTrue(rtx.moveTo(recordsKey));
        assertTrue(rtx.moveToFirstChild());
        firstRecordKey = rtx.getNodeKey();
        assertTrue(rtx.moveToRightSibling());
        secondRecordKey = rtx.getNodeKey();
        firstScoreKey = namedNumberKey(rtx, "score", 0);
      }

      ProjectionIndexChangeListener.resetMaintenanceTelemetry();
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(recordsKey));
        wtx.insertObjectAsFirstChild();
        wtx.insertObjectRecordAsFirstChild("score", new NumberValue(0));
        wtx.commit();
      }
      insertRevision = mostRecentRevision(database);
      assertProjectionState(database, versioningType, definition, insertRevision, 0L, 1L, 2L);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(firstScoreKey));
        assertTrue(wtx.isNumberValue());
        wtx.setNumberValue(11L);
        wtx.commit();
      }
      updateRevision = mostRecentRevision(database);
      assertProjectionState(database, versioningType, definition, updateRevision, 0L, 11L, 2L);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(secondRecordKey));
        wtx.remove();
        wtx.commit();
      }
      deleteRevision = mostRecentRevision(database);
      assertProjectionState(database, versioningType, definition, deleteRevision, 0L, 11L);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(recordsKey));
        wtx.moveSubtreeToFirstChild(firstRecordKey);
        wtx.commit();
      }
      moveRevision = mostRecentRevision(database);
      assertProjectionState(database, versioningType, definition, moveRevision, 11L, 0L);
      assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds());
    }

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath)) {
      assertProjectionState(reopened, versioningType, definition, indexRevision, 1L, 2L);
      assertProjectionState(reopened, versioningType, definition, insertRevision, 0L, 1L, 2L);
      assertProjectionState(reopened, versioningType, definition, updateRevision, 0L, 11L, 2L);
      assertProjectionState(reopened, versioningType, definition, deleteRevision, 0L, 11L);
      assertProjectionState(reopened, versioningType, definition, moveRevision, 11L, 0L);
    }
    assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds());
  }

  private static int mostRecentRevision(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      return session.getMostRecentRevisionNumber();
    }
  }

  private static void assertProjectionState(final Database<JsonResourceSession> database,
      final VersioningType versioningType, final IndexDef definition, final int revision,
      final long... expectedValues) {
    ProjectionIndexCatalog.clearCache();
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
         JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertFalse(session.getResourceConfig().areDeweyIDsStored);
      assertEquals(versioningType, session.getResourceConfig().versioningType);
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, revision, definition);
      assertNotNull(handle);
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
          session, revision, definition.getID(), handle.rowGroupCount()));
      final List<Long> values = new ArrayList<>(expectedValues.length);
      for (final byte[] payload : leaves) {
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(payload);
        for (int row = 0; row < page.getRowCount(); row++) {
          values.add(page.numericColumn(0)[row]);
        }
      }
      assertEquals(Arrays.stream(expectedValues).boxed().toList(), values);
    }
  }

  private static long namedArrayKey(final JsonNodeReadOnlyTrx rtx, final String name) {
    final long restore = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY
            && name.equals(rtx.getName().getLocalName())) {
          return rtx.getNodeKey();
        }
        if (rtx.getKind() == NodeKind.ARRAY) {
          final long arrayKey = rtx.getNodeKey();
          final boolean named = rtx.moveToParent() && rtx.getKind().playsObjectKeyRole()
              && name.equals(rtx.getName().getLocalName());
          rtx.moveTo(arrayKey);
          if (named) {
            return arrayKey;
          }
        }
      }
      throw new AssertionError("missing JSON array " + name);
    } finally {
      rtx.moveTo(restore);
    }
  }

  private static long namedNumberKey(final JsonNodeReadOnlyTrx rtx, final String name,
      final int occurrence) {
    final long restore = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      int seen = 0;
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (!rtx.isNumberValue()) {
          continue;
        }
        final long numberKey = rtx.getNodeKey();
        final boolean named = name.equals(rtx.getName().getLocalName())
            || rtx.moveToParent() && rtx.getKind().playsObjectKeyRole()
                && name.equals(rtx.getName().getLocalName());
        rtx.moveTo(numberKey);
        if (named && seen++ == occurrence) {
          return numberKey;
        }
      }
      throw new AssertionError("missing JSON number " + name + " occurrence " + occurrence);
    } finally {
      rtx.moveTo(restore);
    }
  }
}
