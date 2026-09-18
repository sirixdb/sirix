/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedBuildIntegrationTest {

  private static final String[] RECORDS = {"[]"};

  @TempDir
  Path temporaryDirectory;

  @Test
  void loadTimeProjectionBuildsColumnOnlySortedViewAndServesEqualityPrefixRanges() {
    final Path databasePath = temporaryDirectory.resolve("sorted-load");
    final IndexDef definition = definition(Type.STR);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final long[] recordKeys = new long[4];
    final int initialRevision;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(definition, writer, 4L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","did":"z","time":20},
               {"kind":"identity","op":"create","did":"b","time":10},
               {"kind":"commit","op":"create","did":"a","time":30},
               {"kind":"commit","op":"create","time":5}]
              """), JsonNodeTrx.Commit.NO);
          initialRevision = writer.getRevisionNumber();
          writer.commit();
        }
        readRecordKeys(session, recordKeys);
      }
    }
    final byte[] commitCreate = prefix("commit", "create");
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
      assertArrayEquals(expected("commit", "create", null, 5, recordKeys[3]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("commit", "create", "a", 30, recordKeys[2]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("commit", "create", "z", 20, recordKeys[0]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("identity", "create", "b", 10, recordKeys[1]), cursor.copyKey());
      assertFalse(cursor.advance());
      assertEquals(1, directory.dataLeafCount());
      assertEquals(0, directory.unencodableRows());
      assertEquals(
          List.of(new ProjectionSortedGroupScan.Group(null, 5, 5), new ProjectionSortedGroupScan.Group("z", 20, 20)),
          topK(reader, commitCreate, 2, ProjectionSortedGroupScan.Order.MIN_ASC, false));
      assertEquals(List.of(new ProjectionSortedGroupScan.Group("a", 30, 30)),
          topK(reader, commitCreate, 1, ProjectionSortedGroupScan.Order.MAX_DESC, false));
      assertEquals(List.of(new ProjectionSortedGroupScan.Group("b", 10, 10)),
          topK(reader, prefix("identity", "create"), 1, ProjectionSortedGroupScan.Order.MIN_ASC, true));
      assertEquals(List.of(),
          topK(reader, prefix("commit", "delete"), 1, ProjectionSortedGroupScan.Order.MIN_ASC, true));
      assertEquals(
          List.of(new ProjectionSortedGroupScan.Group(null, 5, 5), new ProjectionSortedGroupScan.Group("z", 20, 20)),
          sortedTopK(session, reader.getRevisionNumber(), Map.of("kind", "commit", "op", "create"), 2));
    }

    final int changedRevision;
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      final long didKey = fieldKey(writer, recordKeys[0], "did");
      assertTrue(writer.moveTo(didKey));
      writer.setStringValue("x");
      final JsonIndexController controller =
          (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
      controller.notifyBeforePageFlush();
      assertTrue(writer.moveTo(didKey));
      writer.setStringValue("b");
      changedRevision = writer.getRevisionNumber();
      writer.commit();
    }
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeReadOnlyTrx before = session.beginNodeReadOnlyTrx(initialRevision);
        JsonNodeReadOnlyTrx after = session.beginNodeReadOnlyTrx(changedRevision)) {
      final ProjectionSortedDirectory.Accessor oldDirectory =
          ProjectionSortedDirectory.open(before.getStorageEngineReader(), 0);
      final ProjectionSortedDirectory.Accessor newDirectory =
          ProjectionSortedDirectory.open(after.getStorageEngineReader(), 0);
      assertNotNull(oldDirectory);
      assertNotNull(newDirectory);
      final byte[] oldKey = expected("commit", "create", "z", 20, recordKeys[0]);
      final byte[] newKey = expected("commit", "create", "b", 20, recordKeys[0]);
      assertArrayEquals(oldKey, oldDirectory.seek(oldKey).copyKey());
      assertArrayEquals(newKey, newDirectory.seek(newKey).copyKey());
      assertFalse(Arrays.equals(oldKey, newDirectory.seek(oldKey).copyKey()));
      assertEquals(
          List.of(new ProjectionSortedGroupScan.Group(null, 5, 5), new ProjectionSortedGroupScan.Group("z", 20, 20)),
          topK(before, commitCreate, 2, ProjectionSortedGroupScan.Order.MIN_ASC, true));
      assertEquals(
          List.of(new ProjectionSortedGroupScan.Group(null, 5, 5), new ProjectionSortedGroupScan.Group("b", 20, 20)),
          sortedTopK(session, changedRevision, Map.of("kind", "commit", "op", "create"), 2));
    }

    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      final long kindKey = fieldKey(writer, recordKeys[1], "kind");
      assertTrue(writer.moveTo(kindKey));
      writer.setStringValue("commit");
      final long operationKey = fieldKey(writer, recordKeys[2], "op");
      assertTrue(writer.moveTo(operationKey));
      writer.setStringValue("delete");
      assertTrue(writer.moveTo(recordKeys[3]));
      writer.remove();
      writer.commit();
    }
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
      assertArrayEquals(expected("commit", "create", "b", 10, recordKeys[1]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("commit", "create", "b", 20, recordKeys[0]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("commit", "delete", "a", 30, recordKeys[2]), cursor.copyKey());
      assertFalse(cursor.advance());
    }

    final long insertedKey;
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      assertTrue(writer.moveToFirstChild());
      insertedKey = writer
                          .insertSubtreeAsFirstChild(
                              JsonShredder.createStringReader(
                                  "{\"kind\":\"commit\",\"op\":\"create\",\"did\":\"c\",\"time\":40}"),
                              JsonNodeTrx.Commit.NO)
                          .getNodeKey();
      writer.commit();
    }
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      final byte[] inserted = expected("commit", "create", "c", 40, insertedKey);
      assertArrayEquals(inserted, directory.seek(inserted).copyKey());
      assertEquals(List.of(new ProjectionSortedGroupScan.Group("b", 10, 20)),
          topK(reader, commitCreate, 1, ProjectionSortedGroupScan.Order.SPAN_DESC, false));
      assertEquals(List.of(new ProjectionSortedGroupScan.Group("c", 40, 40)),
          topK(reader, commitCreate, 1, ProjectionSortedGroupScan.Order.MAX_DESC, false));
    }
  }

  @Test
  void revertToDerivesPriorKeysFromTheRepresentedRevision() {
    final Path databasePath = temporaryDirectory.resolve("sorted-revert");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int first;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(definition(Type.STR), writer, 2L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","did":"a","time":100},
               {"kind":"commit","op":"create","did":"b","time":200}]
              """), JsonNodeTrx.Commit.NO);
          first = writer.getRevisionNumber();
          writer.commit();
        }
        final long[] recordKeys = new long[2];
        readRecordKeys(session, recordKeys);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setTime(writer, recordKeys[0], 50);
          writer.commit();
        }
        final int reverted;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.revertTo(first);
          setTime(writer, recordKeys[0], 50);
          reverted = writer.getRevisionNumber();
          writer.commit();
        }
        assertEquals(
            List.of(new ProjectionSortedGroupScan.Group("a", 50, 50),
                new ProjectionSortedGroupScan.Group("b", 200, 200)),
            sortedTopK(session, reverted, Map.of("kind", "commit", "op", "create"), 2));
        final int revertedAgain;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.revertTo(first);
          setTime(writer, recordKeys[0], 70);
          revertedAgain = writer.getRevisionNumber();
          writer.commit();
        }
        assertEquals(
            List.of(new ProjectionSortedGroupScan.Group("a", 70, 70),
                new ProjectionSortedGroupScan.Group("b", 200, 200)),
            sortedTopK(session, revertedAgain, Map.of("kind", "commit", "op", "create"), 2));
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revertedAgain)) {
          final ProjectionSortedDirectory.Accessor.Cursor cursor =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0).first();
          assertArrayEquals(expected("commit", "create", "a", 70, recordKeys[0]), cursor.copyKey());
          assertTrue(cursor.advance());
          assertArrayEquals(expected("commit", "create", "b", 200, recordKeys[1]), cursor.copyKey());
          assertFalse(cursor.advance());
        }
      }
    }
  }

  @Test
  void viewBuiltInsideAnEditedTransactionFindsItsBuildTimeRows() {
    final Path databasePath = temporaryDirectory.resolve("sorted-mid-transaction");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","did":"a","time":100},
               {"kind":"commit","op":"create","did":"b","time":200}]
              """), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final long[] recordKeys = new long[2];
        readRecordKeys(session, recordKeys);
        final int revision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setTime(writer, recordKeys[0], 50);
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createIndexes(Set.of(definition(Type.STR)), writer);
          setTime(writer, recordKeys[0], 60);
          revision = writer.getRevisionNumber();
          writer.commit();
        }
        assertEquals(
            List.of(new ProjectionSortedGroupScan.Group("a", 60, 60),
                new ProjectionSortedGroupScan.Group("b", 200, 200)),
            sortedTopK(session, revision, Map.of("kind", "commit", "op", "create"), 2));
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          final ProjectionSortedDirectory.Accessor.Cursor cursor =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0).first();
          assertArrayEquals(expected("commit", "create", "a", 60, recordKeys[0]), cursor.copyKey());
          assertTrue(cursor.advance());
          assertArrayEquals(expected("commit", "create", "b", 200, recordKeys[1]), cursor.copyKey());
          assertFalse(cursor.advance());
        }
      }
    }
  }

  @Test
  void unencodableSortKeyIsStoredAndDeclinesTheViewUntilItIsGone() {
    final Path databasePath = temporaryDirectory.resolve("sorted-unencodable");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(definition(Type.STR), writer, 3L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","did":"a","time":100},
               {"kind":"commit","op":"create","did":5,"time":150},
               {"kind":"commit","op":"create","did":"b","time":200}]
              """), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final long[] recordKeys = new long[3];
        readRecordKeys(session, recordKeys);
        final Map<String, String> filter = Map.of("kind", "commit", "op", "create");
        assertEquals(1, unencodableRows(session, session.getMostRecentRevisionNumber()));
        assertNull(sortedTopK(session, session.getMostRecentRevisionNumber(), filter, 2));
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setTime(writer, recordKeys[2], 1.5);
          writer.commit();
        }
        assertEquals(2, unencodableRows(session, session.getMostRecentRevisionNumber()));
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(recordKeys[1]));
          writer.remove();
          setTime(writer, recordKeys[2], 200);
          writer.commit();
        }
        final int repaired = session.getMostRecentRevisionNumber();
        assertEquals(0, unencodableRows(session, repaired));
        assertEquals(List.of(new ProjectionSortedGroupScan.Group("a", 100, 100),
            new ProjectionSortedGroupScan.Group("b", 200, 200)), sortedTopK(session, repaired, filter, 2));
      }
    }
  }

  @Test
  void equalityOnANonStringSortColumnDeclinesTheSortedRoute() {
    final Path databasePath = temporaryDirectory.resolve("sorted-long-prefix");
    final IndexDef longPrefix = definition(Type.LON);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(longPrefix, writer, 2L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":5,"op":"create","did":"a","time":100},
               {"kind":6,"op":"create","did":"b","time":200}]
              """), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        assertEquals(0, unencodableRows(session, revision));
        assertNull(sortedTopK(session, revision, Map.of("kind", "5", "op", "create"), 2));
      }
    }
  }

  @Test
  void oneCommitEditingManyRowsOfOneLeafRewritesThatLeafOnce() {
    final Path databasePath = temporaryDirectory.resolve("sorted-batched");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < 12; i++) {
          rows.append(i == 0
              ? ""
              : ",")
              .append("{\"kind\":\"commit\",\"op\":\"create\",\"did\":\"d")
              .append(i)
              .append("\",\"time\":")
              .append(1000 + i)
              .append('}');
        }
        rows.append(']');
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(definition(Type.STR), writer, 12L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(rows.toString()), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final long[] recordKeys = new long[12];
        readRecordKeys(session, recordKeys);
        final Int2IntOpenHashMap writesByLeaf = new Int2IntOpenHashMap();
        ProjectionSortedLeafStore.setWriteObserverForTesting(leafId -> writesByLeaf.addTo(leafId, 1));
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          for (int i = 0; i < recordKeys.length; i += 2) {
            setTime(writer, recordKeys[i], 10 + i);
          }
          writer.commit();
        } finally {
          ProjectionSortedLeafStore.setWriteObserverForTesting(null);
        }
        assertEquals(1, writesByLeaf.size());
        assertEquals(1, writesByLeaf.get(1));
        assertEquals(
            List.of(new ProjectionSortedGroupScan.Group("d0", 10, 10),
                new ProjectionSortedGroupScan.Group("d2", 12, 12)),
            sortedTopK(session, session.getMostRecentRevisionNumber(), Map.of("kind", "commit", "op", "create"), 2));
      }
    }
  }

  @Test
  void aQueryInsideTheWriteTransactionKeepsTheViewExactThroughLaterEditsOfTheSameRecords() {
    final Path databasePath = temporaryDirectory.resolve("sorted-wtx-query");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        loadThreeRecords(session);
        final long[] recordKeys = new long[3];
        readRecordKeys(session, recordKeys);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setDid(writer, recordKeys[0], "z");
          setTime(writer, recordKeys[1], 50);
          final long inserted = insertFirstRecord(writer, "d", 400);
          queryInsideTransaction(session, writer);
          setDid(writer, recordKeys[0], "a");
          setTime(writer, recordKeys[1], 200);
          setTime(writer, inserted, 10);
          queryInsideTransaction(session, writer);
          setTime(writer, recordKeys[2], 5);
          writer.commit();
        }
        assertSortedRouteMatchesTheData(session, session.getMostRecentRevisionNumber());
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setDid(writer, recordKeys[0], "y");
          setTime(writer, recordKeys[1], 1);
          writer.commit();
        }
        assertSortedRouteMatchesTheData(session, session.getMostRecentRevisionNumber());
      }
    }
  }

  @Test
  void aCommitThatFailsAfterMaintenanceKeepsTheViewExactWhenRetriedAfterMoreEdits() {
    final Path databasePath = temporaryDirectory.resolve("sorted-failed-commit");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        loadThreeRecords(session);
        final long[] recordKeys = new long[3];
        readRecordKeys(session, recordKeys);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final AtomicBoolean failNextCommit = new AtomicBoolean(true);
          // Pre-commit hooks run ahead of the commit's own maintenance, so this hook applies the
          // maintenance itself and then fails: the commit fails after maintenance has run.
          writer.addPreCommitHook(trx -> {
            if (failNextCommit.getAndSet(false)) {
              session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance(true);
              throw new IllegalStateException("injected failure after index maintenance");
            }
          });
          setDid(writer, recordKeys[0], "z");
          setTime(writer, recordKeys[1], 50);
          final IllegalStateException failure = assertThrows(IllegalStateException.class, writer::commit);
          assertEquals("injected failure after index maintenance", failure.getMessage());
          setDid(writer, recordKeys[0], "a");
          setTime(writer, recordKeys[1], 200);
          setTime(writer, recordKeys[2], 5);
          writer.commit();
        }
        assertSortedRouteMatchesTheData(session, session.getMostRecentRevisionNumber());
      }
    }
  }

  @Test
  void aCommitTouchingManyRecordsOfOneLeafReadsThatLeafOncePerStep() {
    final Path databasePath = temporaryDirectory.resolve("sorted-leaf-reads");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < 12; i++) {
          rows.append(i == 0
              ? ""
              : ",")
              .append("{\"kind\":\"commit\",\"op\":\"create\",\"did\":\"d")
              .append(i)
              .append("\",\"time\":")
              .append(1000 + i)
              .append('}');
        }
        rows.append(']');
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(definition(Type.STR), writer, 12L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(rows.toString()), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final long[] recordKeys = new long[12];
        readRecordKeys(session, recordKeys);

        // The view at the represented revision holds exactly the derived keys: the only read of the
        // leaf is the rewrite's.
        assertEquals(1, leafReadsOfCommit(session, writer -> {
          for (int i = 0; i < recordKeys.length; i += 2) {
            setTime(writer, recordKeys[i], 10 + i);
          }
        }));
        assertSortedRouteMatchesTheData(session, session.getMostRecentRevisionNumber());

        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setTime(writer, recordKeys[11], 1.5);
          writer.commit();
        }
        assertEquals(1, unencodableRows(session, session.getMostRecentRevisionNumber()));
        // A reserved row makes the derived keys unproven: they are checked in one key-ordered pass,
        // so the leaf is read once for all of them and once more for the rewrite.
        assertEquals(2, leafReadsOfCommit(session, writer -> {
          for (int i = 1; i < 11; i += 2) {
            setTime(writer, recordKeys[i], 20 + i);
          }
        }));
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setTime(writer, recordKeys[11], 1011);
          writer.commit();
        }
        assertSortedRouteMatchesTheData(session, session.getMostRecentRevisionNumber());
      }
    }
  }

  @Test
  void droppingAnotherIndexInsideTheTransactionKeepsTheViewExact() {
    final Path databasePath = temporaryDirectory.resolve("sorted-drop-other");
    final IndexDef other = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/time", PathParser.Type.JSON)),
        List.of(Type.STR, Type.LON), 1, IndexDef.DbType.JSON);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        loadThreeRecords(session);
        final long[] recordKeys = new long[3];
        readRecordKeys(session, recordKeys);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(other), writer);
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setDid(writer, recordKeys[0], "z");
          queryInsideTransaction(session, writer);
          setTime(writer, recordKeys[1], 50);
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.dropIndexes(Set.of(controller.getIndexes().getIndexDef(1, IndexType.PROJECTION)), writer);
          setDid(writer, recordKeys[0], "a");
          writer.commit();
        }
        assertSortedRouteMatchesTheData(session, session.getMostRecentRevisionNumber());
      }
    }
  }

  /** Sorted data-leaf reads through the transaction's storage while {@code edits} commit. */
  private static int leafReadsOfCommit(final JsonResourceSession session, final Consumer<JsonNodeTrx> edits) {
    final int[] reads = new int[1];
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      edits.accept(writer);
      ProjectionSortedLeafStore.setStorageReadObserverForTesting(leafId -> reads[0]++);
      try {
        writer.commit();
      } finally {
        ProjectionSortedLeafStore.setStorageReadObserverForTesting(null);
      }
    }
    return reads[0];
  }

  @Test
  void maintenanceUnderChangedColumnKindsDeclinesTheViewInsteadOfMixingEncodings() {
    final Path databasePath = temporaryDirectory.resolve("sorted-changed-kinds");
    // kind, op, at, did, time — ordered by all five; "at" is a declared timestamp.
    final IndexDef timed = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/op", PathParser.Type.JSON),
            parse("/[]/at", PathParser.Type.JSON), parse("/[]/did", PathParser.Type.JSON),
            parse("/[]/time", PathParser.Type.JSON)),
        List.of(Type.STR, Type.STR, Type.DATI, Type.STR, Type.LON), 0, IndexDef.DbType.JSON,
        new ProjectionSortedSpec(List.of(0, 1, 2, 3, 4)));
    final byte[] stringLayout = {ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_STRING,
        ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_LONG};
    final byte[] prefix = prefix("commit", "create", "2024-01-01T00:00:00");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final boolean previous = ProjectionTemporalCodec.setTemporalKindsEnabledForTesting(false);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(timed, writer, 3L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","at":"2024-01-01T00:00:00","did":"a","time":100},
               {"kind":"commit","op":"create","at":"2024-01-01T00:00:00","did":"b","time":200},
               {"kind":"commit","op":"create","at":"2024-01-01T00:00:00","did":"c","time":300}]
              """), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final long[] recordKeys = new long[3];
        readRecordKeys(session, recordKeys);
        assertEquals(List.of(new ProjectionSortedGroupScan.Group("a", 100, 100)),
            topKAt(session, session.getMostRecentRevisionNumber(), prefix));

        ProjectionTemporalCodec.setTemporalKindsEnabledForTesting(true);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setTime(writer, recordKeys[0], 50);
          setDid(writer, recordKeys[1], "e");
          writer.commit();
        }
        final int changed = session.getMostRecentRevisionNumber();
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(changed)) {
          final ProjectionSortedDirectory.Accessor directory =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
          assertNotNull(directory);
          assertArrayEquals(stringLayout, directory.layout().toBytes());
          assertEquals(2, directory.unencodableRows());
          final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
          int rows = 0;
          while (cursor.isValid()) {
            final byte[] key = cursor.copyKey();
            assertTrue(ProjectionSortKeyCodec.isUnencodable(key, key.length)
                || directory.layout().lastFieldOffset(key, key.length) > 0, "row " + rows);
            rows++;
            cursor.advance();
          }
          assertEquals(3, rows);
        }
        assertNull(topKAt(session, changed, prefix));

        ProjectionTemporalCodec.setTemporalKindsEnabledForTesting(false);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          setTime(writer, recordKeys[0], 60);
          setTime(writer, recordKeys[1], 70);
          writer.commit();
        }
        final int repaired = session.getMostRecentRevisionNumber();
        assertEquals(0, unencodableRows(session, repaired));
        assertEquals(List.of(new ProjectionSortedGroupScan.Group("a", 60, 60)), topKAt(session, repaired, prefix));
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(repaired)) {
          final ProjectionSortedDirectory.Accessor.Cursor cursor =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0).first();
          assertArrayEquals(timedKey("a", 60, recordKeys[0]), cursor.copyKey());
          assertTrue(cursor.advance());
          assertArrayEquals(timedKey("c", 300, recordKeys[2]), cursor.copyKey());
          assertTrue(cursor.advance());
          assertArrayEquals(timedKey("e", 70, recordKeys[1]), cursor.copyKey());
          assertFalse(cursor.advance());
        }
      }
    } finally {
      ProjectionTemporalCodec.setTemporalKindsEnabledForTesting(previous);
    }
  }

  @Test
  void anOverlongCompositeSortKeyNeverFailsTheLoadOrACommit() {
    final Path databasePath = temporaryDirectory.resolve("sorted-overlong-key");
    final int columns = 17;
    final IndexDef wide = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        IntStream.range(0, columns).mapToObj(column -> parse("/[]/c" + column, PathParser.Type.JSON)).toList(),
        Collections.nCopies(columns, Type.STR), 0, IndexDef.DbType.JSON,
        new ProjectionSortedSpec(IntStream.range(0, columns).boxed().toList()));
    // Every field fits a key on its own; together they exceed even a run's two-byte key length.
    final String longValue = "v".repeat(4000);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(wide, writer, 3L);
          writer.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(
                  "[" + wideRecord(columns, "s") + "," + wideRecord(columns, longValue) + ","
                      + wideRecord(columns, "t") + "]"),
              JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final long[] recordKeys = new long[3];
        readRecordKeys(session, recordKeys);
        assertEquals(1, unencodableRows(session, session.getMostRecentRevisionNumber()));
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          for (int column = 0; column < columns; column++) {
            assertTrue(writer.moveTo(fieldKey(writer, recordKeys[2], "c" + column)));
            writer.setStringValue(longValue);
          }
          writer.commit();
        }
        assertEquals(2, unencodableRows(session, session.getMostRecentRevisionNumber()));
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          for (int column = 0; column < columns; column++) {
            assertTrue(writer.moveTo(fieldKey(writer, recordKeys[1], "c" + column)));
            writer.setStringValue("u");
            assertTrue(writer.moveTo(fieldKey(writer, recordKeys[2], "c" + column)));
            writer.setStringValue("r");
          }
          writer.commit();
        }
        final int repaired = session.getMostRecentRevisionNumber();
        assertEquals(0, unencodableRows(session, repaired));
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(repaired)) {
          final ProjectionSortedDirectory.Accessor.Cursor cursor =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0).first();
          assertArrayEquals(wideKey(columns, "r", recordKeys[2]), cursor.copyKey());
          assertTrue(cursor.advance());
          assertArrayEquals(wideKey(columns, "s", recordKeys[0]), cursor.copyKey());
          assertTrue(cursor.advance());
          assertArrayEquals(wideKey(columns, "u", recordKeys[1]), cursor.copyKey());
          assertFalse(cursor.advance());
        }
      }
    }
  }

  private static String wideRecord(final int columns, final String value) {
    final StringBuilder record = new StringBuilder(columns * (value.length() + 8)).append('{');
    for (int column = 0; column < columns; column++) {
      if (column > 0) {
        record.append(',');
      }
      record.append("\"c").append(column).append("\":\"").append(value).append('"');
    }
    return record.append('}').toString();
  }

  private static byte[] wideKey(final int columns, final String value, final long recordKey) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    for (int column = 0; column < columns; column++) {
      appendString(writer, value);
    }
    writer.appendRecordKey(recordKey);
    return writer.copyKey();
  }

  private static byte[] timedKey(final String did, final long time, final long recordKey) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    appendString(writer, "commit");
    appendString(writer, "create");
    appendString(writer, "2024-01-01T00:00:00");
    appendString(writer, did);
    writer.appendLong(time);
    writer.appendRecordKey(recordKey);
    return writer.copyKey();
  }

  private static @Nullable List<ProjectionSortedGroupScan.Group> topKAt(final JsonResourceSession session,
      final int revision, final byte[] prefix) {
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      return topK(reader, prefix, 1, ProjectionSortedGroupScan.Order.MIN_ASC, true);
    }
  }

  private static void loadThreeRecords(final JsonResourceSession session) {
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      final JsonIndexController controller =
          (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
      controller.createProjectionIndexAtLoadStart(definition(Type.STR), writer, 3L);
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
          [{"kind":"commit","op":"create","did":"a","time":100},
           {"kind":"commit","op":"create","did":"b","time":200},
           {"kind":"commit","op":"create","did":"c","time":300}]
          """), JsonNodeTrx.Commit.NO);
      writer.commit();
    }
  }

  private static long insertFirstRecord(final JsonNodeTrx writer, final String did, final long time) {
    writer.moveToDocumentRoot();
    assertTrue(writer.moveToFirstChild());
    return writer
                 .insertSubtreeAsFirstChild(JsonShredder.createStringReader(
                     "{\"kind\":\"commit\",\"op\":\"create\",\"did\":\"" + did + "\",\"time\":" + time + "}"),
                     JsonNodeTrx.Commit.NO)
                 .getNodeKey();
  }

  /** What a projection query served inside the write transaction does first. */
  private static void queryInsideTransaction(final JsonResourceSession session, final JsonNodeTrx writer) {
    final JsonIndexController controller =
        (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
    assertNotNull(controller.openProjectionIndex(writer.getStorageEngineWriter(), RECORDS,
        new String[] {"kind", "op", "did", "time"}));
  }

  /**
   * The view holds exactly the keys of the revision's records, and the sorted route answers what a
   * generic evaluation over those records answers.
   */
  private static void assertSortedRouteMatchesTheData(final JsonResourceSession session, final int revision) {
    final List<Row> rows = rows(session, revision);
    final ArrayList<byte[]> expectedKeys = new ArrayList<>(rows.size());
    final HashMap<String, Long> firstTimeByDid = new HashMap<>();
    for (final Row row : rows) {
      expectedKeys.add(expected(row.kind(), row.op(), row.did(), row.time(), row.recordKey()));
      if ("commit".equals(row.kind()) && "create".equals(row.op())) {
        firstTimeByDid.merge(row.did(), row.time(), Math::min);
      }
    }
    expectedKeys.sort(Arrays::compareUnsigned);
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      assertEquals(0, directory.unencodableRows());
      final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
      for (final byte[] key : expectedKeys) {
        assertTrue(cursor.isValid());
        assertArrayEquals(key, cursor.copyKey());
        cursor.advance();
      }
      assertFalse(cursor.isValid());
    }
    final ArrayList<Map.Entry<String, Long>> groups = new ArrayList<>(firstTimeByDid.entrySet());
    groups.sort(Map.Entry.<String, Long>comparingByValue().thenComparing(Map.Entry.comparingByKey()));
    final ArrayList<ProjectionSortedGroupScan.Group> generic = new ArrayList<>(2);
    for (int i = 0; i < Math.min(2, groups.size()); i++) {
      final Map.Entry<String, Long> group = groups.get(i);
      generic.add(new ProjectionSortedGroupScan.Group(group.getKey(), group.getValue(), group.getValue()));
    }
    assertEquals(generic, sortedTopK(session, revision, Map.of("kind", "commit", "op", "create"), 2));
  }

  private record Row(long recordKey, String kind, String op, String did, long time) {
  }

  /** Every record of the revision, read field by field through the document itself. */
  private static List<Row> rows(final JsonResourceSession session, final int revision) {
    final ArrayList<Row> rows = new ArrayList<>();
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      assertTrue(reader.moveToDocumentRoot());
      assertTrue(reader.moveToFirstChild());
      if (!reader.moveToFirstChild()) {
        return rows;
      }
      do {
        final long recordKey = reader.getNodeKey();
        String kind = null;
        String op = null;
        String did = null;
        long time = Long.MIN_VALUE;
        assertTrue(reader.moveToFirstChild());
        do {
          switch (reader.getName().getLocalName()) {
            case "kind" -> kind = reader.getValue();
            case "op" -> op = reader.getValue();
            case "did" -> did = reader.getValue();
            case "time" -> time = reader.getNumberValue().longValue();
            default -> throw new AssertionError("unexpected field " + reader.getName());
          }
        } while (reader.moveToRightSibling());
        assertTrue(reader.moveTo(recordKey));
        rows.add(new Row(recordKey, kind, op, did, time));
      } while (reader.moveToRightSibling());
    }
    return rows;
  }

  private static void setDid(final JsonNodeTrx writer, final long recordKey, final String did) {
    assertTrue(writer.moveTo(fieldKey(writer, recordKey, "did")));
    writer.setStringValue(did);
  }

  /** Fields kind, op, did, time; the view is ordered by all four, kind typed as given. */
  private static IndexDef definition(final Type kindType) {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/op", PathParser.Type.JSON),
            parse("/[]/did", PathParser.Type.JSON), parse("/[]/time", PathParser.Type.JSON)),
        List.of(kindType, Type.STR, Type.STR, Type.LON), 0, IndexDef.DbType.JSON,
        new ProjectionSortedSpec(List.of(0, 1, 2, 3)));
  }

  private static @Nullable List<ProjectionSortedGroupScan.Group> sortedTopK(final JsonResourceSession session,
      final int revision, final Map<String, String> filter, final int limit) {
    return ProjectionIndexCatalog.sortedGroupTopK(session, session.getResourceConfig().getResource().toString(),
        revision, RECORDS, filter, "did", "time", limit, ProjectionSortedGroupScan.Order.MIN_ASC, 1, true);
  }

  private static @Nullable List<ProjectionSortedGroupScan.Group> topK(final JsonNodeReadOnlyTrx reader,
      final byte[] prefix, final int limit, final ProjectionSortedGroupScan.Order order, final boolean minOnly) {
    return ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, prefix, limit, order, 1, minOnly, null);
  }

  private static long unencodableRows(final JsonResourceSession session, final int revision) {
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      return directory.unencodableRows();
    }
  }

  private static void readRecordKeys(final JsonResourceSession session, final long[] recordKeys) {
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertTrue(reader.moveToDocumentRoot());
      assertTrue(reader.moveToFirstChild());
      assertTrue(reader.moveToFirstChild());
      for (int i = 0; i < recordKeys.length; i++) {
        recordKeys[i] = reader.getNodeKey();
        if (i + 1 < recordKeys.length) {
          assertTrue(reader.moveToRightSibling());
        }
      }
    }
  }

  private static void setTime(final JsonNodeTrx writer, final long recordKey, final Number time) {
    assertTrue(writer.moveTo(fieldKey(writer, recordKey, "time")));
    writer.setNumberValue(time);
  }

  private static long fieldKey(final JsonNodeTrx writer, final long recordKey, final String field) {
    assertTrue(writer.moveTo(recordKey));
    assertTrue(writer.moveToFirstChild());
    do {
      if (field.equals(writer.getName().getLocalName())) {
        return writer.getNodeKey();
      }
    } while (writer.moveToRightSibling());
    throw new AssertionError("record " + recordKey + " has no field " + field);
  }

  private static byte[] prefix(final String... values) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    for (final String value : values) {
      appendString(writer, value);
    }
    return writer.copyKey();
  }

  private static byte[] expected(final String kind, final String op, final @Nullable String did, final long time,
      final long recordKey) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    appendString(writer, kind);
    appendString(writer, op);
    if (did == null) {
      writer.appendMissing();
    } else {
      appendString(writer, did);
    }
    writer.appendLong(time);
    writer.appendRecordKey(recordKey);
    return writer.copyKey();
  }

  private static void appendString(final ProjectionSortKeyCodec.Writer writer, final String value) {
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    writer.appendUtf8(utf8, 0, utf8.length);
  }
}
