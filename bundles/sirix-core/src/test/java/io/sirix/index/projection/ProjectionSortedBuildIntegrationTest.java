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
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedBuildIntegrationTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void loadTimeProjectionBuildsFilteredSortedViewAndColdReaderFindsIt() {
    final Path databasePath = temporaryDirectory.resolve("sorted-load");
    final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/op", PathParser.Type.JSON),
            parse("/[]/did", PathParser.Type.JSON), parse("/[]/time", PathParser.Type.JSON)),
        List.of(Type.STR, Type.STR, Type.STR, Type.LON), 0, IndexDef.DbType.JSON,
        new ProjectionSortedSpec(List.of(2, 3),
            List.of(new ProjectionSortedSpec.Equality(0, "commit"),
                new ProjectionSortedSpec.Equality(1, "create"))));
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
    }
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
      assertArrayEquals(expectedMissing(recordKeys[3]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("a", 30, recordKeys[2]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("z", 20, recordKeys[0]), cursor.copyKey());
      assertFalse(cursor.advance());
      assertEquals(1, directory.dataLeafCount());
      assertArrayEquals(expected("a", 30, recordKeys[2]),
          directory.seek(expected("a", 30, recordKeys[2])).copyKey());
      assertEquals(List.of(new ProjectionSortedGroupScan.Group(null, 5, 5),
              new ProjectionSortedGroupScan.Group("z", 20, 20)),
          ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 2,
              ProjectionSortedGroupScan.Order.MIN_ASC, 1));
      assertEquals(List.of(new ProjectionSortedGroupScan.Group("a", 30, 30)),
          ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 1,
              ProjectionSortedGroupScan.Order.MAX_DESC, 1));
      final String resourceKey = session.getResourceConfig().getResource().toString();
      assertEquals(List.of(new ProjectionSortedGroupScan.Group(null, 5, 5),
              new ProjectionSortedGroupScan.Group("z", 20, 20)),
          ProjectionIndexCatalog.sortedGroupTopK(session, resourceKey, reader.getRevisionNumber(),
              new String[] {"[]"}, Map.of("kind", "commit", "op", "create"), "did", "time", 2,
              ProjectionSortedGroupScan.Order.MIN_ASC, 1, true));
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
      assertArrayEquals(expected("z", 20, recordKeys[0]),
          oldDirectory.seek(expected("z", 20, recordKeys[0])).copyKey());
      assertArrayEquals(expected("b", 20, recordKeys[0]),
          newDirectory.seek(expected("b", 20, recordKeys[0])).copyKey());
      assertFalse(newDirectory.seek(expected("z", 20, recordKeys[0])).isValid());
      assertEquals(List.of(new ProjectionSortedGroupScan.Group(null, 5, 5),
              new ProjectionSortedGroupScan.Group("z", 20, 20)),
          ProjectionSortedGroupScan.topK(before.getStorageEngineReader(), 0, 2,
              ProjectionSortedGroupScan.Order.MIN_ASC, 1, true));
      final String resourceKey = session.getResourceConfig().getResource().toString();
      assertEquals(List.of(new ProjectionSortedGroupScan.Group(null, 5, 5),
              new ProjectionSortedGroupScan.Group("b", 20, 20)),
          ProjectionIndexCatalog.sortedGroupTopK(session, resourceKey, changedRevision,
              new String[] {"[]"}, Map.of("kind", "commit", "op", "create"), "did", "time", 2,
              ProjectionSortedGroupScan.Order.MIN_ASC, 1, true));
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
      assertArrayEquals(expected("b", 10, recordKeys[1]), cursor.copyKey());
      assertTrue(cursor.advance());
      assertArrayEquals(expected("b", 20, recordKeys[0]), cursor.copyKey());
      assertFalse(cursor.advance());
    }

    final long insertedKey;
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      assertTrue(writer.moveToFirstChild());
      insertedKey = writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"kind\":\"commit\",\"op\":\"create\",\"did\":\"c\",\"time\":40}"),
          JsonNodeTrx.Commit.NO).getNodeKey();
      writer.commit();
    }
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = reopened.beginResourceSession("resource");
        JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      assertArrayEquals(expected("c", 40, insertedKey),
          directory.seek(expected("c", 40, insertedKey)).copyKey());
      assertEquals(List.of(new ProjectionSortedGroupScan.Group("b", 10, 20)),
          ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 1,
              ProjectionSortedGroupScan.Order.SPAN_DESC, 1));
      assertEquals(List.of(new ProjectionSortedGroupScan.Group("c", 40, 40)),
          ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 1,
              ProjectionSortedGroupScan.Order.MAX_DESC, 1));
    }
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

  private static byte[] expected(final String did, final long time, final long recordKey) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    final byte[] utf8 = did.getBytes(StandardCharsets.UTF_8);
    writer.appendUtf8(utf8, 0, utf8.length);
    writer.appendLong(time);
    writer.appendRecordKey(recordKey);
    return writer.copyKey();
  }

  private static byte[] expectedMissing(final long recordKey) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    writer.appendMissing();
    writer.appendLong(5);
    writer.appendRecordKey(recordKey);
    return writer.copyKey();
  }
}
