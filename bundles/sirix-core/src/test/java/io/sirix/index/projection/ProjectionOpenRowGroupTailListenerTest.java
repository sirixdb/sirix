/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

/**
 * Open-row-group row tail through the real maintenance path: single-record appends to the open row
 * group take the tail, a field update or a delete folds it, and every revision reads the rows it
 * committed.
 */
@Isolated
final class ProjectionOpenRowGroupTailListenerTest {
  private static final int INDEX = 0;

  @TempDir
  Path temporaryDirectory;

  private static IndexDef definition() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/did", PathParser.Type.JSON),
            parse("/[]/time", PathParser.Type.JSON)),
        List.of(Type.STR, Type.STR, Type.LON), INDEX, IndexDef.DbType.JSON);
  }

  private static void appendRecord(final JsonResourceSession session, final String did, final long time) {
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      assertTrue(writer.moveToDocumentRoot());
      assertTrue(writer.moveToFirstChild());
      writer.insertSubtreeAsLastChild(
          JsonShredder.createStringReader("{\"kind\":\"commit\",\"did\":\"" + did + "\",\"time\":" + time + "}"),
          JsonNodeTrx.Commit.NO);
      writer.commit();
    }
  }

  private static long[] recordKeys(final JsonResourceSession session, final int count) {
    final long[] keys = new long[count];
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertTrue(reader.moveToDocumentRoot());
      assertTrue(reader.moveToFirstChild());
      assertTrue(reader.moveToFirstChild());
      for (int i = 0; i < count; i++) {
        keys[i] = reader.getNodeKey();
        if (i + 1 < count) {
          assertTrue(reader.moveToRightSibling());
        }
      }
    }
    return keys;
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

  /** The open row group as revision {@code revision} sees it, with the descriptor's tail state. */
  @SuppressWarnings("ArrayRecordComponent") // Tests compare each cell explicitly rather than using record equality.
  private record Seen(ProjectionIndexRowGroupPage page, boolean tailed, int tailBlobs, String[] didValues) {
  }

  private static Seen seen(final JsonResourceSession session, final int revision) {
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader storage = reader.getStorageEngineReader();
      final byte[] raw = ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(storage, INDEX, 1);
      assertNotNull(raw, "row group 1 at revision " + revision);
      final byte[] descriptor =
          ProjectionIndexHOTStorage.readBlob(storage, INDEX, ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
      assertNotNull(descriptor);
      final boolean tailed = RowGroupDescriptor.isTailed(descriptor);
      final byte[] header =
          ProjectionIndexHOTStorage.readBlob(storage, INDEX, ProjectionOpenRowGroupTail.headerSlot(1));
      if (!tailed) {
        assertNull(header, "an untailed row group has no tail header at revision " + revision);
      }
      final int blobs = header == null
          ? 0
          : ProjectionOpenRowGroupTail.Header.decode(header, 1).blobCount();
      final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(storage, INDEX, 1);
      assertEquals(1, all.size());
      assertArrayEquals(raw, all.get(0), "batch assembly agrees with the single read");
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(raw);
      final String[] didValues = new String[page.getRowCount()];
      if (page.columnKind(1) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        final ProjectionIndexMetadata metadata =
            ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(storage, INDEX, 0));
        assertNotNull(metadata);
        final long[] dictionaryHeaderKeys = metadata.valueDictionaryHeaderKeys();
        assertNotNull(dictionaryHeaderKeys);
        final GlobalValueDictionary.ReadView dictionary =
            GlobalValueDictionary.readView(dictionaryHeaderKeys[1], storage);
        assertNotNull(dictionary);
        for (int row = 0; row < page.getRowCount(); row++) {
          didValues[row] = dictionary.valueOfCell(page.numericColumn(1)[row]);
          assertNotNull(didValues[row], "every tail string id resolves");
        }
      } else {
        for (int row = 0; row < page.getRowCount(); row++) {
          didValues[row] = didAt(page, row);
        }
      }
      return new Seen(page, tailed, blobs, didValues);
    }
  }

  private static long timeAt(final ProjectionIndexRowGroupPage page, final int row) {
    return page.numericColumn(2)[row];
  }

  private static String didAt(final ProjectionIndexRowGroupPage page, final int row) {
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, page.columnKind(1));
    final int id = page.stringDictIdColumn(1)[row];
    return new String(page.stringDictionaryEntryBacking(1, id), page.stringDictionaryEntryOffset(1, id),
        page.stringDictionaryEntryLength(1, id), StandardCharsets.UTF_8);
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void middleInsertionThenTailAppendPreservesOrderExceptions(final VersioningType versioningType) {
    final Path databasePath =
        temporaryDirectory.resolve("order-bitmap-" + versioningType.name().toLowerCase(Locale.ROOT));
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final StringBuilder input = new StringBuilder(4096).append('[');
        for (int row = 0; row < 63; row++) {
          if (row > 0) {
            input.append(',');
          }
          input.append("{\"kind\":\"commit\",\"did\":\"d").append(row).append("\",\"time\":").append(row).append('}');
        }
        input.append(']');
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(definition(), writer, 63L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(input.toString()), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final long[] loadedKeys = recordKeys(session, 63);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(loadedKeys[30]));
          writer.insertSubtreeAsRightSibling(
              JsonShredder.createStringReader("{\"kind\":\"commit\",\"did\":\"middle\",\"time\":1000}"),
              JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        final Seen inserted = seen(session, 2);
        assertMiddleInsertionRows(inserted, 64);
        assertFalse(inserted.tailed());
        assertEquals(1, inserted.page().orderExceptionBits().length);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveToDocumentRoot());
          assertTrue(writer.moveToFirstChild());
          writer.insertSubtreeAsLastChild(
              JsonShredder.createStringReader("{\"kind\":\"commit\",\"did\":\"rollback\",\"time\":9999}"),
              JsonNodeTrx.Commit.NO);
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.applyPendingIndexMaintenance(true);
          final byte[] raw =
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX).getRowGroupFromColumnSegmentSlots(
                  1);
          assertNotNull(raw);
          final ProjectionIndexRowGroupPage pending = ProjectionIndexRowGroupPage.deserialize(raw);
          assertEquals(65, pending.getRowCount());
          assertTrue(pending.orderExceptionAt(31));
          assertFalse(pending.orderExceptionAt(64));
          assertEquals(9999L, timeAt(pending, 64));
          writer.rollback();
        }
        assertMiddleInsertionRows(seen(session, 2), 64);
        appendRecord(session, "tail", 2000L);
        final Seen appended = seen(session, 3);
        assertMiddleInsertionRows(appended, 65);
        assertTrue(appended.tailed());
        assertArrayEquals(recordKeys(session, 65), Arrays.copyOf(appended.page().recordKeys(), 65));
        ProjectionOpenRowGroupTail.clearCacheForTesting();
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
          assertArrayEquals(appended.page().serialize(), storage.getRowGroupFromColumnSegmentSlots(1));
          writer.rollback();
        }
      }
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 3; revision++) {
        ProjectionOpenRowGroupTail.clearCacheForTesting();
        final Seen state = seen(session, revision);
        assertMiddleInsertionRows(state, 62 + revision);
        assertEquals(revision == 3, state.tailed());
      }
    }
  }

  private static void assertMiddleInsertionRows(final Seen state, final int rows) {
    assertEquals(rows, state.page().getRowCount());
    for (int row = 0; row < rows; row++) {
      if (rows >= 64 && row == 31) {
        assertEquals("middle", state.didValues()[row]);
        assertEquals(1000L, timeAt(state.page(), row));
        assertTrue(state.page().orderExceptionAt(row));
      } else if (row == 64) {
        assertEquals("tail", state.didValues()[row]);
        assertEquals(2000L, timeAt(state.page(), row));
        assertFalse(state.page().orderExceptionAt(row));
      } else {
        final int originalRow = rows >= 64 && row > 31
            ? row - 1
            : row;
        assertEquals("d" + originalRow, state.didValues()[row]);
        assertEquals(originalRow, timeAt(state.page(), row));
        assertFalse(state.page().orderExceptionAt(row));
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void globalDictionaryAppendsPreserveResolvedIds(final VersioningType versioningType) {
    final String prior = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "always");
    try {
      singleRecordAppendsTakeTheTailAndEveryOtherEditFoldsIt(versioningType);
    } finally {
      if (prior == null) {
        System.clearProperty("sirix.projection.globalDict");
      } else {
        System.setProperty("sirix.projection.globalDict", prior);
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void singleRecordAppendsTakeTheTailAndEveryOtherEditFoldsIt(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve("listener-" + versioningType.name().toLowerCase(Locale.ROOT));
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int loaded;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(definition(), writer, 3L);
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","did":"a","time":1},
               {"kind":"commit","did":"b","time":2},
               {"kind":"identity","did":"c","time":3}]
              """), JsonNodeTrx.Commit.NO);
          loaded = writer.getRevisionNumber();
          writer.commit();
        }
        Seen state = seen(session, loaded);
        if ("always".equals(System.getProperty("sirix.projection.globalDict"))) {
          assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, state.page().columnKind(1));
        }
        assertEquals(3, state.page().getRowCount());
        assertFalse(state.tailed(), "a bulk-loaded row group is not tailed");
        // single-record appends: the tail grows, nothing else is rewritten
        for (int i = 1; i <= 4; i++) {
          appendRecord(session, "d" + i, 10 + i);
          state = seen(session, loaded + i);
          assertEquals(3 + i, state.page().getRowCount(), "rows after append " + i);
          assertTrue(state.tailed(), "append " + i + " leaves the row group tailed");
          assertEquals(i, state.tailBlobs(), "one tail blob per commit");
          assertEquals(10 + i, timeAt(state.page(), 2 + i));
          assertEquals("d" + i, state.didValues()[2 + i]);
        }
        // earlier revisions still see exactly their rows
        assertEquals(5, seen(session, loaded + 2).page().getRowCount());
        assertEquals(12, timeAt(seen(session, loaded + 2).page(), 4));
        // a field update of a persisted row is a column patch: the tail folds first
        final long[] keys = recordKeys(session, 7);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(fieldKey(writer, keys[0], "time")));
          writer.setNumberValue(99L);
          writer.commit();
        }
        state = seen(session, loaded + 5);
        assertEquals(7, state.page().getRowCount());
        assertFalse(state.tailed(), "a column patch folds the tail");
        assertEquals(99L, timeAt(state.page(), 0));
        assertEquals(14L, timeAt(state.page(), 6));
        // appends after the fold open a fresh tail
        appendRecord(session, "e1", 21);
        appendRecord(session, "e2", 22);
        state = seen(session, loaded + 7);
        assertEquals(9, state.page().getRowCount());
        assertTrue(state.tailed());
        assertEquals(2, state.tailBlobs());
        assertEquals(22L, timeAt(state.page(), 8));
        // a delete is a membership rewrite: the tail folds into the rebuilt row group
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(keys[1]));
          writer.remove();
          writer.commit();
        }
        state = seen(session, loaded + 8);
        assertEquals(8, state.page().getRowCount());
        assertFalse(state.tailed(), "a delete folds the tail");
        assertEquals(99L, timeAt(state.page(), 0));
        assertEquals(3L, timeAt(state.page(), 1));
        assertEquals(22L, timeAt(state.page(), 7));
        appendRecord(session, "f1", 31);
        state = seen(session, loaded + 9);
        assertEquals(9, state.page().getRowCount());
        assertTrue(state.tailed());
        assertEquals(1, state.tailBlobs());
        assertEquals(31L, timeAt(state.page(), 8));
        // the tailed revisions before the folds are still reconstructed after many later commits
        assertEquals(9, seen(session, loaded + 7).page().getRowCount());
        assertTrue(seen(session, loaded + 7).tailed());
        assertEquals(7, seen(session, loaded + 4).page().getRowCount());
      }
    }
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      final int loaded = 1;
      assertEquals(3, seen(session, loaded).page().getRowCount());
      assertEquals(7, seen(session, loaded + 4).page().getRowCount());
      assertTrue(seen(session, loaded + 4).tailed());
      assertEquals(99L, timeAt(seen(session, loaded + 5).page(), 0));
      assertFalse(seen(session, loaded + 5).tailed());
      assertEquals(9, seen(session, loaded + 9).page().getRowCount());
      assertEquals(31L, timeAt(seen(session, loaded + 9).page(), 8));
      assertEquals("f1", seen(session, loaded + 9).didValues()[8]);
    }
  }
}
