package io.sirix.index.projection;

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
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budget for appending rows to a projection's open row group.
 *
 * <p>
 * A commit that only appends records to the end of an indexed record set takes the open-row-group
 * tail path: the new rows are stored as a tail of the persisted row group. That path used to
 * re-extract every row of the leaf from the document into a fresh page, only to prove the
 * re-extracted prefix equal to the persisted rows before appending the tail: O(leaf) record visits
 * for a commit that appended a handful of rows (a leaf holds up to {@code MAX_ROWS} rows of every
 * column). The persisted rows of a tail-eligible leaf are untouched by construction (no removal,
 * every insertion at the end, no column-only update pending for it), so only the appended rows are
 * extracted now.
 *
 * <p>
 * Measured on this fixture (600 persisted records, 4 appended in one commit): the commit visits
 * 11,848 records with the prefix re-extraction (every persisted row, every column) and 204 without
 * (the appended rows plus the commit's own bookkeeping). The row group is read back with every row
 * in document order and the appended records counted, so the budget is not bought with a wrong
 * tail.
 */
@Isolated
final class ProjectionTailAppendWorkBudgetTest {

  private static final int INDEX = 0;

  private static final int PERSISTED_ROWS = 600;

  private static final int APPENDED_ROWS = 4;

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @Test
  void appendingToAnOpenRowGroupExtractsOnlyTheAppendedRows() throws Exception {
    final Path databasePath = directory.resolve("database");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .buildPathStatistics(true)
                                                              .build()));
      try (final JsonResourceSession session = database.beginResourceSession("resource")) {
        try (final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(records(0, PERSISTED_ROWS)),
              JsonNodeTrx.Commit.NO);
          ((JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber())).createIndexes(
              Set.of(definition()), writer);
          writer.commit();
        }
        assertEquals(PERSISTED_ROWS, rowCount(session, session.getMostRecentRevisionNumber()),
            "the projection holds every persisted record");

        try (final JsonNodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild(), "the record array");
          for (int i = 0; i < APPENDED_ROWS; i++) {
            assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild(), "the record array");
            writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(record(PERSISTED_ROWS + i)),
                JsonNodeTrx.Commit.NO);
          }
          final WorkReport commit = WorkCapture.of(EngineWorkCounters.REPLAY_RECORD_VISITS).run(writer::commit);
          commit.assertBetween(EngineWorkCounters.REPLAY_RECORD_VISITS, 1, 400,
              "appending rows to an open row group re-extracted the persisted rows of the leaf from the document");
        }
        assertEquals(PERSISTED_ROWS + APPENDED_ROWS, rowCount(session, session.getMostRecentRevisionNumber()),
            "the projection holds the appended records");
      }
    }
  }

  private static IndexDef definition() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/did", PathParser.Type.JSON),
            parse("/[]/time", PathParser.Type.JSON)),
        List.of(Type.STR, Type.STR, Type.LON), INDEX, IndexDef.DbType.JSON);
  }

  private static String records(final int from, final int to) {
    final StringBuilder json = new StringBuilder((to - from) * 48).append('[');
    for (int i = from; i < to; i++) {
      if (i > from) {
        json.append(',');
      }
      json.append(record(i));
    }
    return json.append(']').toString();
  }

  private static String record(final int i) {
    return "{\"kind\":\"commit\",\"did\":\"d" + i + "\",\"time\":" + i + "}";
  }

  /** Rows of the single row group, read through the committed revision's column segment slots. */
  private static int rowCount(final JsonResourceSession session, final int revision) {
    try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader storage = reader.getStorageEngineReader();
      final byte[] raw = ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(storage, INDEX, 1);
      assertNotNull(raw, "row group 1 at revision " + revision);
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(raw);
      final long[] keys = page.recordKeys();
      for (int row = 1; row < page.getRowCount(); row++) {
        assertTrue(keys[row - 1] < keys[row], "record keys stay in document order at row " + row);
      }
      return page.getRowCount();
    }
  }
}
