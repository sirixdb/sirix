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
 * Work budget for maintaining a projection row group of which a commit changes one row.
 *
 * <p>
 * A commit that changes one record used to read every record of that record's row group from the
 * document again: a value update re-extracted the changed column for every row, and an insert or a
 * delete rebuilt the row group from the document. A row group holds up to {@code MAX_ROWS} rows, so
 * the work followed the size of the row group, not the size of the change. Rows the commit did not
 * change are now carried over from the decoded row group, and only the changed rows are read from
 * the document.
 * </p>
 *
 * <p>
 * The fixture is one row group of 900 records with a long, a string, a double and a timestamp
 * column. Its double cells come from the shredder and lack {@code Double} typing, and its
 * timestamps are Z-suffixed, so both carry a sticky column flag: the two cases a real resource has
 * in every row group. Each test runs its commit twice on fresh databases, once under the reference
 * behaviour that reads every row, as a positive control: it proves the counters are live and that
 * the bound separates the two behaviours. Each commit's row group is read back, so the budget is
 * not bought with a wrong row.
 * </p>
 *
 * <p>
 * Measured on this fixture (record visits of the commit, reference behaviour against carry-over): a
 * long cell updated in place 21,176 against 577; a double cell updated in place 21,177 against 596
 * (one carried cell is probed for the purity flag); one record inserted in the middle of the row
 * group and another deleted 20,932 against 1,075. What remains is the commit's own bookkeeping and
 * the changed record.
 * </p>
 */
@Isolated
final class ProjectionUnchangedRowWorkBudgetTest {

  private static final int INDEX = 0;

  private static final int ROWS = 900;

  private static final int LONG_COLUMN = 0;

  private static final int DOUBLE_COLUMN = 2;

  @TempDir
  Path directory;

  @AfterEach
  void restoreOrdinaryMaintenance() {
    ProjectionIndexChangeListener.reextractUnchangedRows(false);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @Test
  void updatingOneLongCellReadsOneRecord() throws Exception {
    final Outcome reference = updateOneCell("reference-long", true, "time", 123_456_789L);
    reference.work.assertAtLeast(EngineWorkCounters.REPLAY_RECORD_VISITS, 15_000,
        "the reference behaviour reads every row of the row group, which is what the bound below excludes");
    assertEquals(ROWS, reference.locality.rowsExtracted(), "the reference behaviour extracts every row");

    final Outcome carry = updateOneCell("carry-long", false, "time", 123_456_789L);
    carry.work.assertBetween(EngineWorkCounters.REPLAY_RECORD_VISITS, 1, 1_000,
        "a one-cell update re-extracted the unchanged rows of its row group from the document");
    assertEquals(1, carry.locality.rowsExtracted(), "only the changed record is read from the document");
    assertEquals(ROWS - 1, carry.locality.rowsCarriedOver(), "every other row is carried over");
    assertEquals(0, carry.locality.witnessProbes(), "a long column has no purity flag to settle");
    assertEquals(123_456_789L, carry.page.numericColumn(LONG_COLUMN)[carry.row], "the updated cell");
    assertEquals(reference.page.numericColumn(LONG_COLUMN)[reference.row],
        carry.page.numericColumn(LONG_COLUMN)[carry.row], "both behaviours persist the same cell");
  }

  @Test
  void updatingOneDoubleCellProbesOneCarriedCellForThePurityFlag() throws Exception {
    final Outcome reference = updateOneCell("reference-double", true, "score", 2.5d);
    reference.work.assertAtLeast(EngineWorkCounters.REPLAY_RECORD_VISITS, 15_000,
        "the reference behaviour reads every row of the row group, which is what the bound below excludes");

    final Outcome carry = updateOneCell("carry-double", false, "score", 2.5d);
    carry.work.assertBetween(EngineWorkCounters.REPLAY_RECORD_VISITS, 1, 1_000,
        "a one-cell update re-extracted the unchanged rows of its row group from the document");
    assertEquals(1, carry.locality.rowsExtracted(), "only the changed record is read from the document");
    assertEquals(ROWS - 1, carry.locality.rowsCarriedOver(), "every other row is carried over");
    // The rewritten cell is Double-typed; the shredded cells beside it are not, and the row group's
    // flag does not say which. One carried cell read from the document settles it.
    assertEquals(1, carry.locality.witnessProbes(), "the first carried cell proves the column impure");
    assertTrue(!carry.page.columnPureDoubleSource(DOUBLE_COLUMN), "the column still holds shredded cells");
    assertEquals(reference.page.columnPureDoubleSource(DOUBLE_COLUMN), carry.page.columnPureDoubleSource(DOUBLE_COLUMN),
        "both behaviours persist the same purity flag");
    assertEquals(reference.page.numericColumn(DOUBLE_COLUMN)[reference.row],
        carry.page.numericColumn(DOUBLE_COLUMN)[carry.row], "both behaviours persist the same cell");
  }

  @Test
  void insertingAndDeletingOneRecordReadsOnlyTheInsertedRecord() throws Exception {
    final Outcome reference = insertAndDelete("reference-membership", true);
    reference.work.assertAtLeast(EngineWorkCounters.REPLAY_RECORD_VISITS, 15_000,
        "the reference behaviour reads every row of the row group, which is what the bound below excludes");
    assertEquals(ROWS, reference.locality.rowsExtracted(), "the reference behaviour extracts every row");

    final Outcome carry = insertAndDelete("carry-membership", false);
    carry.work.assertBetween(EngineWorkCounters.REPLAY_RECORD_VISITS, 1, 1_800,
        "an insert and a delete rebuilt their row group from the document");
    assertEquals(1, carry.locality.rowsExtracted(), "only the inserted record is read from the document");
    assertEquals(ROWS - 1, carry.locality.rowsCarriedOver(), "every surviving row is carried over");
    assertEquals(ROWS, carry.page.getRowCount(), "one row in, one row out");
    assertEquals(-42L, carry.page.numericColumn(LONG_COLUMN)[carry.row], "the inserted record's cell");
  }

  /** What one measured commit did, and the row group it left behind. */
  private record Outcome(WorkReport work, ProjectionIndexChangeListener.MaintenanceLocality locality,
      ProjectionIndexRowGroupPage page, int row) {
  }

  private Outcome updateOneCell(final String name, final boolean reextract, final String field, final Number value)
      throws Exception {
    try (final Database<JsonResourceSession> database = open(name);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      final long recordKey = load(session, ROWS / 2);
      ProjectionIndexChangeListener.reextractUnchangedRows(reextract);
      final WorkReport work;
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        moveToFieldValue(writer, recordKey, field);
        writer.setNumberValue(value);
        work = WorkCapture.of(EngineWorkCounters.REPLAY_RECORD_VISITS).run(writer::commit);
      }
      return outcome(session, work, recordKey);
    }
  }

  private Outcome insertAndDelete(final String name, final boolean reextract) throws Exception {
    try (final Database<JsonResourceSession> database = open(name);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      final long anchor = load(session, ROWS / 2);
      ProjectionIndexChangeListener.reextractUnchangedRows(reextract);
      final WorkReport work;
      final long inserted;
      try (final JsonNodeTrx writer = session.beginNodeTrx()) {
        assertTrue(writer.moveTo(anchor), "the anchor record");
        writer.insertSubtreeAsRightSibling(
            JsonShredder.createStringReader(
                "{\"time\":-42,\"kind\":\"commit\",\"score\":0.5,\"at\":\"2026-02-01T00:00:00Z\"}"),
            JsonNodeTrx.Commit.NO);
        inserted = writer.getNodeKey();
        assertTrue(writer.moveTo(anchor) && writer.moveToLeftSibling(), "the record before the anchor");
        writer.remove();
        work = WorkCapture.of(EngineWorkCounters.REPLAY_RECORD_VISITS).run(writer::commit);
      }
      return outcome(session, work, inserted);
    }
  }

  private Database<JsonResourceSession> open(final String name) {
    final Path databasePath = directory.resolve(name);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
    assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                            .storageType(StorageType.FILE_CHANNEL)
                                                            .buildPathStatistics(true)
                                                            .build()));
    return database;
  }

  /** Load the fixture and build the projection; returns the node key of record {@code ordinal}. */
  private static long load(final JsonResourceSession session, final int ordinal) {
    final StringBuilder json = new StringBuilder(ROWS * 96).append('[');
    for (int i = 0; i < ROWS; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"time\":")
          .append(i)
          .append(",\"kind\":\"")
          .append(i % 3 == 0
              ? "commit"
              : "like")
          .append("\",\"score\":")
          .append(i / 4.0 + 0.5)
          .append(",\"at\":\"2026-01-")
          .append(10 + i % 18)
          .append("T00:00:00Z\"}");
    }
    json.append(']');
    final long recordKey;
    try (final JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
      ((JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber())).createIndexes(
          Set.of(definition()), writer);
      assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild() && writer.moveToFirstChild(),
          "the first record");
      for (int i = 0; i < ordinal; i++) {
        assertTrue(writer.moveToRightSibling(), "record " + (i + 1));
      }
      recordKey = writer.getNodeKey();
      writer.commit();
    }
    // Go cold: a warm page cache would serve the reference behaviour's reads for free.
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    return recordKey;
  }

  private static IndexDef definition() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/time", PathParser.Type.JSON), parse("/[]/kind", PathParser.Type.JSON),
            parse("/[]/score", PathParser.Type.JSON), parse("/[]/at", PathParser.Type.JSON)),
        List.of(Type.LON, Type.STR, Type.DBL, Type.DATI), INDEX, IndexDef.DbType.JSON);
  }

  private static void moveToFieldValue(final JsonNodeTrx writer, final long recordKey, final String field) {
    assertTrue(writer.moveTo(recordKey) && writer.moveToFirstChild(), "record " + recordKey);
    do {
      if (writer.getName() != null && field.equals(writer.getName().getLocalName())) {
        if (writer.hasFirstChild()) {
          writer.moveToFirstChild();
        }
        return;
      }
    } while (writer.moveToRightSibling());
    throw new AssertionError("field " + field + " of record " + recordKey);
  }

  private static Outcome outcome(final JsonResourceSession session, final WorkReport work, final long recordKey) {
    final ProjectionIndexChangeListener.MaintenanceLocality locality =
        ProjectionIndexChangeListener.lastMaintenanceLocality();
    try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final byte[] raw =
          ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader.getStorageEngineReader(), INDEX, 1);
      assertNotNull(raw, "the fixture's row group");
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(raw);
      final long[] keys = page.recordKeys();
      int row = -1;
      for (int i = 0; i < page.getRowCount(); i++) {
        if (i > 0) {
          assertTrue(keys[i - 1] != keys[i], "record keys are distinct at row " + i);
        }
        if (keys[i] == recordKey) {
          row = i;
        }
      }
      assertTrue(row >= 0, "the row group holds record " + recordKey);
      return new Outcome(work, locality, page, row);
    }
  }
}
