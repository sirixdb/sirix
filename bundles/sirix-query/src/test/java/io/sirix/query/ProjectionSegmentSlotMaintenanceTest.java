package io.sirix.query;

import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Sequence;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Incremental maintenance of a SEGMENT-SLOT projection store.
 *
 * <p>Both storage layouts are patched in place at pre-commit. A segment-slot store keys its
 * zone-map descriptor at slotKind 0 of the composite key and each column segment at its own slot,
 * so every row-group read/write in the maintenance path is layout-dispatched; before that dispatch
 * existed the listener bailed out and forced a FULL REBUILD on every change.</p>
 *
 * <p>Correctness alone cannot distinguish the two paths — a rebuild is also correct — so
 * {@link #segmentSlotDeleteLeavesRowGroupsUnrepacked()} pins the difference that IS observable:
 * incremental patching only rewrites the touched row group and leaves the others exactly as they
 * were, whereas a full rebuild re-extracts every record and re-packs the rows densely, shifting
 * them between row groups. The per-row-group row counts therefore differ between the two.</p>
 */
public final class ProjectionSegmentSlotMaintenanceTest extends AbstractJsonTest {

  private static final String[] SOURCE_PATH = { "[]" };

  /** {@code ProjectionIndexRowGroupPage.MAX_ROWS}; a row group holds at most this many rows. */
  private static final int MAX_ROWS = 1024;

  /** Two row groups: 1024 + 476. Enough that a re-pack is visible in the per-group counts. */
  private static final int RECORDS = 1500;

  /** One projection index per database in this test ⇒ storage index number 0. */
  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
  }

  /** Deterministic age of record {@code i}; keeps the aggregate hand-computable. */
  private static long ageOf(final int i) {
    return (i % 50) + 1;
  }

  private static String buildDataset() {
    final StringBuilder sb = new StringBuilder(RECORDS * 48).append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"age\":").append(ageOf(i))
        .append(",\"active\":").append((i & 1) == 0)
        .append(",\"dept\":\"d").append(i % 7).append("\"}");
    }
    return sb.append(']').toString();
  }

  /** Store the dataset and build a SEGMENT-SLOT projection index over it. */
  private void storeAndCreateSegmentSlotProjection() {
    query("jn:store('json-path1','sales.jn','" + buildDataset() + "')");
    final String prior = System.getProperty("sirix.projection.segmentSlotLayout");
    System.setProperty("sirix.projection.segmentSlotLayout", "true");
    try {
      query("""
            let $doc := jn:doc('json-path1','sales.jn')
            let $stats := jn:create-projection-index($doc, '/[]',
                ('/[]/age', '/[]/active', '/[]/dept'),
                ('long', 'boolean', 'string'))
            return {"revision": sdb:commit($doc)}
          """);
    } finally {
      if (prior == null) {
        System.clearProperty("sirix.projection.segmentSlotLayout");
      } else {
        System.setProperty("sirix.projection.segmentSlotLayout", prior);
      }
    }
  }

  private static Database<JsonResourceSession> openDatabase() {
    final Path dbPath =
        Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    return Databases.openJsonDatabase(dbPath);
  }

  private static long sumAges(final SirixVectorizedExecutor executor) {
    final Sequence result = executor.executeAggregate(null, SOURCE_PATH, "sum", "age");
    Assertions.assertNotNull(result, "the aggregate must be SERVED from the projection");
    return ((Int64) result).longValue();
  }

  private static long countAges(final SirixVectorizedExecutor executor) {
    final Sequence result = executor.executeAggregate(null, SOURCE_PATH, "count", "age");
    Assertions.assertNotNull(result, "the aggregate must be SERVED from the projection");
    return ((Int64) result).longValue();
  }

  /** Move the wtx onto record {@code recordIndex}'s "age" field. */
  private static void moveToAgeField(final JsonNodeTrx wtx, final int recordIndex) {
    Assertions.assertTrue(wtx.moveToDocumentRoot());
    Assertions.assertTrue(wtx.moveToFirstChild());          // top-level ARRAY
    Assertions.assertTrue(wtx.moveToFirstChild());          // record 0
    for (int i = 0; i < recordIndex; i++) {
      Assertions.assertTrue(wtx.moveToRightSibling());
    }
    Assertions.assertTrue(wtx.moveToFirstChild());          // first field = "age"
  }

  /** Read the persisted metadata of the projection store at the session's newest revision. */
  private static ProjectionIndexMetadata readMetadata(final JsonResourceSession session) {
    try (final JsonNodeReadOnlyTrx rtx =
             session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final ProjectionIndexMetadata meta = ProjectionIndexMetadata.parse(
          ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0L));
      Assertions.assertNotNull(meta, "projection metadata must be present");
      return meta;
    }
  }

  /** Row count of one row group, read from its zone-map descriptor slot alone. */
  private static long rowGroupRowCount(final JsonResourceSession session, final long rowGroupId) {
    try (final JsonNodeReadOnlyTrx rtx =
             session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      return ProjectionIndexHOTStorage.readRowCountFromColumnSegmentSlots(
          rtx.getStorageEngineReader(), INDEX_NUMBER, rowGroupId);
    }
  }

  private static long expectedTotalAge() {
    long sum = 0;
    for (int i = 0; i < RECORDS; i++) {
      sum += ageOf(i);
    }
    return sum;
  }

  @Test
  public void segmentSlotUpdateIsMaintainedAndKeepsTheLayoutFlag() throws IOException {
    storeAndCreateSegmentSlotProjection();
    final long baseline = expectedTotalAge();

    try (final Database<JsonResourceSession> database = openDatabase();
         final JsonResourceSession session = database.beginResourceSession("sales.jn")) {
      Assertions.assertTrue(readMetadata(session).isColumnSegmentSlotLayout(),
          "the store must have been built in the segment-slot layout");

      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        // Update record 0's age to 9999 (it lives in the FIRST row group).
        moveToAgeField(wtx, 0);
        wtx.setNumberValue(9999);
        final SirixVectorizedExecutor wtxExecutor = new SirixVectorizedExecutor(wtx, 2);
        try {
          Assertions.assertEquals(baseline - ageOf(0) + 9999, sumAges(wtxExecutor),
              "read-your-writes through the segment-slot maintenance path");
        } finally {
          wtxExecutor.close();
        }
        wtx.commit();
      }

      // Persisted state after the commit.
      final SirixVectorizedExecutor afterCommit =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        Assertions.assertEquals(baseline - ageOf(0) + 9999, sumAges(afterCommit),
            "the maintained segment-slot store must serve the updated value");
        Assertions.assertEquals(RECORDS, countAges(afterCommit));
      } finally {
        afterCommit.close();
      }

      // Regression guard: the layout flag is sticky. The public metadata constructor defaults it to
      // the descriptor layout, so a maintenance pass that forgot to re-stamp it would leave every
      // later read reinterpreting this store's composite slot keys under the wrong layout.
      Assertions.assertTrue(readMetadata(session).isColumnSegmentSlotLayout(),
          "maintenance must preserve the segment-slot layout flag");
    }
  }

  @Test
  public void segmentSlotDeleteLeavesRowGroupsUnrepacked() throws IOException {
    storeAndCreateSegmentSlotProjection();
    final int tailRows = RECORDS - MAX_ROWS;

    try (final Database<JsonResourceSession> database = openDatabase();
         final JsonResourceSession session = database.beginResourceSession("sales.jn")) {
      Assertions.assertEquals(MAX_ROWS, rowGroupRowCount(session, 1), "row group 1 starts full");
      Assertions.assertEquals(tailRows, rowGroupRowCount(session, 2), "row group 2 holds the tail");

      final int deletions = 5;
      long deletedAgeSum = 0;
      for (int i = 0; i < deletions; i++) {
        deletedAgeSum += ageOf(i);
      }

      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        // Delete the first `deletions` records — all of them live in row group 1.
        for (int i = 0; i < deletions; i++) {
          Assertions.assertTrue(wtx.moveToDocumentRoot());
          Assertions.assertTrue(wtx.moveToFirstChild());   // ARRAY
          Assertions.assertTrue(wtx.moveToFirstChild());   // current first record
          wtx.remove();
        }
        wtx.commit();
      }

      // Correctness.
      final SirixVectorizedExecutor afterCommit =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        Assertions.assertEquals(RECORDS - deletions, countAges(afterCommit));
        Assertions.assertEquals(expectedTotalAge() - deletedAgeSum, sumAges(afterCommit));
      } finally {
        afterCommit.close();
      }

      // The decisive check: only row group 1 was rewritten, and it simply lost its deleted rows.
      // A full rebuild would have re-extracted all RECORDS-deletions records and re-packed them
      // densely — row group 1 would be back at MAX_ROWS and row group 2 would have shrunk instead.
      Assertions.assertEquals(MAX_ROWS - deletions, rowGroupRowCount(session, 1),
          "row group 1 must keep its remaining rows in place (no re-pack)");
      Assertions.assertEquals(tailRows, rowGroupRowCount(session, 2),
          "row group 2 was untouched and must be byte-for-byte unchanged in row count");
      Assertions.assertTrue(readMetadata(session).isColumnSegmentSlotLayout(),
          "maintenance must preserve the segment-slot layout flag");
    }
  }

  @Test
  public void droppedSegmentSlotIndexRebuildsInTheSameLayout() throws IOException {
    // The layout is STICKY, and a tombstone is the only surviving record of it: dropping the index
    // leaves every row-group slot in place. If the tombstone dropped the flag, the re-creation below
    // — running with the opt-in property UNSET — would rebuild in the descriptor layout and write
    // raw-keyed row groups into a sub-tree still full of `rowGroupId << 16` composite keys, which
    // every later full read rejects as "mixed storage layouts in one sub-tree" (a permanently dead
    // projection). The re-created index must therefore come back as segment-slot and serve.
    storeAndCreateSegmentSlotProjection();
    Assertions.assertNull(System.getProperty("sirix.projection.segmentSlotLayout"),
        "the opt-in property must be unset here, so only the persisted flag can drive the layout");

    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $dropped := jn:drop-projection-index($doc, 0)
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // Re-create the SAME definition — the rebuild must recover the layout from the tombstone.
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);

    try (final Database<JsonResourceSession> database = openDatabase();
         final JsonResourceSession session = database.beginResourceSession("sales.jn")) {
      Assertions.assertTrue(readMetadata(session).isColumnSegmentSlotLayout(),
          "the re-created index must recover the segment-slot layout from the tombstone");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        // A mixed-layout sub-tree would make this full read throw and serve nothing.
        Assertions.assertEquals(expectedTotalAge(), sumAges(executor));
        Assertions.assertEquals(RECORDS, countAges(executor));
      } finally {
        executor.close();
      }
    }
  }
}
