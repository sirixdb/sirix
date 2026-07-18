package io.sirix.query;

import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Sequence;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.node.NodeKind;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Wtx-visible projection serving: an executor bound to an OPEN write
 * transaction serves analytics from the transaction's UNCOMMITTED state —
 * read-your-writes through {@code IndexController#openProjectionIndex},
 * which applies the pending incremental maintenance and reads the leaves
 * through the transaction log. Committed-revision executors keep seeing the
 * committed snapshot (isolation), the commit applies only the remaining
 * delta after a mid-transaction flush (re-entrancy), and a rollback
 * discards everything.
 */
public final class ProjectionIndexWtxServingTest extends AbstractJsonTest {

  private static final String[] SOURCE_PATH = { "[]" };

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
  }

  private void storeAndCreateProjection() {
    query("""
          jn:store('json-path1','sales.jn','[
            {"age": 30, "active": true,  "dept": "Eng"},
            {"age": 45, "active": false, "dept": "Sales"},
            {"age": 52, "active": true,  "dept": "Eng"},
            {"age": 23, "active": true,  "dept": "HR"},
            {"age": 61, "active": false, "dept": "Eng"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
  }

  private static Database<JsonResourceSession> openDatabase() {
    final Path dbPath =
        Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    return Databases.openJsonDatabase(dbPath);
  }

  /** Move the wtx to record {@code recordIndex}'s "age" field (its first child). */
  private static void moveToAgeField(final JsonNodeTrx wtx, final int recordIndex) {
    Assertions.assertTrue(wtx.moveToDocumentRoot());
    Assertions.assertTrue(wtx.moveToFirstChild());          // top-level ARRAY
    Assertions.assertTrue(wtx.moveToFirstChild());          // record 0
    for (int i = 0; i < recordIndex; i++) {
      Assertions.assertTrue(wtx.moveToRightSibling());
    }
    Assertions.assertTrue(wtx.moveToFirstChild());          // first field = "age"
    Assertions.assertEquals(NodeKind.OBJECT_NAMED_NUMBER, wtx.getKind());
  }

  private static long sumAges(final SirixVectorizedExecutor executor) {
    final Sequence result = executor.executeAggregate(null, SOURCE_PATH, "sum", "age");
    Assertions.assertNotNull(result, "the aggregate must be SERVED from the projection");
    return ((Int64) result).longValue();
  }

  @Test
  public void wtxExecutorServesUncommittedStateAndCommitPersistsIt() throws IOException {
    storeAndCreateProjection();
    try (final Database<JsonResourceSession> database = openDatabase();
         final JsonResourceSession session = database.beginResourceSession("sales.jn")) {
      final int committedRevision = session.getMostRecentRevisionNumber();
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        // Uncommitted update: record 0's age 30 → 99.
        moveToAgeField(wtx, 0);
        wtx.setNumberValue(99);

        final SirixVectorizedExecutor wtxExecutor = new SirixVectorizedExecutor(wtx, 2);
        try {
          // Read-your-writes BEFORE commit: 211 - 30 + 99 = 280.
          Assertions.assertEquals(280L, sumAges(wtxExecutor));

          // Isolation: a committed-revision executor still sees 211.
          final SirixVectorizedExecutor committedExecutor =
              new SirixVectorizedExecutor(session, committedRevision, 2);
          try {
            Assertions.assertEquals(211L, sumAges(committedExecutor));
          } finally {
            committedExecutor.close();
          }

          // Flush re-entrancy: the first wtx read applied the pending
          // maintenance; a SECOND update afterwards must be visible to the
          // next wtx read (record 1's age 45 → 46 ⇒ 280 - 45 + 46 = 281)
          // and the commit must apply exactly the remaining delta.
          moveToAgeField(wtx, 1);
          wtx.setNumberValue(46);
          Assertions.assertEquals(281L, sumAges(wtxExecutor));

          // Intermediate commit: the transaction's contract is to REPLACE its
          // storage engine with one bound to the successor revision. The SAME
          // executor must follow it — it resolves the writer and controller
          // through the transaction facade per call.
          wtx.commit();
          Assertions.assertEquals(281L, sumAges(wtxExecutor));

          // And a further uncommitted update in the NEW epoch is visible too
          // (record 2's age 52 → 50 ⇒ 281 - 52 + 50 = 279).
          moveToAgeField(wtx, 2);
          wtx.setNumberValue(50);
          Assertions.assertEquals(279L, sumAges(wtxExecutor));
          wtx.commit();
        } finally {
          wtxExecutor.close();
        }
      }
      final SirixVectorizedExecutor afterCommit =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        Assertions.assertEquals(279L, sumAges(afterCommit));
      } finally {
        afterCommit.close();
      }
    }
  }

  @Test
  public void rollbackDiscardsWtxVisibleChanges() throws IOException {
    storeAndCreateProjection();
    try (final Database<JsonResourceSession> database = openDatabase();
         final JsonResourceSession session = database.beginResourceSession("sales.jn")) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        moveToAgeField(wtx, 0);
        wtx.setNumberValue(1000);
        final SirixVectorizedExecutor wtxExecutor = new SirixVectorizedExecutor(wtx, 2);
        try {
          Assertions.assertEquals(1181L, sumAges(wtxExecutor)); // 211 - 30 + 1000
        } finally {
          wtxExecutor.close();
        }
        wtx.rollback();
      }
      final SirixVectorizedExecutor afterRollback =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        Assertions.assertEquals(211L, sumAges(afterRollback));
      } finally {
        afterRollback.close();
      }
    }
  }

  @Test
  public void deletedRecordDropsOutOfWtxServing() throws IOException {
    storeAndCreateProjection();
    try (final Database<JsonResourceSession> database = openDatabase();
         final JsonResourceSession session = database.beginResourceSession("sales.jn")) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        // Delete record 1 (age 45).
        Assertions.assertTrue(wtx.moveToDocumentRoot());
        Assertions.assertTrue(wtx.moveToFirstChild());     // ARRAY
        Assertions.assertTrue(wtx.moveToFirstChild());     // record 0
        Assertions.assertTrue(wtx.moveToRightSibling());   // record 1
        wtx.remove();
        final SirixVectorizedExecutor wtxExecutor = new SirixVectorizedExecutor(wtx, 2);
        try {
          Assertions.assertEquals(166L, sumAges(wtxExecutor)); // 211 - 45
          final Sequence count = wtxExecutor.executeAggregate(null, SOURCE_PATH, "count", "age");
          Assertions.assertNotNull(count);
          Assertions.assertEquals(4L, ((Int64) count).longValue());
        } finally {
          wtxExecutor.close();
        }
        wtx.rollback();
      }
    }
  }
}
