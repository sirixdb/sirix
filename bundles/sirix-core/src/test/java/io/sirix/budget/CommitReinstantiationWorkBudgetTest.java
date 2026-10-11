package io.sirix.budget;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budget for the device round trips of a durable commit and of closing the committed writer.
 *
 * <p>
 * A durable commit forces the data file for its write-ahead barrier and once more to flush its two
 * beacon copies (the preallocated profile with buffered beacons, the default); nothing else on the
 * commit path may force it. Closing the committed writer used to force the data file once more,
 * although the commit had already forced everything the writer wrote: one device round trip per
 * commit for nothing, since every commit re-instantiates its writer. A writer with no unforced
 * writes and no dirty allocation metadata now closes without forcing.
 *
 * <p>
 * Measured on this fixture (FILE_CHANNEL, default commit profile): each of the three commits forces
 * the data file 2 times, and the transaction's close forces it 0 times (1 before the change).
 */
@Isolated
final class CommitReinstantiationWorkBudgetTest {

  private static final String RESOURCE = "reinstantiation";

  private static final WorkCapture CAPTURE = WorkCapture.of(EngineWorkCounters.DATA_FILE_FORCES);

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void aCommitForcesTheDataFileTwiceAndTheWritersCloseNotAtAll() throws Exception {
    final var databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final JsonNodeTrx trx = session.beginNodeTrx();
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"category\":\"a\"}]"), JsonNodeTrx.Commit.NO);
        final WorkReport firstCommit = CAPTURE.run(trx::commit);
        firstCommit.assertBetween(EngineWorkCounters.DATA_FILE_FORCES, 1, 2,
            "the first commit forced the data file more often than its write-ahead barrier and its beacon flush");
        for (int i = 0; i < 2; i++) {
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild(), "the array root");
          trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"category\":\"b" + i + "\"}"),
              JsonNodeTrx.Commit.NO);
          final WorkReport commit = CAPTURE.run(trx::commit);
          commit.assertBetween(EngineWorkCounters.DATA_FILE_FORCES, 1, 2,
              "a commit forced the data file more often than its write-ahead barrier and its beacon flush");
        }
        assertEquals(4, trx.getRevisionNumber(), "the fixture's revision bookkeeping");
        final WorkReport close = CAPTURE.run(trx::close);
        close.assertZero(EngineWorkCounters.DATA_FILE_FORCES,
            "closing a committed writer forced the data file although the commit had forced everything it wrote");
        // The counter is live: a writer with unforced writes still forces at close (an aborted commit's
        // pages must not be left to the page cache alone before the channel is handed on).
        final JsonNodeTrx aborted = session.beginNodeTrx();
        aborted.moveToDocumentRoot();
        assertTrue(aborted.moveToFirstChild(), "the array root");
        aborted.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"category\":\"c\"}"),
            JsonNodeTrx.Commit.NO);
        aborted.rollback();
        aborted.close();
      }
    }
  }
}
