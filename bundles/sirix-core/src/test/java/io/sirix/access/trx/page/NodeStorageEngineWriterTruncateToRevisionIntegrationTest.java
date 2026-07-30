package io.sirix.access.trx.page;

import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.utils.JsonDocumentCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Johannes Lichtenberger
 */
public final class NodeStorageEngineWriterTruncateToRevisionIntegrationTest {

  private static final Path RESOURCE_DATA_FILE = JsonTestHelper.PATHS.PATH1.getFile()
                                                                           .resolve("resources")
                                                                           .resolve(JsonTestHelper.RESOURCE)
                                                                           .resolve("data")
                                                                           .resolve("sirix.data");

  private Database<JsonResourceSession> database;

  private JsonResourceSession resourceSession;

  private long fileSize;

  /** Snapshot of the commit mode this test runs under; physical-size expectations differ per mode. */
  private boolean preallocatedMode;

  @BeforeEach
  public void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    preallocatedMode = Boolean.parseBoolean(System.getProperty("sirix.commit.preallocated", "true"));
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);

    try (final var wtx = resourceSession.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();
      fileSize = Files.size(RESOURCE_DATA_FILE);
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("b", new StringValue("value"));
      wtx.commit();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("a", new BooleanValue(false));
      wtx.commit();
      // Under the legacy grow-per-commit path every commit physically extends the file; under
      // preallocated commits (the default) later commits land in-place inside the preallocated
      // region, so only monotonicity holds and the revision number is the progress signal.
      if (preallocatedMode) {
        assertTrue(Files.size(RESOURCE_DATA_FILE) >= fileSize);
      } else {
        assertTrue(Files.size(RESOURCE_DATA_FILE) > fileSize);
      }
      assertEquals(4, wtx.getRevisionNumber());
    }
  }

  @AfterEach
  public void tearDown() {
    // The crash-simulation tests reopen the database OUTSIDE the helper's tracking — close
    // explicitly or deleteEverything() can't remove the directory and the next test sees
    // stale revisions. close() may run twice for the helper-tracked instance; ignore that.
    try {
      resourceSession.close();
      database.close();
    } catch (final IllegalStateException alreadyClosed) {
      // closed via the helper already
    }
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test_when_sirix_is_setup_with_3_revisions_truncate_to_first_revision() throws IOException {
    try (final var storageEngineWriter = resourceSession.createStorageEngineWriter()) {
      storageEngineWriter.truncateTo(1);
    }

    // Truncation physically reclaims the removed revisions' bytes — assert it per mode. Legacy
    // restores the exact post-revision-1 size. Preallocated (the default) truncates to revision
    // 1's LOGICAL end, which is strictly below the preallocation-inflated size recorded at
    // revision 1 — a no-op physical truncation would leave the sizes equal, so require strict
    // shrinkage there.
    if (preallocatedMode) {
      assertTrue(Files.size(RESOURCE_DATA_FILE) < fileSize,
          "truncateTo(1) must physically truncate below the preallocation-inflated post-revision-1 size");
    } else {
      assertEquals(fileSize, Files.size(RESOURCE_DATA_FILE));
    }
    assertEquals(1, resourceSession.getMostRecentRevisionNumber());
    try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToDocumentRoot() && rtx.moveToFirstChild(),
          "revision 1 must remain fully readable after truncation");
    }
  }

  /**
   * The unbounded crash window after an explicit rollback: truncation removes the revision both
   * uber beacons advertise, so dying before the next commit used to leave the resource
   * unopenable (beacons referencing truncated offsets — "Truncated revisions record").
   * truncateTo now rewrites the beacons with the rolled-back uber page; a cold reopen with no
   * intervening commit must succeed and the resource must keep working.
   */
  @Test
  public void test_truncate_survives_process_death_before_next_commit() {
    try (final var storageEngineWriter = resourceSession.createStorageEngineWriter()) {
      storageEngineWriter.truncateTo(1);
    }

    // Simulate the process dying right after the rollback: everything closed, caches cold.
    resourceSession.close();
    database.close();
    io.sirix.access.Databases.clearGlobalCaches();

    // Fresh database instance — the helper caches (and would return) the closed one.
    database = io.sirix.access.Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
    assertEquals(1, resourceSession.getMostRecentRevisionNumber(),
        "cold reopen after rollback must anchor on the truncated-to revision");

    try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
      assertEquals(1, rtx.getRevisionNumber());
      assertTrue(rtx.moveToDocumentRoot() && rtx.moveToFirstChild(),
          "the surviving revision's data must be readable");
    }

    // The resource must accept new commits after the rollback (offsets are reused cleanly).
    try (final var wtx = resourceSession.beginNodeTrx()) {
      wtx.commit();
    }
    assertEquals(2, resourceSession.getMostRecentRevisionNumber());
  }

  /**
   * NodeTrx.truncateTo was a silent no-op TODO — TransactionImpl's cross-resource atomicity
   * undo did nothing. It now performs the full rollback.
   */
  @Test
  public void test_nodeTrx_truncateTo_rolls_back_the_resource() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      wtx.truncateTo(1);
    }

    resourceSession.close();
    database.close();
    io.sirix.access.Databases.clearGlobalCaches();

    database = io.sirix.access.Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
    assertEquals(1, resourceSession.getMostRecentRevisionNumber());
    try (final var rtx = resourceSession.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToDocumentRoot() && rtx.moveToFirstChild());
    }
  }

  @Test
  public void test_nodeTrx_truncateTo_validates_the_target_revision() {
    try (final var wtx = resourceSession.beginNodeTrx()) {
      Assertions.assertThrows(IllegalArgumentException.class, () -> wtx.truncateTo(-1));
      Assertions.assertThrows(IllegalArgumentException.class, () -> wtx.truncateTo(wtx.getRevisionNumber()));
      wtx.rollback();
    }
  }
}
