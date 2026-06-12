package io.sirix.access.trx.node;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Auto-commit ({@code beginNodeTrx(maxTime, timeUnit)}) commits from a TIMER thread while the
 * owning thread keeps using the transaction — the documented usage explicitly invites this
 * cross-thread interaction. The post-commit {@code reInstantiate()} swap (close old storage
 * engine → create new → publish) left a multi-millisecond window in which
 * {@code getRevisionNumber()} dereferenced a closed (or null) engine and died with
 * {@code AssertionError: Transaction is already closed!} from
 * {@code NodeStorageEngineReader.assertNotClosed}.
 *
 * <p>This stress test reproduced that race deterministically within roughly a second before the
 * fix (revision number served from a volatile cached value instead of the swapping engine).
 */
final class AutoCommitRevisionReadStressTest {

  /** Short cadence maximizes engine swaps per second; the window opens on every commit. */
  private static final int AUTO_COMMIT_MILLIS = 5;

  private static final long STRESS_DURATION_MILLIS = 3_000;

  private Database<JsonResourceSession> database;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  @DisplayName("getRevisionNumber() polled concurrently with timer auto-commits never observes a closed engine")
  void pollingRevisionNumberWhileAutoCommitTimerSwapsEnginesIsSafe() {
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx(AUTO_COMMIT_MILLIS, TimeUnit.MILLISECONDS)) {
      final int initialRevision = wtx.getRevisionNumber();

      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(STRESS_DURATION_MILLIS);
      int lastSeen = initialRevision;
      long reads = 0;
      while (System.nanoTime() < deadline) {
        // Pre-fix this poll raced the timer thread's reInstantiate() into
        // "AssertionError: Transaction is already closed!" (or an NPE in the null window).
        final int revision = wtx.getRevisionNumber();
        assertTrue(revision >= lastSeen,
            "revision must never move backwards (read " + revision + " after " + lastSeen + ")");
        lastSeen = revision;
        reads++;
        Thread.onSpinWait();
      }

      assertTrue(reads > 0, "the poll loop must have run");
      assertTrue(lastSeen > initialRevision,
          "the auto-commit timer must have committed at least once during the stress window (saw "
              + lastSeen + ", started at " + initialRevision + ")");
    }
  }
}
