/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import io.sirix.io.fault.FaultInjectingStorage;
import io.sirix.io.fault.FaultInjectingWriter;
import io.sirix.io.fault.FaultInjectingWriter.Point;
import io.sirix.io.fault.FaultInjectingWriter.SimulatedCrashException;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * How a synchronous commit reports a failure on each side of the commit point.
 *
 * <p>
 * A commit crosses its commit point when {@code Writer#writeUberPageReference} returns: at that
 * moment revision N is durable and acknowledged. The steps after it — forgetting the spill file,
 * closing durable read pages, releasing the page guard, clearing the transaction-intent log,
 * deleting the recovery marker — are cleanup of a revision that already exists.
 * {@code spec: TXN-307} makes a failure in that cleanup a success for the caller: it is logged, the
 * resource is flagged for repair and the writer becomes terminal, but the commit is NOT reported as
 * failed, because the data IS persisted. {@code spec: TXN-305} is the contrast: a failure known to
 * precede the commit point means nothing was committed and leaves the transaction usable.
 *
 * <p>
 * The failures are injected through {@link FaultInjectingStorage}, which puts a
 * {@link FaultInjectingWriter} in front of the real {@code FILE_CHANNEL} writer of a live resource
 * session, so these are real commits of a real resource.
 */
final class PostDurableCleanupFailureTest {

  private static final String RESOURCE = "resource";

  private Path workRoot;

  @BeforeEach
  void setUp() throws IOException {
    workRoot = Files.createTempDirectory("sirix-post-durable-cleanup-").toRealPath();
    FaultInjectingStorage.install(workRoot);
  }

  @AfterEach
  void tearDown() throws IOException {
    FaultInjectingStorage.uninstall();
    deleteRecursive(workRoot);
  }

  /**
   * A cleanup failure after the commit point: the revision is durable, so the commit must be reported
   * as the success it is, the writer must be terminal rather than running, and the revision must be
   * readable afterwards. {@code spec: TXN-307}
   */
  @Test
  void cleanupFailureAfterTheCommitPointReportsTheCommitAsSucceeded() {
    final Path databasePath = createDatabase();

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final int bootstrapRevision = session.getMostRecentRevisionNumber();

      final JsonNodeTrx wtx = session.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);

      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      injector.injectAt(Point.BEFORE_DISCARD_SPILLED_WRITES, 1);

      // The commit point was crossed, so this must NOT throw — before the fix it reported the
      // simulated cleanup failure as a commit failure (#1380).
      wtx.commit();

      assertFalse(injector.isArmedFor(Point.BEFORE_DISCARD_SPILLED_WRITES),
          "the post-durable cleanup injection never fired, so this run proves nothing");

      // Terminal: the writer does not go back to RUNNING, so it can neither be reused nor retried
      // into a second copy of the same change.
      assertThrows(IllegalStateException.class,
          () -> wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"b\":2}"), JsonNodeTrx.Commit.NO));

      // Nothing uncommitted is left, so closing the terminal writer is clean.
      wtx.close();

      assertEquals(bootstrapRevision + 1, session.getMostRecentRevisionNumber(),
          "the durable revision must be the session's most recent one");
    }

    // The revision survives the session and is readable through a fresh open, whose admission also
    // runs the repair the cleanup failure flagged.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(1, session.getMostRecentRevisionNumber());
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        assertEquals(1, rtx.getChildCount(), "the committed object must still hold its single field");
      }
    }
  }

  /**
   * The contrast: a storage failure before the beacon is written commits nothing, and the transaction
   * keeps the rollback path that makes it usable again. {@code spec: TXN-305}
   *
   * <p>
   * TXN-305 asks for a usable writer, but for a failure this late it asks for the rollback path
   * rather than immediate reuse: page serialization already retired the epoch's buffers, so the
   * mutation path is only safe again once rollback has reconstructed the lineage. What this asserts
   * is the contrast that the post-durable branch must not reach: the ordinary failure is thrown, no
   * revision number is consumed, the work is still counted as dirty, and the writer is not terminal.
   */
  @Test
  void failureBeforeTheCommitPointCommitsNothingAndKeepsTheTransactionUsable() {
    final Path databasePath = createDatabase();

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final int bootstrapRevision = session.getMostRecentRevisionNumber();

      final JsonNodeTrx wtx = session.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);

      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      injector.injectAt(Point.BEFORE_UBER_PAGE_REFERENCE_WRITE, 1);

      assertThrows(SimulatedCrashException.class, wtx::commit);

      assertFalse(injector.isArmedFor(Point.BEFORE_UBER_PAGE_REFERENCE_WRITE),
          "the pre-beacon injection never fired, so this run proves nothing");

      assertEquals(bootstrapRevision, session.getMostRecentRevisionNumber(), "nothing may be durable");

      // The work still counts as uncommitted: only a commit that crossed the commit point clears
      // the dirty counter, so the post-durable branch cannot have been taken here.
      assertThrows(SirixUsageException.class, wtx::close);

      // And the writer is not terminal — it still accepts the rollback TXN-305 preserves.
      wtx.rollback();
      wtx.close();

      assertEquals(bootstrapRevision, session.getMostRecentRevisionNumber());
    }
  }

  private Path createDatabase() {
    final Path databasePath = workRoot.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
    }
    return databasePath;
  }

  private static void deleteRecursive(final Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (final Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (final IOException ignored) {
          // Best effort: a leftover temporary file must not fail the test.
        }
      });
    }
  }
}
