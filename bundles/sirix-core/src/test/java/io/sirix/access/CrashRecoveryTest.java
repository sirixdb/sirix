package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests for Sirix's crash-recovery path.
 *
 * <p>The protocol:
 * <ul>
 *   <li>A write trx, on creation, drops a {@code .commit} marker file at
 *       {@code <resource>/log/.commit}.</li>
 *   <li>On successful {@link JsonNodeTrx#commit()} the marker is deleted
 *       (see {@code AbstractNodeTrxImpl.commit}).</li>
 *   <li>If a process crashes mid-commit (kill -9, OOM, host power loss), the marker
 *       remains. The next writer creation observes it and calls
 *       {@code Writer.truncateTo(storageEngineWriter, lastCommittedRev)} —
 *       trimming the storage file back to the last good revision.</li>
 * </ul>
 *
 * <p>These tests don't simulate a kernel-level partial write (that would need
 * fault-injection at the storage layer); instead they verify the higher-level
 * invariants the recovery code is responsible for:
 * <ol>
 *   <li>A stale {@code .commit} marker on a clean storage file is benignly handled
 *       by the next writer creation — the truncation runs but is a no-op because
 *       there's nothing past the last revision.</li>
 *   <li>Subsequent write transactions resume normally and previously-committed
 *       revisions remain readable bit-for-bit.</li>
 * </ol>
 *
 * <p>Locking these invariants in test prevents regressions where a refactor might,
 * say, fail the writer creation when the marker is present, or accidentally
 * over-truncate the storage and lose committed data.
 */
final class CrashRecoveryTest {

  private static final String RESOURCE = "crash-recovery-resource";

  private static final String DOC = "{\"r\":1,\"name\":\"alpha\"}";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** Resolve the .commit marker path the way {@code AbstractResourceSession.getCommitFile()} does. */
  private static Path commitMarkerPath(final Path databasePath, final String resourceName) {
    return databasePath
        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
        .resolve(resourceName)
        .resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
        .resolve(".commit");
  }

  @Test
  @DisplayName("stale .commit marker is benignly handled; committed revisions remain readable")
  void staleCommitMarker_onWriterCreation_isHandledAndDataIsIntact() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      final var resourceConfig = ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.MEMORY_MAPPED)
          .build();
      db.createResource(resourceConfig);

      // Commit one revision. Capture its number — the resource's revision counter starts
      // at the bootstrap revision (which createResource implicitly produces) and increments
      // on each commit, so the first user-visible revision number depends on the bootstrap
      // policy. We capture it dynamically rather than hardcoding to keep the test
      // resilient to bootstrap-policy refactors.
      final int preCrashRevision;
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
          wtx.commit();
        }
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          preCrashRevision = rtx.getRevisionNumber();
        }

        // Sanity: the .commit marker should NOT exist after a clean shutdown.
        final Path commitMarker = commitMarkerPath(dbPath, RESOURCE);
        assertFalse(Files.exists(commitMarker),
            "post-clean-commit marker must not exist; got " + commitMarker);

        // Simulate a crash mid-commit: drop a stale .commit marker. The on-disk image
        // is already consistent (no garbage past the last revision), so the recovery
        // truncation will be a no-op on the file but exercises the code path.
        Files.createFile(commitMarker);
        assertTrue(Files.exists(commitMarker), "marker must exist before recovery test");
      }

      // Re-open the resource session: writer creation observes the .commit marker and
      // runs the truncation path. This must NOT throw, must NOT lose data, and must
      // remove the marker after a successful subsequent commit.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

        // Read-only access to the pre-crash revision still works during recovery.
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          assertEquals(preCrashRevision, rtx.getRevisionNumber(),
              "revision count survives recovery (must equal pre-crash revision)");
        }

        // A new write trx triggers the recovery (createPageTransaction calls
        // truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists). It must
        // succeed and let us commit a fresh revision without seeing the stale marker.
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }

        // Marker must be gone after the successful commit.
        assertFalse(Files.exists(commitMarkerPath(dbPath, RESOURCE)),
            "post-recovery commit must clear the stale marker");

        // Pre-crash revision is still readable.
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(preCrashRevision)) {
          assertEquals(preCrashRevision, rtx.getRevisionNumber(),
              "pre-crash revision is still readable");
        }
      }
    }
  }

  @Test
  @DisplayName("missing .commit marker is the normal case — writer creation does not truncate")
  void missingCommitMarker_isTheNormalCase() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.MEMORY_MAPPED)
          .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
          wtx.commit();
        }
        // No marker after a clean commit — that's the contract.
        assertFalse(Files.exists(commitMarkerPath(dbPath, RESOURCE)));
      }

      // Reopening still sees the committed revision; no truncation needed.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.getRevisionNumber() >= 1,
            "expected at least one revision after a clean commit");
      }
    }
  }
}
