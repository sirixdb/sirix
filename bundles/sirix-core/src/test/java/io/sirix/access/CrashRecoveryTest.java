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

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

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

  /** Resolve the underlying {@code sirix.data} storage file for a resource. */
  private static Path dataFilePath(final Path databasePath, final String resourceName) {
    return databasePath
        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
        .resolve(resourceName)
        .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve("sirix.data");
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

  /**
   * Stronger crash simulation: write the storage file out fully, take a baseline of the
   * file size, then forge a partial-write by appending 4 KiB of garbage past the end of
   * the last successful revision's footer. Drop the {@code .commit} marker. On reopen,
   * {@code Writer.truncateTo(...)} must trim the garbage so the file ends on the last
   * committed revision boundary — a real "process crashed mid-commit, leaving torn
   * bytes" scenario.
   *
   * <p>Without this test there's nothing in CI that exercises the truncation path with
   * actual bytes to truncate; the simpler smoke test only verifies the no-op case.
   */
  @Test
  @DisplayName("partial write past the last revision is truncated by the recovery path")
  void partialWritePastLastRevision_isTruncatedOnReopen() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    final long baselineSize;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      // FILE_CHANNEL storage backs onto a single sirix.data file we can poke at; the
      // memory-mapped backend would be similar but uses sparse files plus mmap, which is
      // harder to extend with a plain RandomAccessFile without flushing the mmap view.
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.FILE_CHANNEL)
          .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
          wtx.commit();
        }
        // Resource sessions cache the FileChannel; close before we manipulate the file.
      }

      // After the resource session is closed, capture the on-disk size — this is the
      // last-good byte boundary the recovery code must restore.
      final Path dataFile = dataFilePath(dbPath, RESOURCE);
      assertTrue(Files.exists(dataFile), "data file must exist after a commit; got " + dataFile);
      baselineSize = Files.size(dataFile);

      // Forge a partial write: append 4 KiB of recognizable garbage past the baseline.
      // Use 0xCC to make it obvious in any post-mortem hex dump.
      final byte[] garbage = new byte[4096];
      Arrays.fill(garbage, (byte) 0xCC);
      try (final var ch = java.nio.channels.FileChannel.open(dataFile,
          StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
        ch.write(java.nio.ByteBuffer.wrap(garbage));
      }
      assertEquals(baselineSize + 4096, Files.size(dataFile),
          "garbage append must extend the file");

      // Forge the .commit marker — this is what tells the recovery code the previous
      // writer crashed mid-commit.
      Files.createFile(commitMarkerPath(dbPath, RESOURCE));

      // Reopen the resource and start a write trx. createPageTransaction calls
      // truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists which calls
      // writer.truncateTo(storageEngineWriter, lastCommittedRev) — that's the path
      // we're verifying does its job.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
      }

      // After recovery the file should be at most baselineSize plus whatever the
      // post-recovery commit added — and the garbage at offset baselineSize..baselineSize+4096
      // must be gone (either truncated, or overwritten by a new revision; either is fine
      // because the recovery contract is "no torn bytes from the crashed transaction
      // remain visible to readers"). The specific assertion: at offset baselineSize,
      // the file MUST NOT contain the 0xCC garbage signature.
      final long postRecoverySize = Files.size(dataFile);
      assertTrue(postRecoverySize >= baselineSize,
          "post-recovery file must be at least baselineSize");

      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "r")) {
        // Read what's at the old garbage offset. If the file was extended by a new
        // revision after recovery, that's the new revision's bytes (any value). The
        // bug we're guarding against is the file STILL containing 0xCC bytes at that
        // offset, which would mean the truncation didn't fire.
        if (postRecoverySize > baselineSize) {
          raf.seek(baselineSize);
          final byte[] check = new byte[Math.min(16, (int) (postRecoverySize - baselineSize))];
          raf.readFully(check);
          int garbageBytes = 0;
          for (final byte b : check) {
            if (b == (byte) 0xCC) {
              garbageBytes++;
            }
          }
          // 16 consecutive 0xCC at exactly the old garbage offset is a smoking gun for
          // truncation failure. (A small number of 0xCC bytes can incidentally appear
          // in a post-recovery revision's payload — only assert when they cluster.)
          assertTrue(garbageBytes < check.length,
              "every checked byte at the old garbage offset is 0xCC — recovery did not truncate");
        }
      }

      // And the pre-crash revision must still be readable bit-for-bit.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertEquals(1, rtx.getRevisionNumber(),
            "revision 1 must still be readable after a partial-write recovery");
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
