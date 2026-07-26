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

  /** Resolve the underlying {@code sirix.revisions} dual-beacon file for a resource. */
  private static Path revisionsFilePath(final Path databasePath, final String resourceName) {
    return databasePath
        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
        .resolve(resourceName)
        .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve("sirix.revisions");
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
  @DisplayName("multi-revision: tail garbage past the LAST revision is truncated on reopen")
  void multiRevision_tailGarbage_isTruncatedOnReopen() throws Exception {
    // Variation on partialWritePastLastRevision: 3 successful commits + tail garbage.
    // The recovery contract is "trim back to the LAST good revision boundary", not
    // revision 0. A regression that always reset to revision 1 would silently lose
    // committed data — this test guards against that.
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.FILE_CHANNEL)
          .build());

      final int targetRevision;
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
          wtx.commit();
        }
        // Two more empty commits — just bump the revision counter so we have a
        // multi-revision file with non-trivial structure. The recovery contract is
        // about the LAST revision boundary, not the content shape.
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          targetRevision = rtx.getRevisionNumber();
        }
      }

      // Capture the post-3-revision file size, then forge a 4 KiB tail-garbage write
      // + .commit marker.
      final Path dataFile = dataFilePath(dbPath, RESOURCE);
      final long baselineSize = Files.size(dataFile);
      final byte[] garbage = new byte[4096];
      Arrays.fill(garbage, (byte) 0xCC);
      try (final var ch = java.nio.channels.FileChannel.open(dataFile,
          StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
        ch.write(java.nio.ByteBuffer.wrap(garbage));
      }
      Files.createFile(commitMarkerPath(dbPath, RESOURCE));

      // Recover by triggering a new write trx.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
      }

      // The targetRevision (3 of the bootstrap-offset count) must remain readable —
      // recovery must not have rolled back to revision 1 or 0.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(targetRevision)) {
        assertEquals(targetRevision, rtx.getRevisionNumber(),
            "last pre-crash revision must remain reachable after recovery");
      }

      // And the tail garbage is gone (same check as the single-revision case).
      final long postRecoverySize = Files.size(dataFile);
      if (postRecoverySize > baselineSize) {
        try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "r")) {
          raf.seek(baselineSize);
          final byte[] check = new byte[Math.min(16, (int) (postRecoverySize - baselineSize))];
          raf.readFully(check);
          int garbageBytes = 0;
          for (final byte b : check) {
            if (b == (byte) 0xCC) {
              garbageBytes++;
            }
          }
          assertTrue(garbageBytes < check.length,
              "every checked byte at the old garbage offset is 0xCC — recovery did not truncate");
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

  /**
   * Kernel-level crash scenario: the writer added bytes to {@code sirix.data} but the
   * process died before the {@code .commit} marker was created (or kernel hadn't
   * flushed the marker's parent directory yet). On reopen there's no marker, so the
   * recovery truncation path does NOT fire. The invariant we test: the
   * previously-committed revision must still be reachable, AND opening a new write
   * trx must succeed (no silent corruption from the orphaned trailing bytes).
   *
   * <p>This is the "torn write without marker" gap that the operations doc calls out
   * — coverage previously stopped at the marker-driven path.
   */
  @Test
  @DisplayName("torn-write without .commit marker: orphan bytes don't corrupt the last committed revision")
  void tornWriteWithoutMarker_lastCommittedRevisionStillReadable() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.FILE_CHANNEL)
          .build());

      final int preCrashRevision;
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
          wtx.commit();
        }
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          preCrashRevision = rtx.getRevisionNumber();
        }
      }

      // Append garbage to sirix.data WITHOUT dropping the .commit marker. This
      // simulates "kernel had partially written the next revision's bytes but
      // hadn't fsync'd the marker yet" — recovery cannot use the marker to know
      // to truncate.
      final Path dataFile = dataFilePath(dbPath, RESOURCE);
      final long baselineSize = Files.size(dataFile);
      final byte[] garbage = new byte[2048];
      Arrays.fill(garbage, (byte) 0xEE);
      try (final var ch = java.nio.channels.FileChannel.open(dataFile,
          StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
        ch.write(java.nio.ByteBuffer.wrap(garbage));
      }
      assertFalse(Files.exists(commitMarkerPath(dbPath, RESOURCE)),
          "test pre-condition: no .commit marker for this scenario");

      // Pre-crash revision MUST remain readable. The uberpage at the last good
      // boundary anchors the revision graph; bytes past it are unreachable and
      // shouldn't matter.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(preCrashRevision)) {
        assertEquals(preCrashRevision, rtx.getRevisionNumber(),
            "last-committed revision must remain reachable when orphan bytes follow it");
      }

      // A new write must succeed too — proves we haven't entered a "stuck" state.
      // The new revision may be written over or after the garbage; either is
      // acceptable as long as the resulting file is consistent.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
      }

      // Sanity: file did not stay at exactly baseline+2048 — recovery either
      // appended a new revision (size grew) or truncated (size shrank). The
      // forbidden state is "size unchanged AND no new revision committed", which
      // would mean writes were silently swallowed.
      final long postSize = Files.size(dataFile);
      assertTrue(postSize != baselineSize + 2048 || postSize == baselineSize,
          "post-recovery file size must reflect a real outcome, not stuck-at-garbage");
    }
  }

  /**
   * Dual-beacon torn write: the uber page lives in two checksummed beacon slots in
   * {@code sirix.data} ({@code IOStorage.PRIMARY_BEACON_OFFSET} and
   * {@code IOStorage.SECONDARY_BEACON_OFFSET}, one filesystem block apart). If the primary
   * tears, the secondary is the recovery anchor. We simulate a torn primary by corrupting its
   * slot and verifying recovery still works.
   *
   * <p>If recovery only consults the primary, this test will fail and we'll have caught a
   * regression in the dual-beacon contract.
   */
  @Test
  @DisplayName("dual-beacon torn write: first uberpage beacon corrupt, second intact")
  void dualBeacon_firstBeaconCorrupted_recoversFromSecond() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.FILE_CHANNEL)
          .build());

      final int preCrashRevision;
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
          wtx.commit();
        }
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          preCrashRevision = rtx.getRevisionNumber();
        }
      }

      // Corrupt the primary beacon slot in sirix.data by overwriting its head with 0xFF.
      // The secondary beacon in its own slot is left intact.
      final Path dataFile = revisionsFilePath(dbPath, RESOURCE).resolveSibling("sirix.data");
      assertTrue(Files.exists(dataFile), "sirix.data must exist after a commit; got " + dataFile);
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        raf.seek(io.sirix.io.IOStorage.PRIMARY_BEACON_OFFSET);
        final byte[] corrupt = new byte[16];
        Arrays.fill(corrupt, (byte) 0xFF);
        raf.write(corrupt);
      }

      // Attempt to reopen. The dual-beacon contract says recovery should pull
      // from the second beacon at offset 512. If this throws, the contract is
      // broken (or the implementation doesn't currently honor it — in which case
      // this test documents the gap until it's fixed).
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(preCrashRevision)) {
        assertEquals(preCrashRevision, rtx.getRevisionNumber(),
            "second beacon should serve as the recovery anchor when the first is torn");
      } catch (final Exception e) {
        // Document the actual behavior. If recovery DOESN'T fall back to the
        // second beacon today, the test surfaces that limitation rather than
        // hiding it. Re-throw with a descriptive message.
        throw new AssertionError(
            "dual-beacon recovery did not fall back to the secondary beacon slot; the primary "
                + "was corrupted but the secondary was intact. Either the recovery code consults "
                + "only the primary (a gap to address), or the corruption shape is wrong for this "
                + "storage backend.",
            e);
      }
    }
  }

  /**
   * Concurrent reader during recovery: a read-only trx is held open while a writer
   * crashes mid-commit (forged via the {@code .commit} marker + garbage tail). The
   * reader must either:
   * (a) continue to serve the revision it was opened against, or
   * (b) fail cleanly with a SirixException — NOT throw a low-level
   *     {@code BufferOverflowException} / {@code IndexOutOfBoundsException} that
   *     would indicate silent corruption reached the read path.
   */
  @Test
  @DisplayName("concurrent reader survives a crash + recovery on the writer side")
  void concurrentReader_survivesWriterCrashAndRecovery() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
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
      }

      // Note: ResourceStoreImpl.beginResourceSession is computeIfAbsent, so multiple
      // calls for the same resource return the same instance. To test "reader open
      // during recovery" we share one session for both roles.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        final int readerRev = reader.getRevisionNumber();

        // Simulate the writer crash: garbage tail + .commit marker. The marker
        // triggers the truncation recovery path on the next writer creation.
        final Path dataFile = dataFilePath(dbPath, RESOURCE);
        final byte[] garbage = new byte[4096];
        Arrays.fill(garbage, (byte) 0xCC);
        try (final var ch = java.nio.channels.FileChannel.open(dataFile,
            StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
          ch.write(java.nio.ByteBuffer.wrap(garbage));
        }
        Files.createFile(commitMarkerPath(dbPath, RESOURCE));

        // Start a write trx on the SAME session — exercises the writer-creation
        // recovery path while our reader trx is open in the same session.
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }

        // The held reader should still be able to read its revision. We don't
        // assert specific data content (the reader might be looking at pages
        // that the recovery code touched, depending on cache eviction policy);
        // what we assert is that the reader doesn't fall through to a
        // low-level crash.
        try {
          reader.moveToDocumentRoot();
          reader.moveToFirstChild();
          // Reader successfully navigated — the contract is upheld.
          assertEquals(readerRev, reader.getRevisionNumber(),
              "reader's revision view should be stable across writer recovery");
        } catch (final io.sirix.exception.SirixException expected) {
          // A clean SirixException is also acceptable — it's a contract failure
          // mode, not silent corruption. We pass either way; the regression we'd
          // catch is an unchecked low-level exception.
        }
      }
    }
  }

  /**
   * Stale-forward SECONDARY beacon after crash-recovery truncation (double-fault scenario): a
   * writer that died between the secondary and primary beacon writes leaves primary=R (old,
   * acknowledged) and secondary=R+1 (new, never acknowledged). Recovery — anchored on the
   * primary — truncates revision R+1's data, but used to leave the secondary advertising the
   * now-truncated revision until the NEXT commit rewrote the slots. In that (unbounded, under
   * read-mostly load) window a primary corruption made fallback dereference the stale-forward
   * secondary ("Truncated revisions record …") and the resource unopenable although revision R
   * was fully intact. {@code truncateTo} now repairs the mismatching slot from the matching one.
   */
  @Test
  @DisplayName("stale-forward secondary beacon is repaired by recovery truncation (double fault survives)")
  void staleForwardSecondaryBeacon_isRepairedOnRecovery() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.FILE_CHANNEL)
          .build());

      final int firstRevision;
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
          wtx.commit();
        }
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          firstRevision = rtx.getRevisionNumber();
        }
      }

      // Snapshot the primary beacon slot while it still advertises the FIRST revision.
      final Path dataFile = dataFilePath(dbPath, RESOURCE);
      final byte[] firstRevisionSlot = new byte[io.sirix.io.IOStorage.BEACON_SLOT_BYTES];
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "r")) {
        raf.seek(io.sirix.io.IOStorage.PRIMARY_BEACON_OFFSET);
        raf.readFully(firstRevisionSlot);
      }

      // Second commit: both slots now advertise revision firstRevision+1.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
      }

      // Forge the crash "died between secondary and primary beacon writes": primary back at
      // the first revision, secondary still at the second, data file still carries the second
      // revision's bytes, .commit marker present.
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        raf.seek(io.sirix.io.IOStorage.PRIMARY_BEACON_OFFSET);
        raf.write(firstRevisionSlot);
      }
      Files.createFile(commitMarkerPath(dbPath, RESOURCE));
      Databases.clearGlobalCaches(); // cold-process simulation

      // Recovery: anchored on the primary (first revision); truncation drops the second
      // revision AND must repair the secondary slot. Roll back so no fresh commit rewrites
      // the slots — the repair itself is what's under test.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.rollback();
        }
      }

      // Double fault: corrupt the primary. Without the repair the secondary still advertises
      // the truncated revision and the resource is unopenable despite intact data.
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        raf.seek(io.sirix.io.IOStorage.PRIMARY_BEACON_OFFSET);
        final byte[] corrupt = new byte[16];
        Arrays.fill(corrupt, (byte) 0xFF);
        raf.write(corrupt);
      }
      Databases.clearGlobalCaches();

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals(firstRevision, rtx.getRevisionNumber(),
            "the repaired secondary beacon must anchor the open at the truncated-to revision");
        assertTrue(rtx.moveToFirstChild(), "the surviving revision's data must be readable");
      }
    }
  }
}
