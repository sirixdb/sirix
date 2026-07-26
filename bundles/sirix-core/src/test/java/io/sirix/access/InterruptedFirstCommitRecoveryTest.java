package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.utils.OS;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.io.Superblock;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Crash-recovery for an interrupted FIRST commit (layout-F5 gap).
 *
 * <p>The superblock and the two uber-page beacon slots of {@code sirix.data} are only written at
 * the END of a resource's first commit (by {@code writeUberPageReference}); until then the header
 * region {@code [0, DATA_REGION_START)} is a sparse hole while the buffered page writer may
 * already have flushed &ge; 64 KiB of data pages past {@code DATA_REGION_START}. A process crash
 * in that window leaves a NON-EMPTY {@code sirix.data} whose header is all zeros (or, crashing a
 * little later, a valid superblock with still-zero beacon slots). {@code IOStorage.exists()} is
 * size-based, so the next open takes the "load" path and fails on the superblock/beacon
 * validation — permanently, even though NOTHING was ever committed and a fresh bootstrap is
 * provably safe.
 *
 * <p>The recovery contract under test: on an open failure, the resource is re-initialized empty
 * if and only if the on-disk state PROVES no commit ever completed — all-zero (or
 * valid-but-beaconless) header AND no checksum-valid record in {@code sirix.revisions}. Any
 * nonzero beacon bytes or any checksum-valid revision record means data may exist, and the open
 * must keep failing with the existing actionable error (no destructive re-bootstrap).
 *
 * <p>Fabrication note: {@code createResource} itself runs the bootstrap commit (revision 0), so a
 * freshly created resource already has a fully committed file pair. The tests therefore fabricate
 * the crashed-first-commit image explicitly: truncate both files back to the pre-first-commit
 * state (size 0) and re-create exactly the bytes such a crash leaves behind.
 */
final class InterruptedFirstCommitRecoveryTest {

  private static final String RESOURCE = "interrupted-first-commit-resource";

  private static final String DOC = "{\"r\":1,\"name\":\"alpha\"}";

  /**
   * More than {@code Writer.FLUSH_SIZE} (64_000): the gap's trigger is the buffered writer having
   * flushed at least one buffer of uncommitted pages before the crash.
   */
  private static final int GARBAGE_TAIL_BYTES = 128 * 1024;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** Resolve the underlying {@code sirix.data} storage file for a resource. */
  private static Path dataFilePath(final Path databasePath, final String resourceName) {
    return databasePath
        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
        .resolve(resourceName)
        .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve("sirix.data");
  }

  /** Resolve the underlying {@code sirix.revisions} file for a resource. */
  private static Path revisionsFilePath(final Path databasePath, final String resourceName) {
    return databasePath
        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
        .resolve(resourceName)
        .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve("sirix.revisions");
  }

  private static void createDbAndResource(final Path dbPath, final StorageType storageType) {
    // Windows cannot truncate or replace a file while memory mappings may still be live (mapped
    // files are hard-locked), so interrupted-first-commit recovery's in-place re-initialization
    // is unsupported for the MEMORY_MAPPED backend there — see docs/KNOWN_LIMITATIONS.md. The
    // FILE_CHANNEL cases cover the recovery logic itself on Windows.
    assumeFalse(OS.isWindows() && storageType == StorageType.MEMORY_MAPPED,
        "MEMORY_MAPPED recovery re-initialization is unsupported on Windows");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(storageType)
          .build());
    }
  }

  /**
   * Fabricates the on-disk image of a process that died during the resource's FIRST commit after
   * the buffered writer flushed pages past {@code DATA_REGION_START}: both files truncated to the
   * pre-first-commit state, then a &gt; 64 KiB garbage tail of "flushed-but-uncommitted pages" at
   * {@code DATA_REGION_START} — leaving the header region {@code [0, DATA_REGION_START)} a sparse
   * zero hole (optionally with a real superblock at offset 0, the slightly-later crash point
   * between the superblock write and the beacon writes).
   */
  private static void fabricateInterruptedFirstCommit(final Path dbPath, final boolean withValidSuperblock)
      throws IOException {
    final Path dataFile = dataFilePath(dbPath, RESOURCE);
    final Path revisionsFile = revisionsFilePath(dbPath, RESOURCE);

    try (final FileChannel ch = FileChannel.open(dataFile, StandardOpenOption.WRITE, StandardOpenOption.SPARSE)) {
      ch.truncate(0);
      if (withValidSuperblock) {
        final ByteBuffer superblock = Superblock.build(Superblock.ROLE_DATA);
        while (superblock.hasRemaining()) {
          ch.write(superblock, superblock.position());
        }
      }
      final byte[] garbage = new byte[GARBAGE_TAIL_BYTES];
      Arrays.fill(garbage, (byte) 0xAB);
      final ByteBuffer tail = ByteBuffer.wrap(garbage);
      while (tail.hasRemaining()) {
        ch.write(tail, IOStorage.DATA_REGION_START + tail.position());
      }
    }
    // The revisions file of a first commit that died before the RevisionRootPage was serialized
    // holds no record at all.
    try (final FileChannel ch = FileChannel.open(revisionsFile, StandardOpenOption.WRITE)) {
      ch.truncate(0);
    }
  }

  private static String messageChain(final Throwable throwable) {
    final StringBuilder messages = new StringBuilder();
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
      for (final Throwable suppressed : t.getSuppressed()) {
        messages.append(suppressed.getMessage()).append('\n');
      }
    }
    return messages.toString();
  }

  /** Open the healed resource, verify it is empty, commit {@link #DOC} and read it back. */
  private static void assertOpensEmptyAcceptsCommitAndReadsBack(final Path dbPath) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      // Re-initialized empty: the session is in the fresh-bootstrap state (revision counter at
      // the bootstrap revision, exactly like a resource whose first commit never started).
      assertEquals(0, session.getMostRecentRevisionNumber(),
          "re-bootstrapped resource must be at the bootstrap revision");

      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
        wtx.commit();
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "committed document must be readable after the heal");
        assertEquals(2, rtx.getChildCount(), "committed object must hold both keys of " + DOC);
      }
    }

    // Cold re-open: the re-initialized files must be durable and self-consistent on disk.
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = db.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild(), "committed data must survive a cold reopen");
      assertEquals(2, rtx.getChildCount());
    }
  }

  @ParameterizedTest(name = "storageType={0}")
  @EnumSource(value = StorageType.class, names = { "FILE_CHANNEL", "MEMORY_MAPPED" })
  @DisplayName("interrupted first commit (all-zero header + flushed garbage tail) re-bootstraps empty")
  void zeroHeaderWithGarbageTail_isRebootstrappedEmpty(final StorageType storageType) throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    createDbAndResource(dbPath, storageType);

    fabricateInterruptedFirstCommit(dbPath, false);
    Databases.clearGlobalCaches(); // cold-process simulation

    assertOpensEmptyAcceptsCommitAndReadsBack(dbPath);
  }

  @ParameterizedTest(name = "storageType={0}")
  @EnumSource(value = StorageType.class, names = { "FILE_CHANNEL", "MEMORY_MAPPED" })
  @DisplayName("interrupted first commit (valid superblock, beacons never written) re-bootstraps empty")
  void validSuperblockZeroBeacons_isRebootstrappedEmpty(final StorageType storageType) throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    createDbAndResource(dbPath, storageType);

    fabricateInterruptedFirstCommit(dbPath, true);
    Databases.clearGlobalCaches(); // cold-process simulation

    assertOpensEmptyAcceptsCommitAndReadsBack(dbPath);
  }

  /**
   * Variant two of the gap: resource creation's bootstrap commit COMPLETED (checksum-valid
   * beacons advertising the empty revision 0), but the just-created {@code sirix.revisions} was
   * lost/truncated by the environment (directory entry dropped on power cut, filesystem repair)
   * before anything else committed. The open then fails with "Truncated revisions record for
   * revision 0" although the only acknowledged revision is the empty bootstrap one and nothing
   * on disk is reachable — must self-heal exactly like the zero-header variant.
   */
  @ParameterizedTest(name = "storageType={0}, revisionsFileDamage={1}")
  @MethodSource("revisionsFileDamageVariants")
  @DisplayName("bootstrap-only resource with truncated/empty/partial revisions file re-bootstraps empty")
  void truncatedRevisionsRecordAtRevisionZero_isRebootstrappedEmpty(final StorageType storageType,
      final String damage, final long truncateToSize) throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    createDbAndResource(dbPath, storageType);

    truncate(revisionsFilePath(dbPath, RESOURCE), truncateToSize);
    Databases.clearGlobalCaches(); // cold-process simulation

    assertOpensEmptyAcceptsCommitAndReadsBack(dbPath);
  }

  private static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> revisionsFileDamageVariants() {
    final long recordStart = IOStorage.REVISIONS_RECORDS_START;
    return java.util.stream.Stream.of(StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED)
        .flatMap(storageType -> java.util.stream.Stream.of(
            org.junit.jupiter.params.provider.Arguments.of(storageType, "lost (size 0)", 0L),
            org.junit.jupiter.params.provider.Arguments.of(storageType, "header only (record 0 gone)", recordStart),
            org.junit.jupiter.params.provider.Arguments.of(storageType, "torn mid-record 0", recordStart + 4)));
  }

  /**
   * NEGATIVE control for variant two: a truncated record for a revision PAST the bootstrap one
   * on a resource WITH committed data is REAL CORRUPTION — earlier records are checksum-valid
   * and the beacons advertise the later revision, so data exists. The open must keep failing
   * with the existing actionable error and the files must be left untouched.
   */
  @Test
  @DisplayName("committed resource with truncated revisions record at revision N>0 fails without re-bootstrap")
  void truncatedRevisionsRecordAtLaterRevision_failsWithoutRebootstrap() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    createDbAndResource(dbPath, StorageType.FILE_CHANNEL);

    // One real commit beyond the bootstrap revision (data!), then cut its record off.
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
        wtx.commit();
      }
    }

    final Path dataFile = dataFilePath(dbPath, RESOURCE);
    final Path revisionsFile = revisionsFilePath(dbPath, RESOURCE);
    truncate(revisionsFile, IOStorage.revisionsFileOffset(1)); // record 0 survives, record 1 cut
    final long dataSize = Files.size(dataFile);
    final long revisionsSize = Files.size(revisionsFile);

    Databases.clearGlobalCaches(); // cold-process simulation

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      final RuntimeException failure = assertThrows(RuntimeException.class, () -> {
        try (final JsonResourceSession ignored = db.beginResourceSession(RESOURCE)) {
          // must not be reached
        }
      });
      assertTrue(messageChain(failure).contains("Truncated revisions record for revision 1"),
          "expected the existing truncated-record failure, got: " + messageChain(failure));
    }

    // NO re-bootstrap: the files (committed data!) must be byte-count identical.
    assertEquals(dataSize, Files.size(dataFile), "data file must not be truncated/re-initialized");
    assertEquals(revisionsSize, Files.size(revisionsFile), "revisions file must not be truncated further");
  }

  /**
   * NEGATIVE control for variant two: losing the WHOLE revisions file of a resource whose
   * beacons advertise a revision past the bootstrap one means committed data exists but is
   * unreachable — corruption to surface loudly, never to auto-wipe (the no-valid-record check
   * alone would pass here; the beacon-advertised-revision check is what must refuse).
   */
  @Test
  @DisplayName("committed resource with lost revisions file fails without re-bootstrap")
  void lostRevisionsFileWithCommittedData_failsWithoutRebootstrap() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    createDbAndResource(dbPath, StorageType.FILE_CHANNEL);

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
        wtx.commit();
      }
    }

    final Path dataFile = dataFilePath(dbPath, RESOURCE);
    truncate(revisionsFilePath(dbPath, RESOURCE), 0);
    final long dataSize = Files.size(dataFile);

    Databases.clearGlobalCaches(); // cold-process simulation

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      final RuntimeException failure = assertThrows(RuntimeException.class, () -> {
        try (final JsonResourceSession ignored = db.beginResourceSession(RESOURCE)) {
          // must not be reached
        }
      });
      assertTrue(messageChain(failure).contains("Truncated revisions record"),
          "expected the existing truncated-record failure, got: " + messageChain(failure));
    }

    assertEquals(dataSize, Files.size(dataFile), "data file must not be truncated/re-initialized");
  }

  private static void truncate(final Path file, final long size) throws IOException {
    try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
      ch.truncate(size);
      ch.force(true);
    }
  }

  /**
   * NEGATIVE control: a resource WITH a completed commit whose superblock sector is later lost
   * (zeroed) must NOT be re-bootstrapped — the beacon slots still hold checksum-valid uber pages
   * and the revisions file holds checksum-valid records, i.e. data exists. The open must keep
   * failing with the existing all-zeros message and the files must be left untouched.
   */
  @Test
  @DisplayName("committed resource with zeroed superblock (sector loss) fails without re-bootstrap")
  void committedResourceWithZeroedSuperblock_failsWithoutRebootstrap() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    createDbAndResource(dbPath, StorageType.FILE_CHANNEL);

    // One real commit beyond the bootstrap revision.
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
        wtx.commit();
      }
    }

    final Path dataFile = dataFilePath(dbPath, RESOURCE);
    final Path revisionsFile = revisionsFilePath(dbPath, RESOURCE);

    // Sector-loss simulation: zero ONLY the superblock block [0, PRIMARY_BEACON_OFFSET); both
    // beacon slots and all data pages stay intact.
    try (final FileChannel ch = FileChannel.open(dataFile, StandardOpenOption.WRITE)) {
      final ByteBuffer zeros = ByteBuffer.allocate((int) IOStorage.PRIMARY_BEACON_OFFSET);
      while (zeros.hasRemaining()) {
        ch.write(zeros, zeros.position());
      }
    }
    final long dataSize = Files.size(dataFile);
    final long revisionsSize = Files.size(revisionsFile);

    Databases.clearGlobalCaches(); // cold-process simulation

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      final SirixIOException failure = assertThrows(SirixIOException.class, () -> {
        try (final JsonResourceSession ignored = db.beginResourceSession(RESOURCE)) {
          // must not be reached
        }
      });
      assertTrue(messageChain(failure).contains("superblock is all zeros"),
          "expected the existing all-zeros superblock failure, got: " + messageChain(failure));
    }

    // NO re-bootstrap: the files (committed data!) must be byte-count identical.
    assertEquals(dataSize, Files.size(dataFile), "data file must not be truncated/re-initialized");
    assertEquals(revisionsSize, Files.size(revisionsFile), "revisions file must not be truncated");
  }

  /**
   * NEGATIVE control: a valid superblock with GARBAGE (nonzero, checksum-invalid) beacon slots is
   * corruption, not a provably-uncommitted first commit — the open must fail with the existing
   * beacon-corruption error and must not re-initialize anything.
   */
  @Test
  @DisplayName("valid superblock with garbage beacon bytes fails without re-bootstrap")
  void validSuperblockWithGarbageBeacons_failsWithoutRebootstrap() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    createDbAndResource(dbPath, StorageType.FILE_CHANNEL);

    fabricateInterruptedFirstCommit(dbPath, true);

    final Path dataFile = dataFilePath(dbPath, RESOURCE);
    // Overwrite both beacon slots with nonzero garbage (torn/foreign bytes).
    try (final FileChannel ch = FileChannel.open(dataFile, StandardOpenOption.WRITE)) {
      final byte[] garbage = new byte[(int) (IOStorage.DATA_REGION_START - IOStorage.PRIMARY_BEACON_OFFSET)];
      Arrays.fill(garbage, (byte) 0xFF);
      final ByteBuffer buffer = ByteBuffer.wrap(garbage);
      while (buffer.hasRemaining()) {
        ch.write(buffer, IOStorage.PRIMARY_BEACON_OFFSET + buffer.position());
      }
    }
    final long dataSize = Files.size(dataFile);

    Databases.clearGlobalCaches(); // cold-process simulation

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      final SirixIOException failure = assertThrows(SirixIOException.class, () -> {
        try (final JsonResourceSession ignored = db.beginResourceSession(RESOURCE)) {
          // must not be reached
        }
      });
      assertTrue(messageChain(failure).contains("beacon"),
          "expected the existing beacon-corruption failure, got: " + messageChain(failure));
    }

    assertEquals(dataSize, Files.size(dataFile), "data file must not be truncated/re-initialized");
  }
}
