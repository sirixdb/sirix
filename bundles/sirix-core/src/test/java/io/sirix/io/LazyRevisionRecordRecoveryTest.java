package io.sirix.io;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Durability contract of the LAZY REVISION RECORD commit profile
 * ({@code -Dsirix.commit.lazyRevisionRecord}, on by default with preallocated commits).
 *
 * <p>Under that profile the {@code sirix.revisions} channel is opened BUFFERED and the per-commit
 * 32-byte record is no longer made durable by its own {@code force}. Instead the record rides a
 * 16-entry ring embedded in the trailing pad of BOTH uber-beacon slots: the ring is written before
 * the write-ahead {@code fdatasync} that already hardens the data file, so a committed revision's
 * record is durable the moment its beacon is. That removes one of the three device round-trips per
 * commit (3 -&gt; 2) without weakening crash recovery.
 *
 * <p>The invariants under test:
 * <ol>
 *   <li>every committed revision's record is present AND checksum-valid in both beacon rings;</li>
 *   <li>a record lost from the revisions file inside the ring window is salvaged transparently and
 *       the file is self-healed in place, so the next open needs no salvage;</li>
 *   <li>a record lost OUTSIDE the ring window fails loudly rather than silently serving garbage;</li>
 *   <li>records about to be EVICTED from the ring are forced to the revisions file first, so the
 *       ring going away entirely never costs a committed revision;</li>
 *   <li>a torn/zeroed ring entry never masquerades as a valid record.</li>
 * </ol>
 */
final class LazyRevisionRecordRecoveryTest {

  private static final String RESOURCE = "lazy-revision-record-resource";

  private static final int CAPACITY = IOStorage.REVISION_RECORD_TAIL_LOG_CAPACITY;

  @BeforeEach
  void setUp() {
    // The whole contract only exists under the lazy profile; a run that disabled it via system
    // property gets the legacy per-commit force and nothing here applies.
    assumeTrue(IOStorage.lazyRevisionRecordsEnabled(), "lazy revision records are disabled");
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.clearGlobalCaches();
  }

  // ----------------------------------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------------------------------

  private static Path resourcePath(final Path dbPath) {
    return dbPath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(RESOURCE);
  }

  private static Path dataFilePath(final Path dbPath) {
    return resourcePath(dbPath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                               .resolve(IOStorage.FILENAME);
  }

  private static Path revisionsFilePath(final Path dbPath) {
    return resourcePath(dbPath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                               .resolve(IOStorage.REVISIONS_FILENAME);
  }

  /**
   * Create the resource and commit {@code commits} revisions (one distinct object record each, so
   * every revision is genuinely different). The database is closed on return and the global caches
   * are dropped, so subsequent reads have to go to disk — a warm cache would mask the very damage
   * these tests inject.
   *
   * @return the last user-visible revision number
   */
  private static int createResourceWithRevisions(final Path dbPath, final int commits) {
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    int lastRevision;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        for (int i = 0; i < commits; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            if (i == 0) {
              wtx.insertObjectAsFirstChild();
            } else {
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild();
            }
            wtx.insertObjectRecordAsFirstChild("k" + i, new StringValue("v" + i));
            wtx.commit();
          }
        }
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          lastRevision = rtx.getRevisionNumber();
        }
      }
    }
    Databases.clearGlobalCaches();
    return lastRevision;
  }

  /** Open every revision {@code 1..last} through the full session path. */
  private static void assertAllRevisionsReadable(final Path dbPath, final int last) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      for (int revision = 1; revision <= last; revision++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals(revision, rtx.getRevisionNumber(), "time-travel to revision " + revision);
          assertTrue(rtx.moveToFirstChild(), "revision " + revision + " must be navigable");
        }
      }
    }
  }

  /** The raw 32 bytes of a revision record in {@code sirix.revisions}. */
  private static byte[] readRecordBytes(final Path dbPath, final int revision) throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE)
                                        .order(ByteOrder.LITTLE_ENDIAN);
    try (final FileChannel channel = FileChannel.open(revisionsFilePath(dbPath), StandardOpenOption.READ)) {
      long position = IOStorage.revisionsFileOffset(revision);
      while (buffer.hasRemaining()) {
        final int read = channel.read(buffer, position);
        if (read <= 0) {
          break;
        }
        position += read;
      }
    }
    return buffer.array();
  }

  /** Zero a revision record in {@code sirix.revisions}, leaving the beacon rings untouched. */
  private static void wipeRecord(final Path dbPath, final int revision) throws Exception {
    final ByteBuffer zeros = ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE);
    try (final FileChannel channel = FileChannel.open(revisionsFilePath(dbPath), StandardOpenOption.WRITE)) {
      channel.write(zeros, IOStorage.revisionsFileOffset(revision));
    }
  }

  /** Zero the ENTIRE tail-log ring in both uber-beacon slots. */
  private static void wipeWholeTailLog(final Path dbPath) throws Exception {
    try (final FileChannel channel = FileChannel.open(dataFilePath(dbPath), StandardOpenOption.WRITE)) {
      for (final long slotOffset : new long[] {IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET}) {
        final ByteBuffer zeros = ByteBuffer.allocate(IOStorage.REVISION_RECORD_TAIL_LOG_BYTES);
        channel.write(zeros, slotOffset + IOStorage.REVISION_RECORD_TAIL_LOG_SLOT_OFFSET);
      }
    }
  }

  /**
   * Assert that opening the given revision fails as UNSALVAGEABLE. The failure surfaces as a
   * {@link SirixIOException} on the direct read path, but the eager revision-record prefetch loads
   * records asynchronously, so the very same exception can also arrive wrapped in a
   * {@link CompletionException}. What matters is the diagnosis, not the wrapper: the cause chain
   * must name the exhausted salvage source.
   */
  private static void assertUnsalvageable(final Path dbPath, final int revision) {
    final Throwable thrown = assertThrows(Throwable.class, () -> {
      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        rtx.moveToFirstChild();
      }
    }, "revision " + revision + " has no intact record anywhere and must not be served");

    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      if (cause instanceof SirixIOException && cause.getMessage() != null
          && cause.getMessage().contains("no salvageable tail-log copy")) {
        return;
      }
      if (cause.getCause() == cause) {
        break;
      }
    }
    throw new AssertionError("expected an unsalvageable-record failure naming the tail-log salvage source", thrown);
  }

  /** The 48-byte tail-log entry a revision occupies in the given beacon slot. */
  private static ByteBuffer readTailLogEntry(final Path dbPath, final long slotOffset, final int revision)
      throws Exception {
    final ByteBuffer buffer = ByteBuffer.allocate(IOStorage.REVISION_RECORD_TAIL_LOG_ENTRY_BYTES)
                                        .order(ByteOrder.LITTLE_ENDIAN);
    try (final FileChannel channel = FileChannel.open(dataFilePath(dbPath), StandardOpenOption.READ)) {
      long position = slotOffset + IOStorage.tailLogEntryOffsetInSlot(revision);
      while (buffer.hasRemaining()) {
        final int read = channel.read(buffer, position);
        if (read <= 0) {
          break;
        }
        position += read;
      }
    }
    return buffer;
  }

  // ----------------------------------------------------------------------------------------------
  // (1) Every committed revision's record is in both beacon rings, checksum-valid
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(1) each committed revision's record is staged, valid, and identical in BOTH beacon rings")
  void committedRevisionsAreStagedInBothBeaconRings() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    // Fewer commits than the ring holds, so nothing has been overwritten yet.
    final int last = createResourceWithRevisions(dbPath, CAPACITY - 2);

    for (int revision = 1; revision <= last; revision++) {
      final ByteBuffer primary = readTailLogEntry(dbPath, IOStorage.PRIMARY_BEACON_OFFSET, revision);
      final ByteBuffer secondary = readTailLogEntry(dbPath, IOStorage.SECONDARY_BEACON_OFFSET, revision);

      assertEquals(revision, primary.getInt(0), "primary ring entry must be tagged with its revision");
      assertEquals(revision, secondary.getInt(0), "secondary ring entry must be tagged with its revision");
      assertTrue(IOStorage.tailLogEntryValidAt(primary, 0), "primary ring entry must be checksum-valid");
      assertTrue(IOStorage.tailLogEntryValidAt(secondary, 0), "secondary ring entry must be checksum-valid");
      assertArrayEquals(primary.array(), secondary.array(), "both beacon slots must carry the same entry");

      // And the staged 32-byte record is byte-identical to the one in the revisions file.
      final byte[] staged = new byte[IOStorage.REVISIONS_FILE_RECORD_SIZE];
      primary.position(2 * Integer.BYTES);
      primary.get(staged);
      assertArrayEquals(readRecordBytes(dbPath, revision), staged,
                        "the staged copy must equal the revisions-file record for revision " + revision);
    }
  }

  // ----------------------------------------------------------------------------------------------
  // (2) A record lost inside the window is salvaged AND the file is self-healed
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(2) a record wiped inside the ring window is salvaged and healed back into the file")
  void wipedRecordInsideWindowIsSalvagedAndHealed() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int last = createResourceWithRevisions(dbPath, CAPACITY - 2);

    final int damaged = last - 1;
    final byte[] before = readRecordBytes(dbPath, damaged);
    wipeRecord(dbPath, damaged);
    Databases.clearGlobalCaches();

    assertAllRevisionsReadable(dbPath, last);

    Databases.clearGlobalCaches();
    assertArrayEquals(before, readRecordBytes(dbPath, damaged),
                      "the salvaged record must be written back over the wiped slot");
  }

  @Test
  @DisplayName("(2) a revisions file TRUNCATED inside the ring window is salvaged and re-grown")
  void truncatedRevisionsFileInsideWindowIsSalvaged() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int last = createResourceWithRevisions(dbPath, CAPACITY - 2);

    // Chop the last two records off entirely — the reader hits a short read, not a bad checksum.
    final long truncatedSize = IOStorage.revisionsFileOffset(last - 1);
    try (final FileChannel channel = FileChannel.open(revisionsFilePath(dbPath), StandardOpenOption.WRITE)) {
      channel.truncate(truncatedSize);
    }
    Databases.clearGlobalCaches();

    assertAllRevisionsReadable(dbPath, last);

    assertTrue(Files.size(revisionsFilePath(dbPath)) >= IOStorage.revisionsFileOffset(last)
                   + IOStorage.REVISIONS_FILE_RECORD_SIZE,
               "the healed file must cover the highest committed revision again");
  }

  // ----------------------------------------------------------------------------------------------
  // (3) Outside the window there is nothing to salvage — fail loudly
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(3) a record wiped OUTSIDE the ring window fails loudly instead of serving garbage")
  void wipedRecordOutsideWindowFailsLoudly() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    // Commit well past the ring so revision 1's entry has been overwritten by later revisions.
    final int last = createResourceWithRevisions(dbPath, CAPACITY + 4);

    wipeRecord(dbPath, 1);
    Databases.clearGlobalCaches();

    assertUnsalvageable(dbPath, 1);

    // Note the blast radius: records are prefetched for the WHOLE revision range when the resource
    // is opened, so an unsalvageable record fails the open rather than only the time-travel read of
    // that one revision. That is the intended fail-closed behavior — an unreadable revision graph
    // must not be served partially — and it is what makes (4) below the load-bearing guarantee: no
    // committed record may ever depend on the ring alone.
    assertTrue(last > CAPACITY, "the damaged record must be outside the ring window for this test");
  }

  // ----------------------------------------------------------------------------------------------
  // (4) Eviction forces the record first — losing the whole ring costs no committed revision
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(4) records evicted from the ring were forced to the revisions file beforehand")
  void evictedRecordsWereForcedBeforeEviction() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int last = createResourceWithRevisions(dbPath, CAPACITY * 2 + 3);

    // Take the entire salvage source away: whatever the revisions file holds now is exactly what
    // the eviction guard's forces made durable.
    wipeWholeTailLog(dbPath);
    Databases.clearGlobalCaches();

    assertAllRevisionsReadable(dbPath, last);
  }

  // ----------------------------------------------------------------------------------------------
  // (5) A torn ring entry is never trusted
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(5) a torn ring entry is rejected — salvage never trusts an unchecksummed copy")
  void tornRingEntryIsNeverTrusted() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int last = createResourceWithRevisions(dbPath, CAPACITY - 2);

    final int damaged = last - 1;
    // Flip a byte in the STAGED RECORD of both copies: revision tag and length stay intact, so only
    // the entry checksum can tell the copy apart from a good one.
    final int recordByteOffset = IOStorage.tailLogEntryOffsetInSlot(damaged) + 2 * Integer.BYTES + 1;
    try (final FileChannel channel = FileChannel.open(dataFilePath(dbPath), StandardOpenOption.READ,
                                                      StandardOpenOption.WRITE)) {
      for (final long slotOffset : new long[] {IOStorage.PRIMARY_BEACON_OFFSET,
          IOStorage.SECONDARY_BEACON_OFFSET}) {
        final long position = slotOffset + recordByteOffset;
        final ByteBuffer one = ByteBuffer.allocate(1);
        channel.read(one, position);
        one.flip();
        final byte flipped = (byte) (one.get(0) ^ 0xFF);
        channel.write(ByteBuffer.wrap(new byte[] {flipped}), position);
      }
    }

    final ByteBuffer entry = readTailLogEntry(dbPath, IOStorage.PRIMARY_BEACON_OFFSET, damaged);
    assertEquals(damaged, entry.getInt(0), "the tampered entry still carries its revision tag");
    assertTrue(!IOStorage.tailLogEntryValidAt(entry, 0), "a tampered entry must fail its checksum");

    // The revisions file is intact, so the (rejected) ring copy is never consulted and the resource
    // opens normally — the ring is a salvage source, not a second opinion.
    Databases.clearGlobalCaches();
    assertAllRevisionsReadable(dbPath, last);

    // But once the file record is gone too, the torn copy must NOT be accepted as a substitute.
    wipeRecord(dbPath, damaged);
    Databases.clearGlobalCaches();
    assertUnsalvageable(dbPath, damaged);
  }
}
