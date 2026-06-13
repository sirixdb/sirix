package io.sirix.io;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.io.memorymapped.MMStorage;
import io.sirix.page.RevisionRootPage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hardening regression tests: RevisionRootPage bodies are content-hash-validated on read, like
 * every other page.
 *
 * <p>Background. Every page's XXH3 (of its compressed payload) is stored on its PARENT's
 * PageReference and verified on read ({@code AbstractReader.verifyChecksumIfNeeded}). The
 * RevisionRootPage is the one page reached WITHOUT a parent reference — via
 * {@code Reader.readRevisionRootPage}, whose offset comes from a fixed-slot record in
 * {@code sirix.revisions}. Before this change that path deserialized the body with no integrity
 * check at all: a single flipped byte in a RevisionRootPage on disk was silently deserialized
 * (documented in {@code docs/DISK_FORMAT.md} §3 "Still not covered").
 *
 * <p>The fix stores the RevisionRootPage's own page hash in the revisions record's former
 * "reserved" 4th field and verifies the body against it on read. The record checksum is widened to
 * cover that field (24 bytes) when it is present; a record whose hash field is {@code 0} is a
 * LEGACY (beta1-and-earlier) record and keeps the 16-byte checksum, so old resources still open.
 *
 * <p>Each corruption case is paired with a "demonstrates the pre-fix gap" assertion: the same
 * corrupted body, read through a reader with verification DISABLED, deserializes to a non-null
 * page instead of throwing — exactly the (un-hardened) behavior the production default now
 * prevents. (Pre-fix, the verify-enabled path behaved identically to the verify-disabled path,
 * because there was no verification on this path at all; verified independently by stashing the
 * fix.)
 */
final class RevisionRootPageHashValidationTest {

  private static final String RESOURCE = "revroot-hash-resource";

  private static final String DOC = "{\"r\":1,\"name\":\"alpha\",\"nested\":{\"a\":[1,2,3],\"b\":true}}";

  @BeforeEach
  void setUp() {
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
    return resourcePath(dbPath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve("sirix.data");
  }

  private static Path revisionsFilePath(final Path dbPath) {
    return resourcePath(dbPath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve("sirix.revisions");
  }

  /**
   * Create a resource with the given storage type and commit {@code commits} revisions (each a
   * fresh subtree insert so the RevisionRootPage genuinely differs). Returns the last user-visible
   * revision number. The database is fully closed on return so the on-disk files can be poked.
   */
  private int createResourceWithRevisions(final Path dbPath, final StorageType storageType, final int commits) {
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    int lastRevision;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(storageType)
          .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        for (int i = 0; i < commits; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
            wtx.commit();
          }
        }
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          lastRevision = rtx.getRevisionNumber();
        }
      }
    }
    // Drop any cached RevisionFileData / pages so a subsequent raw read goes to disk.
    Databases.clearGlobalCaches();
    return lastRevision;
  }

  /** Deserialize the on-disk resource configuration (carries storageType + verifyChecksumsOnRead). */
  private ResourceConfiguration loadConfig(final Path dbPath) {
    return ResourceConfiguration.deserialize(resourcePath(dbPath));
  }

  /** A fresh per-call cache so a reader never sees a stale RevisionFileData from another phase. */
  private static AsyncCache<Integer, RevisionFileData> freshCache() {
    return Caffeine.newBuilder().maximumSize(10_000).buildAsync();
  }

  /** Build a storage instance directly (no global cache, no session) for the given config. */
  private static IOStorage storageFor(final ResourceConfiguration config) {
    return config.getStorageType() == StorageType.MEMORY_MAPPED
        ? new MMStorage(config, freshCache())
        : new FileChannelStorage(config, freshCache());
  }

  /** Read revision record {@code revision} straight from {@code sirix.revisions} (little-endian). */
  private static long[] readRecord(final Path dbPath, final int revision) throws Exception {
    final long off = IOStorage.revisionsFileOffset(revision);
    try (final RandomAccessFile raf = new RandomAccessFile(revisionsFilePath(dbPath).toFile(), "r")) {
      final byte[] buf = new byte[IOStorage.REVISIONS_FILE_RECORD_SIZE];
      raf.seek(off);
      raf.readFully(buf);
      final ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
      return new long[] {bb.getLong(0), bb.getLong(8), bb.getLong(16), bb.getLong(24)};
    }
  }

  /** Overwrite one 8-byte field (0=offset,1=timestamp,2=checksum,3=hash) of a revision record. */
  private static void writeRecordField(final Path dbPath, final int revision, final int fieldIndex,
      final long value) throws Exception {
    final long off = IOStorage.revisionsFileOffset(revision) + fieldIndex * 8L;
    final ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(value).flip();
    try (final FileChannel ch = FileChannel.open(revisionsFilePath(dbPath), StandardOpenOption.WRITE)) {
      ch.write(bb, off);
    }
  }

  /**
   * Flip a byte INSIDE the RevisionRootPage's compressed body (the {@code [u32 len][payload]} at
   * the record's offset; we poke {@code payload[byteInBody]}). This corrupts exactly the bytes the
   * page hash covers, without touching the length prefix or any other page.
   */
  private static void corruptRevisionRootBody(final Path dbPath, final int revision, final int byteInBody)
      throws Exception {
    final long recordOffset = readRecord(dbPath, revision)[0];
    final long bodyByteOffset = recordOffset + Integer.BYTES + byteInBody;
    try (final RandomAccessFile raf = new RandomAccessFile(dataFilePath(dbPath).toFile(), "rw")) {
      raf.seek(bodyByteOffset);
      final int original = raf.read();
      raf.seek(bodyByteOffset);
      raf.write(original ^ 0xFF);
    }
  }

  /** Read a single revision-root via a freshly-built reader for {@code config}. */
  private static RevisionRootPage readRevisionRoot(final ResourceConfiguration config, final int revision) {
    final IOStorage storage = storageFor(config);
    try (final Reader reader = storage.createReader()) {
      return reader.readRevisionRootPage(revision, config);
    } finally {
      storage.close();
    }
  }

  /**
   * Assert the read does NOT surface a {@link SirixCorruptionException} — i.e. the content-hash
   * integrity layer did not run / was bypassed. This is the pre-fix behavior on this path: a
   * corrupt body was either silently deserialized into a wrong page OR blew up deep in the
   * deserializer with an opaque low-level exception ({@code NegativeArraySizeException} etc.), but
   * was NEVER cleanly flagged as corruption. Either of those non-corruption outcomes is acceptable
   * here; a {@code SirixCorruptionException} is NOT (that would mean verification ran).
   */
  private static void assertNoCorruptionDetection(final ResourceConfiguration config, final int revision) {
    try {
      assertNotNull(readRevisionRoot(config, revision),
          "verification bypassed: a (possibly wrong) page is returned, not a corruption error");
    } catch (final SirixCorruptionException unexpected) {
      throw new AssertionError("expected NO clean corruption detection on this path, but got one", unexpected);
    } catch (final RuntimeException deserializationBlewUp) {
      // A corrupt compressed body can also fail deserialization with a low-level exception — that
      // is precisely the un-hardened failure mode (garbage-in surfaces as an opaque crash, not a
      // clean SirixCorruptionException). Acceptable: it is not a corruption *detection*.
    }
  }

  // ----------------------------------------------------------------------------------------------
  // (a) Corrupt RevisionRootPage body -> SirixCorruptionException (both backends)
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(a) FILE_CHANNEL: flipped byte in a RevisionRootPage body is detected on read")
  void fileChannel_corruptRevisionRootBody_throws() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int rev = createResourceWithRevisions(dbPath, StorageType.FILE_CHANNEL, 2);

    corruptRevisionRootBody(dbPath, rev, 3);

    // Verify-enabled (production default) MUST reject the corrupt body.
    final SirixCorruptionException ex =
        assertThrows(SirixCorruptionException.class, () -> readRevisionRoot(loadConfig(dbPath), rev),
            "a flipped RevisionRootPage body byte must be caught by hash verification");
    assertTrue(ex.getMessage().toLowerCase().contains("corruption"), "should report corruption");

    // Demonstrate the pre-fix gap: with verification DISABLED the integrity layer is bypassed —
    // the corrupt body is NOT cleanly flagged (it returns a wrong page or crashes in the
    // deserializer). That is exactly the un-hardened behavior the default now prevents.
    assertNoCorruptionDetection(withVerificationDisabled(loadConfig(dbPath)), rev);
  }

  @Test
  @DisplayName("(a) MEMORY_MAPPED: flipped byte in a RevisionRootPage body is detected on read")
  void memoryMapped_corruptRevisionRootBody_throws() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int rev = createResourceWithRevisions(dbPath, StorageType.MEMORY_MAPPED, 2);

    corruptRevisionRootBody(dbPath, rev, 3);

    assertThrows(SirixCorruptionException.class, () -> readRevisionRoot(loadConfig(dbPath), rev),
        "a flipped RevisionRootPage body byte must be caught by hash verification (mmap)");

    assertNoCorruptionDetection(withVerificationDisabled(loadConfig(dbPath)), rev);
  }

  // ----------------------------------------------------------------------------------------------
  // (b) Corrupt the record's offset / timestamp / hash field -> caught by the record checksum
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(b) corrupt record offset field is caught by the record checksum")
  void corruptRecordOffset_throws() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int rev = createResourceWithRevisions(dbPath, StorageType.FILE_CHANNEL, 1);

    final long[] before = readRecord(dbPath, rev);
    writeRecordField(dbPath, rev, 0, before[0] ^ 0xABCDL); // flip the offset, leave checksum stale

    assertThrows(SirixIOException.class, () -> readRevisionRoot(loadConfig(dbPath), rev),
        "a corrupted offset must fail the record checksum");
  }

  @Test
  @DisplayName("(b) corrupt record timestamp field is caught by the record checksum")
  void corruptRecordTimestamp_throws() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int rev = createResourceWithRevisions(dbPath, StorageType.MEMORY_MAPPED, 1);

    final long[] before = readRecord(dbPath, rev);
    writeRecordField(dbPath, rev, 1, before[1] ^ 0x5L);

    assertThrows(SirixIOException.class, () -> readRevisionRoot(loadConfig(dbPath), rev),
        "a corrupted timestamp must fail the record checksum (mmap)");
  }

  @Test
  @DisplayName("(b) corrupt record HASH field is caught by the (24-byte) record checksum")
  void corruptRecordHashField_throws() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int rev = createResourceWithRevisions(dbPath, StorageType.FILE_CHANNEL, 1);

    final long[] before = readRecord(dbPath, rev);
    // Sanity: a fresh record written by this build carries a non-zero hash in field 3.
    assertTrue(before[3] != 0L, "a record written by this build must carry a non-zero page hash");
    // Flip the hash field only — the 24-byte checksum must now mismatch (it covers field 3).
    writeRecordField(dbPath, rev, 3, before[3] ^ 0x1L);

    assertThrows(SirixIOException.class, () -> readRevisionRoot(loadConfig(dbPath), rev),
        "tampering with the page-hash field must fail the 24-byte record checksum");
  }

  // ----------------------------------------------------------------------------------------------
  // (c) verifyChecksumsOnRead=false -> opt-out respected (corrupt body still reads)
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(c) verifyChecksumsOnRead=false does NOT raise a corruption error for a corrupt body")
  void verificationDisabled_corruptBody_doesNotThrowCorruption() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int rev = createResourceWithRevisions(dbPath, StorageType.FILE_CHANNEL, 1);

    corruptRevisionRootBody(dbPath, rev, 3);

    // Opt-out honored: the hash check is skipped, so no SirixCorruptionException is raised for the
    // tampered body (it is read back as-is — possibly garbage). Contrast (a), where the same
    // corruption under the default ON yields a clean SirixCorruptionException.
    assertNoCorruptionDetection(withVerificationDisabled(loadConfig(dbPath)), rev);
  }

  // ----------------------------------------------------------------------------------------------
  // (d) Backward compat: a legacy record (hash field == 0, 16-byte checksum) opens cleanly
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(d) legacy record (hash==0, 16-byte checksum) opens cleanly — beta1 resources still work")
  void legacyRecord_opensCleanly() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    final int rev = createResourceWithRevisions(dbPath, StorageType.FILE_CHANNEL, 1);

    // Downgrade the record to the LEGACY format beta1 wrote: hash field 0, checksum over 16 bytes.
    final long[] r = readRecord(dbPath, rev);
    writeRecordField(dbPath, rev, 3, 0L);                                   // hash field -> legacy 0
    writeRecordField(dbPath, rev, 2, IOStorage.revisionRecordChecksum(r[0], r[1])); // 16-byte checksum
    Databases.clearGlobalCaches();

    // Opens and reads with NO false-positive corruption error (default verification ON).
    final RevisionRootPage page = readRevisionRoot(loadConfig(dbPath), rev);
    assertNotNull(page, "a legacy (hash==0) record must open cleanly under this build");

    // And because a legacy record carries no page hash, the body is (correctly) NOT verified:
    // corrupting it is NOT cleanly flagged — there is simply nothing to check against. This proves
    // the hash==0 disambiguation: present-hash => verify, absent-hash (legacy) => skip.
    corruptRevisionRootBody(dbPath, rev, 3);
    Databases.clearGlobalCaches();
    assertNoCorruptionDetection(loadConfig(dbPath), rev);
  }

  @Test
  @DisplayName("(d) bulk getRevisionFileData over a legacy record opens cleanly (mmap + filechannel)")
  void legacyRecord_bulkRead_opensCleanly() throws Exception {
    for (final StorageType type : new StorageType[] {StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED}) {
      JsonTestHelper.deleteEverything();
      Databases.clearGlobalCaches();
      final Path dbPath = PATHS.PATH1.getFile();
      final int rev = createResourceWithRevisions(dbPath, type, 2);

      // Downgrade revision `rev` to legacy.
      final long[] r = readRecord(dbPath, rev);
      writeRecordField(dbPath, rev, 3, 0L);
      writeRecordField(dbPath, rev, 2, IOStorage.revisionRecordChecksum(r[0], r[1]));
      Databases.clearGlobalCaches();

      final ResourceConfiguration config = loadConfig(dbPath);
      final IOStorage storage = storageFor(config);
      try (final Reader reader = storage.createReader()) {
        final RevisionFileData[] all = reader.getRevisionFileData(0, rev + 1);
        assertEquals(rev + 1, all.length, type + ": bulk read returns all records");
        assertEquals(0L, all[rev].pageHash(), type + ": downgraded record reports a 0 (legacy) hash");
      } finally {
        storage.close();
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // (e) Round-trip: a clean resource reads every revision (no false positives) incl. time-travel
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(e) clean multi-revision resource reads every revision + time-travels with no false positive")
  void cleanResource_allRevisionsReadable() throws Exception {
    for (final StorageType type : new StorageType[] {StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED}) {
      JsonTestHelper.deleteEverything();
      Databases.clearGlobalCaches();
      final Path dbPath = PATHS.PATH1.getFile();
      final int last = createResourceWithRevisions(dbPath, type, 4);

      final ResourceConfiguration config = loadConfig(dbPath);
      // Raw reader: read every revision-root directly (verification ON by default).
      for (int rev = 1; rev <= last; rev++) {
        final RevisionRootPage page = readRevisionRoot(config, rev);
        assertNotNull(page, type + ": revision " + rev + " must read cleanly");
        assertEquals(rev, page.getRevision(), type + ": revision number round-trips");
      }

      // And via the full session/time-travel path (exercises the RevisionRootPage cache + readers).
      Databases.clearGlobalCaches();
      try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
          final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        for (int rev = 1; rev <= last; rev++) {
          try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
            assertEquals(rev, rtx.getRevisionNumber(), type + ": time-travel to revision " + rev);
            assertTrue(rtx.moveToFirstChild(), type + ": revision " + rev + " data must be navigable");
          }
        }
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // (f) Checksum/sentinel invariants (pure, no I/O) — lock in the soundness reasoning
  // ----------------------------------------------------------------------------------------------

  @Test
  @DisplayName("(f) expectedRevisionRecordChecksum dispatches 16-byte (legacy) vs 24-byte (present)")
  void checksumDispatch_legacyVsPresent() {
    final long off = 0x1122334455667788L;
    final long ts = 0x0000017F_ABCDEF01L;
    final long hash = 0xDEADBEEFCAFEBABEL;

    // Present hash -> 24-byte variant; legacy (0) -> 16-byte variant.
    assertEquals(IOStorage.revisionRecordChecksum(off, ts, hash),
        IOStorage.expectedRevisionRecordChecksum(off, ts, hash), "present hash -> 24-byte checksum");
    assertEquals(IOStorage.revisionRecordChecksum(off, ts),
        IOStorage.expectedRevisionRecordChecksum(off, ts, 0L), "hash==0 -> legacy 16-byte checksum");
    // The two variants must differ for the same (off,ts) — otherwise the hash field would be
    // unprotected and a present record indistinguishable from a legacy one by checksum alone.
    assertTrue(IOStorage.revisionRecordChecksum(off, ts) != IOStorage.revisionRecordChecksum(off, ts, hash),
        "16- and 24-byte checksums must differ so the hash field is genuinely covered");
  }

  @Test
  @DisplayName("(f) an all-zero slot never matches the legacy checksum (recovery scan safety)")
  void allZeroSlot_neverMatchesLegacyChecksum() {
    // hasNoChecksumValidRevisionRecord relies on this: a zeroed slot has storedChecksum==0, which
    // must not equal the checksum of (offset=0, timestamp=0, hash=0/legacy).
    assertTrue(IOStorage.expectedRevisionRecordChecksum(0L, 0L, 0L) != 0L,
        "checksum of an all-zero legacy record must be non-zero");
  }

  @Test
  @DisplayName("(f) hash normalization: 0 -> non-zero sentinel; everything else (incl. sentinel) unchanged")
  void hashNormalization_isFailClosed() {
    assertEquals(IOStorage.ZERO_HASH_SENTINEL, IOStorage.normalizeRevisionRootPageHash(0L),
        "an all-zero real hash is remapped to the sentinel so the stored field is never 0");
    assertTrue(IOStorage.ZERO_HASH_SENTINEL != 0L, "the sentinel itself must be non-zero");
    assertEquals(0xCAFEL, IOStorage.normalizeRevisionRootPageHash(0xCAFEL), "non-zero hashes are unchanged");
    // A page whose real hash equals the sentinel is stored unchanged (verifies fine, no remap).
    assertEquals(IOStorage.ZERO_HASH_SENTINEL,
        IOStorage.normalizeRevisionRootPageHash(IOStorage.ZERO_HASH_SENTINEL),
        "a real hash equal to the sentinel is stored as-is (round-trips, no false positive)");
  }

  // ----------------------------------------------------------------------------------------------

  /**
   * Clone the persisted config but with {@code verifyChecksumsOnRead=false}. Copies the
   * byte-handler pipeline + storage type so deserialization stays byte-compatible, and points the
   * (public, mutable) {@code resourcePath} at the SAME on-disk files as the persisted config — the
   * storage classes read only {@code resourcePath} + {@code byteHandlePipeline}. Used to exhibit
   * the pre-fix (un-hardened) read behavior, where no verification ran on this path at all.
   */
  private static ResourceConfiguration withVerificationDisabled(final ResourceConfiguration persisted) {
    final ResourceConfiguration disabled = new ResourceConfiguration.Builder(RESOURCE)
        .storageType(persisted.getStorageType())
        .byteHandlerPipeline(persisted.byteHandlePipeline)
        .verifyChecksumsOnRead(false)
        .build();
    disabled.resourcePath = persisted.resourcePath;
    return disabled;
  }
}
