package io.sirix.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for UberPage corruption detection and resilience.
 *
 * <p>The UberPage is the main entry point into the resource storage — it stores the revision count
 * and bootstraps the entire page tree. It is written twice (dual beacon at offsets 0 and 512) for
 * redundancy. These tests verify that various corruption scenarios are detected and reported
 * correctly.</p>
 *
 * <p>Corruption scenarios covered:
 * <ul>
 *   <li>Empty or truncated data files (simulating power failure during first write)</li>
 *   <li>Corrupt data length headers (partial write corruption)</li>
 *   <li>Bit flips in compressed page data (bit rot)</li>
 *   <li>Complete overwrite of beacon area (disk sector zeroing or catastrophic corruption)</li>
 *   <li>Primary beacon corrupt with secondary intact (fallback testing)</li>
 *   <li>Revisions file corruption (beacon mismatch)</li>
 * </ul>
 * </p>
 */
class UberPageCorruptionTest {

  private Path tempDir;
  private ResourceConfiguration resourceConfig;

  /** Offset of the second UberPage beacon in the data file. */
  private static final int SECOND_BEACON_OFFSET = IOStorage.FIRST_BEACON >> 1; // 512

  @BeforeEach
  void setUp() throws IOException {
    tempDir = Files.createTempDirectory("sirix-uberpage-corruption-");
    final Path dataDir = tempDir.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath());
    Files.createDirectories(dataDir);
    resourceConfig = ResourceConfiguration.newBuilder("corruption-test")
        .storageType(StorageType.FILE_CHANNEL)
        .build();
    resourceConfig.resourcePath = tempDir;
  }

  @AfterEach
  void tearDown() {
    if (tempDir != null) {
      deleteRecursively(tempDir.toFile());
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private IOStorage createStorage() {
    return new FileChannelStorage(resourceConfig, Caffeine.newBuilder().buildAsync(),
        new RevisionIndexHolder());
  }

  private void writeValidUberPage() {
    final IOStorage storage = createStorage();
    try {
      final PageReference ref = new PageReference();
      final UberPage page = new UberPage();
      ref.setPage(page);
      final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();
      try (final Writer writer = storage.createWriter()) {
        writer.writeUberPageReference(resourceConfig, ref, page, bytes);
      }
    } finally {
      storage.close();
    }
  }

  private Path getDataFilePath() {
    return tempDir.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve("sirix.data");
  }

  private Path getRevisionsFilePath() {
    return tempDir.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve("sirix.revisions");
  }

  private UberPage readUberPage() {
    final IOStorage storage = createStorage();
    try (final Reader reader = storage.createReader()) {
      final PageReference ref = reader.readUberPageReference();
      return (UberPage) ref.getPage();
    } finally {
      storage.close();
    }
  }

  private static void deleteRecursively(final java.io.File file) {
    if (file.isDirectory()) {
      final java.io.File[] children = file.listFiles();
      if (children != null) {
        for (final java.io.File child : children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  /**
   * Read a 4-byte int from the data file at the given offset using native byte order.
   * FileChannelWriter uses Chronicle Bytes (native order), so we must match.
   */
  private int readNativeInt(Path file, long offset) throws IOException {
    try (final FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
      final ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
      fc.read(buf, offset);
      buf.flip();
      return buf.getInt();
    }
  }

  /**
   * Write a 4-byte int to the data file at the given offset using native byte order.
   */
  private void writeNativeInt(Path file, long offset, int value) throws IOException {
    try (final FileChannel fc = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      final ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
      buf.putInt(value);
      buf.flip();
      fc.write(buf, offset);
    }
  }

  // ---------------------------------------------------------------------------
  // Baseline: valid read-back sanity check
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Baseline: Valid UberPage round-trip")
  class BaselineTests {

    @Test
    @DisplayName("A freshly written UberPage can be read back correctly")
    void validUberPageRoundTrip() {
      writeValidUberPage();
      final UberPage page = readUberPage();
      assertNotNull(page, "UberPage must not be null");
      assertTrue(page.getRevisionCount() > 0, "Revision count must be positive");
    }

    @Test
    @DisplayName("Dual beacons produce identical data at offsets 0 and 512")
    void dualBeaconsAreIdentical() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();

      // FileChannelWriter uses Chronicle Bytes (native byte order) for the length header
      final int len1 = readNativeInt(dataFile, 0);
      final int len2 = readNativeInt(dataFile, SECOND_BEACON_OFFSET);

      assertTrue(len1 > 0, "Primary beacon data length must be positive");
      assertEquals(len1, len2, "Both beacons must have the same data length");

      try (final FileChannel fc = FileChannel.open(dataFile, StandardOpenOption.READ)) {
        final ByteBuffer data1 = ByteBuffer.allocate(len1);
        fc.read(data1, 4);
        data1.flip();

        final ByteBuffer data2 = ByteBuffer.allocate(len2);
        fc.read(data2, SECOND_BEACON_OFFSET + 4);
        data2.flip();

        assertEquals(data1, data2, "Both beacons must contain identical compressed data");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Empty / Truncated data file
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Empty or Truncated Data File")
  class TruncatedFileTests {

    @Test
    @DisplayName("Empty data file (0 bytes) throws on read")
    void emptyDataFileThrows() throws IOException {
      writeValidUberPage();
      // Truncate data file to 0 bytes
      try (final RandomAccessFile raf = new RandomAccessFile(getDataFilePath().toFile(), "rw")) {
        raf.setLength(0);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "Reading from an empty data file must throw");
    }

    @Test
    @DisplayName("Data file shorter than length header (< 4 bytes) throws on read")
    void tooShortForLengthHeader() throws IOException {
      writeValidUberPage();
      // Truncate to just 2 bytes — not enough for a 4-byte int length header
      try (final RandomAccessFile raf = new RandomAccessFile(getDataFilePath().toFile(), "rw")) {
        raf.setLength(2);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "Reading from a file too short for the data length header must throw");
    }

    @Test
    @DisplayName("Data file has valid length header but truncated page data throws on read")
    void truncatedPageData() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();

      // Read the length header using native byte order (matching Chronicle Bytes)
      final int claimedLength = readNativeInt(dataFile, 0);
      assertTrue(claimedLength > 0, "Claimed length must be positive for a valid UberPage");

      // Truncate to length header only (no page data at all)
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        raf.setLength(4);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "Truncated page data (shorter than claimed length) must throw");
    }
  }

  // ---------------------------------------------------------------------------
  // Corrupt data length header
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Corrupt Data Length Header")
  class CorruptLengthHeaderTests {

    @Test
    @DisplayName("Negative data length header on BOTH beacons throws on read")
    void negativeLengthHeaderBothBeacons() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();
      // Corrupt both beacons' length headers
      writeNativeInt(dataFile, 0, -1);
      writeNativeInt(dataFile, SECOND_BEACON_OFFSET, -1);

      assertThrows(Exception.class, () -> readUberPage(),
          "Negative data length header on both beacons must cause an error");
    }

    @Test
    @DisplayName("Zero data length header on BOTH beacons throws on read")
    void zeroLengthHeaderBothBeacons() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();
      writeNativeInt(dataFile, 0, 0);
      writeNativeInt(dataFile, SECOND_BEACON_OFFSET, 0);

      assertThrows(Exception.class, () -> readUberPage(),
          "Zero data length header on both beacons must cause an error");
    }

    @Test
    @DisplayName("Shortened data length on BOTH beacons truncates compressed frame")
    void shortenedLengthHeaderBothBeacons() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();
      final int realLength = readNativeInt(dataFile, 0);
      assertTrue(realLength > 2, "Real compressed length must be > 2 for this test");

      // Set both beacons' length to 2 bytes — too short for a valid LZ4 frame
      writeNativeInt(dataFile, 0, 2);
      writeNativeInt(dataFile, SECOND_BEACON_OFFSET, 2);

      assertThrows(Exception.class, () -> readUberPage(),
          "A truncated compressed frame on both beacons must fail");
    }

    @Test
    @DisplayName("Corrupt primary length header falls back to secondary beacon")
    void corruptPrimaryLengthFallsBackToSecondary() throws IOException {
      writeValidUberPage();
      // Corrupt only primary beacon's length header
      writeNativeInt(getDataFilePath(), 0, -1);

      // Secondary beacon is intact → fallback should succeed
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "Corrupt primary length header should fall back to secondary beacon");
      assertNotNull(page);
      assertTrue(page.getRevisionCount() > 0);
    }
  }

  // ---------------------------------------------------------------------------
  // Bit flips in compressed data (bit rot simulation)
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Bit Flips in Compressed Data (Bit Rot)")
  class BitFlipTests {

    @Test
    @DisplayName("Bit flip in primary beacon only — fallback to secondary succeeds")
    void bitFlipPrimaryOnlyFallsBack() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();

      final int dataLength = readNativeInt(dataFile, 0);
      assertTrue(dataLength > 0, "Data length must be positive");

      // Flip a bit only in the primary beacon
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        final int pos = 4 + dataLength / 2;
        raf.seek(pos);
        final byte original = raf.readByte();
        raf.seek(pos);
        raf.writeByte(original ^ 0x40);
      }

      // Secondary beacon is intact → should recover
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "Bit flip in primary beacon should fall back to secondary");
      assertNotNull(page);
      assertTrue(page.getRevisionCount() > 0);
    }

    @Test
    @DisplayName("Bit flips in BOTH beacons cause unrecoverable read failure")
    void bitFlipsBothBeacons() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();

      final int dataLength = readNativeInt(dataFile, 0);
      assertTrue(dataLength > 0, "Data length must be positive");

      // Flip bits in both primary and secondary beacons
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        // Corrupt primary beacon
        final int primaryPos = 4 + dataLength / 2;
        raf.seek(primaryPos);
        final byte primaryOriginal = raf.readByte();
        raf.seek(primaryPos);
        raf.writeByte(primaryOriginal ^ 0x40);

        // Corrupt secondary beacon
        final int secondaryPos = SECOND_BEACON_OFFSET + 4 + dataLength / 2;
        raf.seek(secondaryPos);
        final byte secondaryOriginal = raf.readByte();
        raf.seek(secondaryPos);
        raf.writeByte(secondaryOriginal ^ 0x40);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "Bit flips in both beacons must cause an unrecoverable read failure");
    }
  }

  // ---------------------------------------------------------------------------
  // Complete overwrite of beacon area
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Complete Overwrite of Beacon Area")
  class OverwriteTests {

    @Test
    @DisplayName("Zeroed-out primary beacon recovers via secondary beacon")
    void zeroedPrimaryBeaconFallsBack() throws IOException {
      writeValidUberPage();
      // Overwrite only the primary beacon area (first 512 bytes) with zeros
      try (final RandomAccessFile raf = new RandomAccessFile(getDataFilePath().toFile(), "rw")) {
        raf.seek(0);
        raf.write(new byte[SECOND_BEACON_OFFSET]);
      }

      // Secondary beacon at offset 512 is intact → fallback should work
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "Zeroed primary beacon should recover via secondary beacon");
      assertNotNull(page);
      assertTrue(page.getRevisionCount() > 0);
    }

    @Test
    @DisplayName("Random garbage over primary beacon recovers via secondary beacon")
    void randomGarbagePrimaryBeaconFallsBack() throws IOException {
      writeValidUberPage();
      final byte[] garbage = new byte[SECOND_BEACON_OFFSET];
      new Random(0xDEAD).nextBytes(garbage);

      try (final RandomAccessFile raf = new RandomAccessFile(getDataFilePath().toFile(), "rw")) {
        raf.seek(0);
        raf.write(garbage);
      }

      // Secondary beacon at offset 512 is intact → fallback should work
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "Random garbage on primary beacon should recover via secondary beacon");
      assertNotNull(page);
      assertTrue(page.getRevisionCount() > 0);
    }

    @Test
    @DisplayName("Both beacons overwritten with zeros causes unrecoverable failure")
    void bothBeaconsZeroed() throws IOException {
      writeValidUberPage();
      // Zero out the entire beacon area (both copies)
      try (final RandomAccessFile raf = new RandomAccessFile(getDataFilePath().toFile(), "rw")) {
        raf.seek(0);
        raf.write(new byte[IOStorage.FIRST_BEACON]);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "Both beacons zeroed must cause an unrecoverable read failure");
    }

    @Test
    @DisplayName("Both beacons overwritten with random garbage causes unrecoverable failure")
    void bothBeaconsGarbage() throws IOException {
      writeValidUberPage();
      final byte[] garbage = new byte[IOStorage.FIRST_BEACON];
      new Random(0xCAFE).nextBytes(garbage);

      try (final RandomAccessFile raf = new RandomAccessFile(getDataFilePath().toFile(), "rw")) {
        raf.seek(0);
        raf.write(garbage);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "Both beacons garbled must cause an unrecoverable read failure");
    }
  }

  // ---------------------------------------------------------------------------
  // Primary beacon corrupt, secondary intact
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Primary Beacon Corrupt, Secondary Intact")
  class BeaconFallbackTests {

    @Test
    @DisplayName("Primary beacon zeroed while secondary intact — fallback recovers")
    void primaryCorruptSecondaryIntactFallbackRecovers() throws IOException {
      writeValidUberPage();

      // Verify secondary beacon is readable by itself
      final Path dataFile = getDataFilePath();
      final int secondBeaconLength = readNativeInt(dataFile, SECOND_BEACON_OFFSET);
      assertTrue(secondBeaconLength > 0, "Secondary beacon should contain valid data");

      // Zero out only the primary beacon (first 512 bytes)
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        raf.seek(0);
        raf.write(new byte[SECOND_BEACON_OFFSET]);
      }

      // The reader should fall back to the secondary beacon at offset 512
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "With primary beacon corrupt, reader must fall back to secondary beacon");
      assertNotNull(page, "Recovered UberPage must not be null");
      assertTrue(page.getRevisionCount() > 0, "Recovered UberPage must have valid revision count");
    }

    @Test
    @DisplayName("Secondary beacon corrupt while primary intact — read still succeeds")
    void secondaryCorruptPrimaryIntact() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();

      // Corrupt only the secondary beacon area (offset 512 to 1024)
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        final byte[] garbage = new byte[SECOND_BEACON_OFFSET];
        new Random(0xBEEF).nextBytes(garbage);
        raf.seek(SECOND_BEACON_OFFSET);
        raf.write(garbage);
      }

      // Primary beacon is still intact, so read should succeed
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "With primary beacon intact, read must succeed even if secondary is corrupt");
      assertNotNull(page);
      assertTrue(page.getRevisionCount() > 0);
    }
  }

  // ---------------------------------------------------------------------------
  // Revisions file corruption
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Revisions File Corruption")
  class RevisionsFileCorruptionTests {

    @Test
    @DisplayName("Empty revisions file does not prevent UberPage read from data file")
    void emptyRevisionsFileStillAllowsDataFileRead() throws IOException {
      writeValidUberPage();
      // Truncate revisions file to 0 bytes
      try (final RandomAccessFile raf = new RandomAccessFile(getRevisionsFilePath().toFile(), "rw")) {
        raf.setLength(0);
      }

      // UberPage read uses the DATA file, not the revisions file.
      // The revisions file is for revision-root-page offset lookups.
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "UberPage read should use the data file, not the revisions file");
      assertNotNull(page);
    }

    @Test
    @DisplayName("Deleted revisions file does not prevent UberPage read")
    void deletedRevisionsFileStillAllowsRead() throws IOException {
      writeValidUberPage();
      Files.deleteIfExists(getRevisionsFilePath());

      // FileChannelStorage.createReader() creates the revisions file if missing,
      // so this should still work for UberPage reads.
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "UberPage read should succeed even if revisions file was deleted");
      assertNotNull(page);
    }

    @Test
    @DisplayName("Garbled revisions file does not prevent UberPage read from data file")
    void garbledRevisionsFileStillAllowsDataFileRead() throws IOException {
      writeValidUberPage();
      final byte[] garbage = new byte[2048];
      new Random(0xFACE).nextBytes(garbage);
      try (final RandomAccessFile raf = new RandomAccessFile(getRevisionsFilePath().toFile(), "rw")) {
        raf.seek(0);
        raf.write(garbage);
      }

      // UberPage itself lives in the data file at offset 0
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "UberPage read should use the data file, not the revisions file");
      assertNotNull(page);
    }
  }

  // ---------------------------------------------------------------------------
  // Edge cases in serialized page content
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("Edge Cases in Page Content")
  class PageContentEdgeCases {

    @Test
    @DisplayName("Swapped beacon data (secondary written to primary position) still deserializes")
    void swappedBeaconStillDeserializes() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();

      // Read secondary beacon raw bytes (length header + compressed data)
      final int len = readNativeInt(dataFile, SECOND_BEACON_OFFSET);
      final byte[] secondaryRaw = new byte[4 + len];
      try (final FileChannel fc = FileChannel.open(dataFile, StandardOpenOption.READ)) {
        final ByteBuffer buf = ByteBuffer.allocate(4 + len);
        fc.read(buf, SECOND_BEACON_OFFSET);
        buf.flip();
        buf.get(secondaryRaw);
      }

      // Overwrite primary beacon with secondary beacon data (they should be identical)
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        raf.seek(0);
        raf.write(secondaryRaw);
      }

      // Since both beacons contain the same UberPage, this should succeed
      final UberPage page = assertDoesNotThrow(() -> readUberPage(),
          "Swapped beacon data should still deserialize correctly");
      assertNotNull(page);
    }

    @Test
    @DisplayName("Data file with only length header (4 bytes, zero-length page) throws")
    void onlyLengthHeaderZeroData() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();
      // Write zero length in native byte order, then truncate
      writeNativeInt(dataFile, 0, 0);
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        raf.setLength(4);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "A zero-length page body must fail deserialization");
    }

    @Test
    @DisplayName("Data length claims 1 byte — too short for any valid page")
    void singleBytePageData() throws IOException {
      writeValidUberPage();
      final Path dataFile = getDataFilePath();
      // Write length=1 in native byte order
      writeNativeInt(dataFile, 0, 1);
      try (final RandomAccessFile raf = new RandomAccessFile(dataFile.toFile(), "rw")) {
        // Write a single byte of page data after the 4-byte header
        raf.seek(4);
        raf.writeByte(0xFF);
        raf.setLength(5);
      }

      assertThrows(Exception.class, () -> readUberPage(),
          "A 1-byte page body is too small for any valid UberPage");
    }
  }

  // ---------------------------------------------------------------------------
  // UberPage serialization format validation
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("UberPage Serialization Format")
  class SerializationFormatTests {

    @Test
    @DisplayName("Written UberPage starts with correct page kind byte after decompression")
    void correctPageKindByte() {
      writeValidUberPage();

      // If we can successfully read it back, the page kind byte (3) was correct
      final UberPage page = readUberPage();
      assertNotNull(page);
    }

    @Test
    @DisplayName("UberPage preserves revision count through write-read cycle")
    void revisionCountPreserved() {
      final IOStorage storage = createStorage();
      final int expectedRevisionCount;
      try {
        final PageReference ref = new PageReference();
        final UberPage page = new UberPage();
        expectedRevisionCount = page.getRevisionCount();
        ref.setPage(page);
        final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();
        try (final Writer writer = storage.createWriter()) {
          writer.writeUberPageReference(resourceConfig, ref, page, bytes);
        }
      } finally {
        storage.close();
      }

      final UberPage readBack = readUberPage();
      assertEquals(expectedRevisionCount, readBack.getRevisionCount(),
          "Revision count must be preserved through the write-read cycle");
    }

    @Test
    @DisplayName("Clone constructor increments revision count for non-bootstrap page")
    void cloneIncrements() {
      final UberPage original = new UberPage();
      original.setBootstrap(false);

      final UberPage clone = new UberPage(original);
      assertEquals(original.getRevisionCount() + 1, clone.getRevisionCount(),
          "Clone of a non-bootstrap UberPage must increment revision count");
    }

    @Test
    @DisplayName("Bootstrap clone preserves revision count")
    void bootstrapClonePreserves() {
      final UberPage original = new UberPage();
      assertTrue(original.isBootstrap());

      final UberPage clone = new UberPage(original);
      assertEquals(original.getRevisionCount(), clone.getRevisionCount(),
          "Clone of a bootstrap UberPage must preserve revision count");
    }
  }

  // ---------------------------------------------------------------------------
  // Concurrent corruption scenarios
  // ---------------------------------------------------------------------------

  @Nested
  @DisplayName("File Permissions and Access Errors")
  class AccessErrorTests {

    @Test
    @DisplayName("Deleted data file after writer close throws on read")
    void deletedDataFileThrows() throws IOException {
      writeValidUberPage();
      Files.delete(getDataFilePath());

      // The storage recreates the file as empty — reading must fail with SirixIOException
      assertThrows(SirixIOException.class, () -> readUberPage(),
          "Reading from a deleted (recreated-empty) data file must throw SirixIOException");
    }
  }
}
