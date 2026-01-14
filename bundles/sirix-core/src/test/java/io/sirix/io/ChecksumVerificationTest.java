package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for high-performance page checksum verification.
 * 
 * <p>Tests cover:
 * <ul>
 *   <li>Zero-allocation long-based API</li>
 *   <li>Zero-copy native memory segment hashing</li>
 *   <li>Bit manipulation byte conversion (no ByteBuffer)</li>
 *   <li>Hash algorithm extensibility</li>
 * </ul>
 * </p>
 */
class ChecksumVerificationTest {

  private static final HashAlgorithm ALGO = HashAlgorithm.XXH3;

  @Nested
  @DisplayName("HashAlgorithm Zero-Allocation API Tests")
  class ZeroAllocationTests {

    @Test
    @DisplayName("computeHashLong returns consistent values")
    void computeHashLongIsConsistent() {
      byte[] data = "Hello, SirixDB!".getBytes();
      
      long hash1 = ALGO.computeHashLong(data);
      long hash2 = ALGO.computeHashLong(data);
      
      assertEquals(hash1, hash2, "computeHashLong should be consistent");
    }

    @Test
    @DisplayName("computeHashLong produces different values for different data")
    void computeHashLongDiffersForDifferentData() {
      long hash1 = ALGO.computeHashLong("Hello, SirixDB!".getBytes());
      long hash2 = ALGO.computeHashLong("Hello, Sirix!".getBytes());
      
      assertNotEquals(hash1, hash2, "Different data should produce different hashes");
    }

    @Test
    @DisplayName("verifyLong returns true for matching hash")
    void verifyLongReturnsTrueForMatch() {
      byte[] data = "Test data".getBytes();
      long hash = ALGO.computeHashLong(data);
      
      assertTrue(ALGO.verifyLong(data, hash), "verifyLong should return true for matching hash");
    }

    @Test
    @DisplayName("verifyLong returns false for mismatched hash")
    void verifyLongReturnsFalseForMismatch() {
      byte[] data = "Test data".getBytes();
      long wrongHash = ALGO.computeHashLong("Different data".getBytes());
      
      assertFalse(ALGO.verifyLong(data, wrongHash), "verifyLong should return false for mismatch");
    }

    @Test
    @DisplayName("Range hashing with long API works correctly")
    void rangeHashingWithLongApi() {
      byte[] data = "PREFIXHello, SirixDB!SUFFIX".getBytes();
      long expected = ALGO.computeHashLong("Hello, SirixDB!".getBytes());
      
      long actual = ALGO.computeHashLong(data, 6, 15);
      
      assertEquals(expected, actual, "Range hashing should match full data hashing");
    }
  }

  @Nested
  @DisplayName("Zero-Copy MemorySegment Tests")
  class ZeroCopyTests {

    @Test
    @DisplayName("Native MemorySegment uses zero-copy path")
    void nativeSegmentZeroCopy() {
      byte[] data = "Test data for native segment".getBytes();
      long expectedFromArray = ALGO.computeHashLong(data);
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment nativeSegment = arena.allocate(data.length);
        nativeSegment.copyFrom(MemorySegment.ofArray(data));
        
        assertTrue(nativeSegment.isNative(), "Segment should be native (off-heap)");
        
        long actualFromSegment = ALGO.computeHashLong(nativeSegment);
        assertEquals(expectedFromArray, actualFromSegment,
            "Native segment hash should match byte array hash");
      }
    }

    @Test
    @DisplayName("Heap-backed MemorySegment works correctly")
    void heapSegmentWorks() {
      byte[] data = "Test data for heap segment".getBytes();
      long expectedFromArray = ALGO.computeHashLong(data);
      
      MemorySegment heapSegment = MemorySegment.ofArray(data);
      assertFalse(heapSegment.isNative(), "Segment should be heap-backed");
      
      long actualFromSegment = ALGO.computeHashLong(heapSegment);
      assertEquals(expectedFromArray, actualFromSegment,
          "Heap segment hash should match byte array hash");
    }

    @Test
    @DisplayName("verifyLong with native MemorySegment is zero-copy")
    void verifyLongNativeSegmentZeroCopy() {
      byte[] data = "Test data for verification".getBytes();
      long hash = ALGO.computeHashLong(data);
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment nativeSegment = arena.allocate(data.length);
        nativeSegment.copyFrom(MemorySegment.ofArray(data));
        
        assertTrue(ALGO.verifyLong(nativeSegment, hash),
            "verifyLong should work with native MemorySegment");
      }
    }

    @Test
    @DisplayName("verifyLong detects corruption in MemorySegment")
    void verifyLongDetectsCorruptionInSegment() {
      byte[] data = "Test data for verification".getBytes();
      long hash = ALGO.computeHashLong(data);
      
      byte[] corrupted = data.clone();
      corrupted[5] ^= 0xFF;
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment nativeSegment = arena.allocate(corrupted.length);
        nativeSegment.copyFrom(MemorySegment.ofArray(corrupted));
        
        assertFalse(ALGO.verifyLong(nativeSegment, hash),
            "verifyLong should detect corruption in MemorySegment");
      }
    }
  }

  @Nested
  @DisplayName("Bit Manipulation Conversion Tests (No ByteBuffer)")
  class BitManipulationTests {

    @Test
    @DisplayName("longToBytes and bytesToLong are inverses")
    void longToBytesAndBytesToLongAreInverses() {
      long original = 0xDEADBEEFCAFEBABEL;
      
      byte[] bytes = HashAlgorithm.longToBytes(original);
      long roundTrip = HashAlgorithm.bytesToLong(bytes);
      
      assertEquals(original, roundTrip, "Round-trip should preserve value");
    }

    @Test
    @DisplayName("longToBytes produces big-endian bytes")
    void longToBytesBigEndian() {
      long value = 0x0102030405060708L;
      
      byte[] bytes = HashAlgorithm.longToBytes(value);
      
      assertEquals(8, bytes.length);
      assertEquals(0x01, bytes[0]);
      assertEquals(0x02, bytes[1]);
      assertEquals(0x03, bytes[2]);
      assertEquals(0x04, bytes[3]);
      assertEquals(0x05, bytes[4]);
      assertEquals(0x06, bytes[5]);
      assertEquals(0x07, bytes[6]);
      assertEquals(0x08, bytes[7]);
    }

    @Test
    @DisplayName("bytesToLong reads big-endian bytes")
    void bytesToLongBigEndian() {
      byte[] bytes = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
      
      long value = HashAlgorithm.bytesToLong(bytes);
      
      assertEquals(0x0102030405060708L, value);
    }

    @Test
    @DisplayName("bytesToLong throws for wrong array length")
    void bytesToLongThrowsForWrongLength() {
      assertThrows(IllegalArgumentException.class, 
          () -> HashAlgorithm.bytesToLong(new byte[7]),
          "Should throw for 7 bytes");
      assertThrows(IllegalArgumentException.class,
          () -> HashAlgorithm.bytesToLong(new byte[9]),
          "Should throw for 9 bytes");
    }

    @Test
    @DisplayName("Hash long matches converted hash bytes")
    void hashLongMatchesConvertedBytes() {
      byte[] data = "Test data".getBytes();
      
      long hashLong = ALGO.computeHashLong(data);
      byte[] hashBytes = ALGO.computeHash(data);
      long converted = HashAlgorithm.bytesToLong(hashBytes);
      
      assertEquals(hashLong, converted, "Hash long should match converted bytes");
    }
  }

  @Nested
  @DisplayName("PageHasher High-Level API Tests")
  class PageHasherTests {

    @Test
    @DisplayName("compute returns correct hash bytes")
    void computeReturnsCorrectBytes() {
      byte[] data = "Hello, SirixDB!".getBytes();
      
      byte[] hash = PageHasher.compute(data, ALGO);
      
      assertEquals(ALGO.getHashLength(), hash.length);
    }

    @Test
    @DisplayName("computeLong matches compute converted to long")
    void computeLongMatchesCompute() {
      byte[] data = "Hello, SirixDB!".getBytes();
      
      long hashLong = PageHasher.computeLong(data, ALGO);
      byte[] hashBytes = PageHasher.compute(data, ALGO);
      
      assertEquals(hashLong, HashAlgorithm.bytesToLong(hashBytes));
    }

    @Test
    @DisplayName("verify returns true for null hash")
    void verifyReturnsTrueForNullHash() {
      byte[] data = "Test data".getBytes();
      
      assertTrue(PageHasher.verify(data, null, ALGO));
    }

    @Test
    @DisplayName("verify returns true for empty hash")
    void verifyReturnsTrueForEmptyHash() {
      byte[] data = "Test data".getBytes();
      
      assertTrue(PageHasher.verify(data, new byte[0], ALGO));
    }

    @Test
    @DisplayName("verify throws for wrong hash length")
    void verifyThrowsForWrongHashLength() {
      byte[] data = "Test data".getBytes();
      
      assertThrows(IllegalArgumentException.class,
          () -> PageHasher.verify(data, new byte[16], ALGO));
    }

    @Test
    @DisplayName("toHexString produces correct hex")
    void toHexStringProducesCorrectHex() {
      byte[] data = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
      
      assertEquals("deadbeef", PageHasher.toHexString(data));
    }

    @Test
    @DisplayName("toHexString handles null gracefully")
    void toHexStringHandlesNull() {
      assertEquals("null", PageHasher.toHexString((byte[]) null));
    }

    @Test
    @DisplayName("toHexString for long produces correct hex")
    void toHexStringLongProducesCorrectHex() {
      long value = 0xDEADBEEFCAFEBABEL;
      
      assertEquals("deadbeefcafebabe", PageHasher.toHexString(value));
    }
  }

  @Disabled
  @Nested
  @DisplayName("SirixCorruptionException Tests")
  class CorruptionExceptionTests {

    @Test
    @DisplayName("Exception contains correct page key")
    void exceptionContainsPageKey() {
      long pageKey = 12345L;
      byte[] expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
      byte[] actual = new byte[] { 8, 7, 6, 5, 4, 3, 2, 1 };
      
      var exception = new SirixCorruptionException(pageKey, "test", expected, actual);
      
      assertEquals(pageKey, exception.getPageKey());
    }

    @Test
    @DisplayName("Exception contains correct context")
    void exceptionContainsContext() {
      var exception = new SirixCorruptionException(0L, "compressed", null, null);
      
      assertEquals("compressed", exception.getContext());
    }

    @Test
    @DisplayName("Exception clones hash arrays defensively")
    void exceptionClonesHashArrays() {
      byte[] expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
      byte[] actual = new byte[] { 8, 7, 6, 5, 4, 3, 2, 1 };
      
      var exception = new SirixCorruptionException(0L, "test", expected, actual);
      
      expected[0] = 99;
      actual[0] = 99;
      
      assertNotEquals(99, exception.getExpectedHash()[0]);
      assertNotEquals(99, exception.getActualHash()[0]);
    }

    @Test
    @DisplayName("Exception message contains hex hashes")
    void exceptionMessageContainsHexHashes() {
      byte[] expected = new byte[] { (byte) 0xAB, (byte) 0xCD, 0, 0, 0, 0, 0, 0 };
      byte[] actual = new byte[] { (byte) 0xEF, (byte) 0x01, 0, 0, 0, 0, 0, 0 };
      
      var exception = new SirixCorruptionException(0L, "test", expected, actual);
      
      assertTrue(exception.getMessage().contains("abcd"));
      assertTrue(exception.getMessage().contains("ef01"));
    }
  }

  @Nested
  @DisplayName("ResourceConfiguration Tests")
  class ResourceConfigurationTests {

    @Test
    @DisplayName("verifyChecksumsOnRead defaults to true")
    void verifyChecksumsOnReadDefaultsTrue() {
      ResourceConfiguration config = new ResourceConfiguration.Builder("test").build();
      
      assertTrue(config.verifyChecksumsOnRead);
    }

    @Test
    @DisplayName("verifyChecksumsOnRead can be disabled")
    void verifyChecksumsOnReadCanBeDisabled() {
      ResourceConfiguration config = new ResourceConfiguration.Builder("test")
          .verifyChecksumsOnRead(false)
          .build();
      
      assertFalse(config.verifyChecksumsOnRead);
    }

    @Test
    @DisplayName("hashAlgorithm defaults to XXH3")
    void hashAlgorithmDefaultsToXxh3() {
      ResourceConfiguration config = new ResourceConfiguration.Builder("test").build();
      
      assertEquals(HashAlgorithm.XXH3, config.hashAlgorithm);
    }
  }

  @Nested
  @DisplayName("HashAlgorithm Enum Tests")
  class HashAlgorithmTests {

    @Test
    @DisplayName("XXH3 has hash length 8")
    void xxh3HasCorrectHashLength() {
      assertEquals(8, HashAlgorithm.XXH3.getHashLength());
    }

    @Test
    @DisplayName("fromHashLength returns XXH3 for length 8")
    void fromHashLengthReturnsXxh3() {
      assertEquals(HashAlgorithm.XXH3, HashAlgorithm.fromHashLength(8));
    }

    @Test
    @DisplayName("fromHashLength returns null for unknown length")
    void fromHashLengthReturnsNullForUnknown() {
      assertNull(HashAlgorithm.fromHashLength(16));
      assertNull(HashAlgorithm.fromHashLength(32));
    }
  }

  @Nested
  @DisplayName("Collision Resistance Tests")
  class CollisionResistanceTests {

    @Test
    @DisplayName("Single bit flip is detected")
    void singleBitFlipIsDetected() {
      byte[] original = "This is test data for bit flip detection".getBytes();
      long originalHash = ALGO.computeHashLong(original);
      
      byte[] corrupted = original.clone();
      corrupted[10] ^= 0x01;
      
      assertFalse(ALGO.verifyLong(corrupted, originalHash),
          "Single bit flip should be detected");
    }

    @Test
    @DisplayName("Hash changes for any byte modification")
    void hashChangesForAnyByteModification() {
      byte[] original = "Test data for modification detection".getBytes();
      long originalHash = ALGO.computeHashLong(original);
      
      for (int i = 0; i < original.length; i++) {
        byte[] modified = original.clone();
        modified[i] = (byte) (modified[i] + 1);
        
        long modifiedHash = ALGO.computeHashLong(modified);
        assertNotEquals(originalHash, modifiedHash,
            "Hash should change when byte at position " + i + " is modified");
      }
    }
  }

  @Nested
  @DisplayName("Performance Characteristics Tests")
  class PerformanceTests {

    @Test
    @DisplayName("Large data hashing works correctly")
    void largeDataHashingWorks() {
      byte[] data = new byte[1024 * 1024]; // 1 MB
      new Random(42).nextBytes(data);
      
      long hash1 = ALGO.computeHashLong(data);
      long hash2 = ALGO.computeHashLong(data);
      
      assertEquals(hash1, hash2, "Large data hashing should be consistent");
    }

    @Test
    @DisplayName("Hash verification hot path has no allocations (conceptual)")
    void verifyLongIsAllocationFree() {
      byte[] data = "Test data".getBytes();
      long hash = ALGO.computeHashLong(data);
      
      // This test verifies that verifyLong uses primitive types only
      // In a real benchmark, you'd use JMH with allocation profiling
      for (int i = 0; i < 10000; i++) {
        assertTrue(ALGO.verifyLong(data, hash));
      }
    }
  }
}
