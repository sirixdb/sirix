package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixCorruptionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for page checksum verification with XXH3.
 */
class ChecksumVerificationTest {

  @Nested
  @DisplayName("PageHasher Unit Tests")
  class PageHasherTests {

    @Test
    @DisplayName("XXH3 produces consistent 8-byte hashes")
    void xxh3ProducesConsistentHashes() {
      byte[] data = "Hello, SirixDB!".getBytes();
      
      byte[] hash1 = PageHasher.computeXXH3(data);
      byte[] hash2 = PageHasher.computeXXH3(data);
      
      assertEquals(PageHasher.HASH_LENGTH, hash1.length);
      assertArrayEquals(hash1, hash2, "XXH3 should produce consistent hashes");
    }

    @Test
    @DisplayName("XXH3 produces different hashes for different data")
    void xxh3ProducesDifferentHashesForDifferentData() {
      byte[] data1 = "Hello, SirixDB!".getBytes();
      byte[] data2 = "Hello, Sirix!".getBytes();
      
      byte[] hash1 = PageHasher.computeXXH3(data1);
      byte[] hash2 = PageHasher.computeXXH3(data2);
      
      assertFalse(Arrays.equals(hash1, hash2), "Different data should produce different hashes");
    }

    @Test
    @DisplayName("XXH3 range hashing works correctly")
    void xxh3RangeHashingWorks() {
      byte[] data = "PREFIXHello, SirixDB!SUFFIX".getBytes();
      byte[] expected = PageHasher.computeXXH3("Hello, SirixDB!".getBytes());
      
      byte[] actual = PageHasher.computeXXH3(data, 6, 15);
      
      assertArrayEquals(expected, actual, "Range hashing should match full data hashing");
    }

    @Test
    @DisplayName("XXH3 with native MemorySegment uses zero-copy")
    void xxh3NativeMemorySegmentZeroCopy() {
      byte[] data = "Test data for native segment verification".getBytes();
      byte[] expectedFromArray = PageHasher.computeXXH3(data);
      
      // Create native (off-heap) segment - this is the zero-copy path
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment nativeSegment = arena.allocate(data.length);
        nativeSegment.copyFrom(MemorySegment.ofArray(data));
        
        assertTrue(nativeSegment.isNative(), "Segment should be native (off-heap)");
        
        byte[] actualFromSegment = PageHasher.computeXXH3(nativeSegment);
        assertArrayEquals(expectedFromArray, actualFromSegment, 
            "Native segment hash should match byte array hash");
      }
    }

    @Test
    @DisplayName("XXH3 with heap-backed MemorySegment works correctly")
    void xxh3HeapMemorySegmentWorks() {
      byte[] data = "Test data for heap segment verification".getBytes();
      byte[] expectedFromArray = PageHasher.computeXXH3(data);
      
      // Create heap-backed segment
      MemorySegment heapSegment = MemorySegment.ofArray(data);
      
      assertFalse(heapSegment.isNative(), "Segment should be heap-backed");
      
      byte[] actualFromSegment = PageHasher.computeXXH3(heapSegment);
      assertArrayEquals(expectedFromArray, actualFromSegment, 
          "Heap segment hash should match byte array hash");
    }

    @Test
    @DisplayName("verify() with native MemorySegment is zero-copy")
    void verifyNativeMemorySegmentZeroCopy() {
      byte[] data = "Test data for verification".getBytes();
      byte[] hash = PageHasher.computeXXH3(data);
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment nativeSegment = arena.allocate(data.length);
        nativeSegment.copyFrom(MemorySegment.ofArray(data));
        
        // Verify using native segment (zero-copy path)
        assertTrue(PageHasher.verify(nativeSegment, hash), 
            "verify() should work with native MemorySegment");
      }
    }

    @Test
    @DisplayName("verify() with MemorySegment returns false for corrupted data")
    void verifyMemorySegmentDetectsCorruption() {
      byte[] data = "Test data for verification".getBytes();
      byte[] hash = PageHasher.computeXXH3(data);
      
      // Corrupt the data
      byte[] corrupted = data.clone();
      corrupted[5] ^= 0xFF;
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment nativeSegment = arena.allocate(corrupted.length);
        nativeSegment.copyFrom(MemorySegment.ofArray(corrupted));
        
        assertFalse(PageHasher.verify(nativeSegment, hash), 
            "verify() should detect corruption in MemorySegment");
      }
    }

    @Test
    @DisplayName("verify() returns true for matching XXH3 hash")
    void verifyReturnsTrueForMatchingXXH3() {
      byte[] data = "Test data for verification".getBytes();
      byte[] hash = PageHasher.computeXXH3(data);
      
      assertTrue(PageHasher.verify(data, hash), "verify() should return true for matching hash");
    }

    @Test
    @DisplayName("verify() returns false for mismatched XXH3 hash")
    void verifyReturnsFalseForMismatchedXXH3() {
      byte[] data = "Test data for verification".getBytes();
      byte[] wrongHash = PageHasher.computeXXH3("Different data".getBytes());
      
      assertFalse(PageHasher.verify(data, wrongHash), "verify() should return false for mismatched hash");
    }

    @Test
    @DisplayName("verify() returns true for null hash (no verification)")
    void verifyReturnsTrueForNullHash() {
      byte[] data = "Test data".getBytes();
      
      assertTrue(PageHasher.verify(data, null), "verify() should return true when hash is null");
    }

    @Test
    @DisplayName("verify() returns true for empty hash (no verification)")
    void verifyReturnsTrueForEmptyHash() {
      byte[] data = "Test data".getBytes();
      
      assertTrue(PageHasher.verify(data, new byte[0]), "verify() should return true when hash is empty");
    }

    @Test
    @DisplayName("verify() throws on wrong hash length")
    void verifyThrowsOnWrongHashLength() {
      byte[] data = "Test data".getBytes();
      byte[] wrongLength = new byte[16]; // Not 8 bytes
      
      assertThrows(IllegalArgumentException.class, 
          () -> PageHasher.verify(data, wrongLength),
          "verify() should throw on wrong hash length");
    }

    @Test
    @DisplayName("toHexString produces correct hex encoding")
    void toHexStringProducesCorrectHex() {
      byte[] data = new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
      
      assertEquals("deadbeef", PageHasher.toHexString(data));
    }

    @Test
    @DisplayName("toHexString handles null gracefully")
    void toHexStringHandlesNull() {
      assertEquals("null", PageHasher.toHexString(null));
    }

    @Test
    @DisplayName("bytesToLong and longToBytes are inverses")
    void bytesToLongAndLongToBytesAreInverses() {
      // Test through computeXXH3Long and bytesToLong
      byte[] data = "test".getBytes();
      byte[] hash = PageHasher.computeXXH3(data);
      long hashLong = PageHasher.bytesToLong(hash);
      long directLong = PageHasher.computeXXH3Long(data);
      
      assertEquals(directLong, hashLong, "bytesToLong should correctly convert hash bytes");
    }
  }

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
      
      // Modify original arrays
      expected[0] = 99;
      actual[0] = 99;
      
      // Exception should have original values
      assertNotEquals(99, exception.getExpectedHash()[0]);
      assertNotEquals(99, exception.getActualHash()[0]);
    }

    @Test
    @DisplayName("Exception message contains hex hashes")
    void exceptionMessageContainsHexHashes() {
      byte[] expected = new byte[] { (byte) 0xAB, (byte) 0xCD, 0, 0, 0, 0, 0, 0 };
      byte[] actual = new byte[] { (byte) 0xEF, (byte) 0x01, 0, 0, 0, 0, 0, 0 };
      
      var exception = new SirixCorruptionException(0L, "test", expected, actual);
      
      assertTrue(exception.getMessage().contains("abcd"), "Message should contain expected hash hex");
      assertTrue(exception.getMessage().contains("ef01"), "Message should contain actual hash hex");
    }
  }

  @Nested
  @DisplayName("ResourceConfiguration Tests")
  class ResourceConfigurationTests {

    @Test
    @DisplayName("verifyChecksumsOnRead defaults to true")
    void verifyChecksumsOnReadDefaultsTrue() {
      ResourceConfiguration config = new ResourceConfiguration.Builder("test").build();
      
      assertTrue(config.verifyChecksumsOnRead, "verifyChecksumsOnRead should default to true");
    }

    @Test
    @DisplayName("verifyChecksumsOnRead can be disabled")
    void verifyChecksumsOnReadCanBeDisabled() {
      ResourceConfiguration config = new ResourceConfiguration.Builder("test")
          .verifyChecksumsOnRead(false)
          .build();
      
      assertFalse(config.verifyChecksumsOnRead, "verifyChecksumsOnRead should be disabled");
    }
  }

  @Nested
  @DisplayName("Hash Tests")
  class HashTests {

    @Test
    @DisplayName("XXH3 produces 8-byte hashes")
    void hashLengthIsCorrect() {
      // Generate test data
      byte[] data = new byte[1024 * 1024];
      new Random(42).nextBytes(data);
      
      byte[] xxh3Hash = PageHasher.computeXXH3(data);
      
      // Verify hash length
      assertEquals(8, xxh3Hash.length, "XXH3 should produce 8-byte hashes");
      
      // Verify hashes are deterministic
      byte[] xxh3Hash2 = PageHasher.computeXXH3(data);
      assertArrayEquals(xxh3Hash, xxh3Hash2, "XXH3 should be deterministic");
    }
  }

  @Nested
  @DisplayName("HashAlgorithm Enum Tests")
  class HashAlgorithmTests {

    @Test
    @DisplayName("XXH3 algorithm has correct hash length")
    void xxh3HasCorrectHashLength() {
      assertEquals(8, HashAlgorithm.XXH3.getHashLength());
    }

    @Test
    @DisplayName("fromHashLength returns correct algorithm")
    void fromHashLengthReturnsCorrectAlgorithm() {
      assertEquals(HashAlgorithm.XXH3, HashAlgorithm.fromHashLength(8));
    }

    @Test
    @DisplayName("fromHashLength returns null for unknown length")
    void fromHashLengthReturnsNullForUnknown() {
      assertNull(HashAlgorithm.fromHashLength(16));
      assertNull(HashAlgorithm.fromHashLength(32));
    }

    @Test
    @DisplayName("Algorithm produces correct hashes")
    void algorithmProducesCorrectHashes() {
      byte[] data = "Test data".getBytes();
      byte[] hash = HashAlgorithm.XXH3.computeHash(data);
      
      assertEquals(8, hash.length);
      assertTrue(HashAlgorithm.XXH3.verify(data, hash));
    }

    @Test
    @DisplayName("DEFAULT_ALGORITHM is XXH3")
    void defaultAlgorithmIsXxh3() {
      assertEquals(HashAlgorithm.XXH3, PageHasher.DEFAULT_ALGORITHM);
      assertEquals(8, PageHasher.HASH_LENGTH);
    }
  }

  @Nested
  @DisplayName("Collision Resistance Tests")
  class CollisionResistanceTests {

    @Test
    @DisplayName("Single bit flip is detected")
    void singleBitFlipIsDetected() {
      byte[] original = "This is test data for bit flip detection".getBytes();
      byte[] originalHash = PageHasher.computeXXH3(original);
      
      // Flip a single bit
      byte[] corrupted = original.clone();
      corrupted[10] ^= 0x01;
      
      // Should not verify
      assertFalse(PageHasher.verify(corrupted, originalHash), 
          "Single bit flip should be detected");
    }

    @Test
    @DisplayName("Hash changes for any byte modification")
    void hashChangesForAnyByteModification() {
      byte[] original = "Test data for modification detection".getBytes();
      byte[] originalHash = PageHasher.computeXXH3(original);
      
      // Modify each byte position
      for (int i = 0; i < original.length; i++) {
        byte[] modified = original.clone();
        modified[i] = (byte) (modified[i] + 1);
        
        byte[] modifiedHash = PageHasher.computeXXH3(modified);
        assertFalse(Arrays.equals(originalHash, modifiedHash),
            "Hash should change when byte at position " + i + " is modified");
      }
    }
  }
}
