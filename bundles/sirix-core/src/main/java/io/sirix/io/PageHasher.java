package io.sirix.io;

import java.lang.foreign.MemorySegment;

/**
 * Page checksum computation and verification.
 * 
 * <p>Supports multiple hash algorithms with automatic detection based on hash length.
 * Currently uses XXH3 (~15 GB/s) as the default. New algorithms can be added to
 * {@link HashAlgorithm} for future extensibility.</p>
 * 
 * <p>For native (off-heap) memory segments, verification is zero-copy.</p>
 */
public final class PageHasher {
  
  /**
   * Default hash algorithm for new pages.
   */
  public static final HashAlgorithm DEFAULT_ALGORITHM = HashAlgorithm.XXH3;
  
  /**
   * Hash length for the default algorithm.
   */
  public static final int HASH_LENGTH = DEFAULT_ALGORITHM.getHashLength();
  
  private PageHasher() {
    // Utility class
  }
  
  // ==================== Convenience methods using default algorithm ====================
  
  /**
   * Compute hash of byte array using default algorithm (XXH3).
   * 
   * @param data the data to hash
   * @return hash bytes
   */
  public static byte[] computeXXH3(byte[] data) {
    return DEFAULT_ALGORITHM.computeHash(data);
  }
  
  /**
   * Compute hash of byte array range using default algorithm (XXH3).
   * 
   * @param data   the data to hash
   * @param offset start offset
   * @param length number of bytes to hash
   * @return hash bytes
   */
  public static byte[] computeXXH3(byte[] data, int offset, int length) {
    return DEFAULT_ALGORITHM.computeHash(data, offset, length);
  }
  
  /**
   * Compute hash of a MemorySegment using default algorithm (XXH3).
   * Zero-copy for native segments.
   * 
   * @param segment the memory segment to hash
   * @return hash bytes
   */
  public static byte[] computeXXH3(MemorySegment segment) {
    return DEFAULT_ALGORITHM.computeHash(segment);
  }
  
  /**
   * Compute hash as long using default algorithm (XXH3).
   * Zero-copy for native segments, avoids byte[] allocation.
   * 
   * @param segment the memory segment to hash
   * @return hash as long
   */
  public static long computeXXH3Long(MemorySegment segment) {
    return DEFAULT_ALGORITHM.computeHashLong(segment);
  }
  
  /**
   * Compute hash as long using default algorithm (XXH3).
   * Avoids byte[] allocation.
   * 
   * @param data the data to hash
   * @return hash as long
   */
  public static long computeXXH3Long(byte[] data) {
    return DEFAULT_ALGORITHM.computeHashLong(data);
  }
  
  // ==================== Verification with auto-detection ====================
  
  /**
   * Verify data against expected hash with automatic algorithm detection.
   * 
   * <p>Algorithm is detected by hash length. If no matching algorithm is found,
   * throws IllegalArgumentException.</p>
   * 
   * @param data         the data to verify
   * @param expectedHash the expected hash (may be null)
   * @return true if hash matches or expectedHash is null/empty, false on mismatch
   * @throws IllegalArgumentException if hash length doesn't match any known algorithm
   */
  public static boolean verify(byte[] data, byte[] expectedHash) {
    if (expectedHash == null || expectedHash.length == 0) {
      return true; // No hash to verify
    }
    
    HashAlgorithm algo = HashAlgorithm.fromHashLength(expectedHash.length);
    if (algo == null) {
      throw new IllegalArgumentException(
          "Unknown hash algorithm for length " + expectedHash.length + 
          ". Known lengths: " + knownHashLengths());
    }
    
    return algo.verify(data, expectedHash);
  }
  
  /**
   * Verify MemorySegment against expected hash with automatic algorithm detection.
   * Zero-copy for native segments with supported algorithms.
   * 
   * @param segment      the memory segment to verify
   * @param expectedHash the expected hash (may be null)
   * @return true if hash matches or expectedHash is null/empty, false on mismatch
   * @throws IllegalArgumentException if hash length doesn't match any known algorithm
   */
  public static boolean verify(MemorySegment segment, byte[] expectedHash) {
    if (expectedHash == null || expectedHash.length == 0) {
      return true; // No hash to verify
    }
    
    HashAlgorithm algo = HashAlgorithm.fromHashLength(expectedHash.length);
    if (algo == null) {
      throw new IllegalArgumentException(
          "Unknown hash algorithm for length " + expectedHash.length + 
          ". Known lengths: " + knownHashLengths());
    }
    
    return algo.verify(segment, expectedHash);
  }
  
  /**
   * Verify data range against expected hash with automatic algorithm detection.
   * 
   * @param data         the data array
   * @param offset       start offset
   * @param length       number of bytes to verify
   * @param expectedHash the expected hash (may be null)
   * @return true if hash matches or expectedHash is null/empty, false on mismatch
   * @throws IllegalArgumentException if hash length doesn't match any known algorithm
   */
  public static boolean verify(byte[] data, int offset, int length, byte[] expectedHash) {
    if (expectedHash == null || expectedHash.length == 0) {
      return true;
    }
    
    HashAlgorithm algo = HashAlgorithm.fromHashLength(expectedHash.length);
    if (algo == null) {
      throw new IllegalArgumentException(
          "Unknown hash algorithm for length " + expectedHash.length);
    }
    
    // Compute hash of the range and compare
    byte[] actualHash = algo.computeHash(data, offset, length);
    return java.util.Arrays.equals(actualHash, expectedHash);
  }
  
  // ==================== Utility methods ====================
  
  /**
   * Compute actual hash for error reporting using default algorithm.
   * 
   * @param data the data that was hashed
   * @return the computed hash
   */
  public static byte[] computeActualHash(byte[] data) {
    return DEFAULT_ALGORITHM.computeHash(data);
  }
  
  /**
   * Compute actual hash from MemorySegment for error reporting using default algorithm.
   * 
   * @param segment the segment that was hashed
   * @return the computed hash
   */
  public static byte[] computeActualHash(MemorySegment segment) {
    return DEFAULT_ALGORITHM.computeHash(segment);
  }
  
  /**
   * Convert 8-byte array to long (big-endian).
   */
  public static long bytesToLong(byte[] bytes) {
    return HashAlgorithm.bytesToLong(bytes);
  }
  
  /**
   * Format hash as hex string for logging.
   */
  public static String toHexString(byte[] hash) {
    if (hash == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder(hash.length * 2);
    for (byte b : hash) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
  
  /**
   * Get a string listing known hash lengths for error messages.
   */
  private static String knownHashLengths() {
    StringBuilder sb = new StringBuilder();
    for (HashAlgorithm algo : HashAlgorithm.values()) {
      if (sb.length() > 0) sb.append(", ");
      sb.append(algo.getHashLength()).append(" (").append(algo.name()).append(")");
    }
    return sb.toString();
  }
}
