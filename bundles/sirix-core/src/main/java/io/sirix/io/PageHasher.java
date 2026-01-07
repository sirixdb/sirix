package io.sirix.io;

import java.lang.foreign.MemorySegment;

/**
 * High-performance page checksum computation and verification.
 * 
 * <p>Design principles (aligned with financial/HFT systems):
 * <ul>
 *   <li><b>Zero allocation in hot paths:</b> Use {@code long}-based methods for verification</li>
 *   <li><b>Zero-copy:</b> Native memory segments are hashed directly via address</li>
 *   <li><b>No ByteBuffer:</b> Direct bit manipulation for conversions</li>
 *   <li><b>Primitive comparison:</b> Long equality instead of Arrays.equals()</li>
 * </ul>
 * </p>
 * 
 * <p>Default algorithm: XXH3 (~15 GB/s throughput)</p>
 */
public final class PageHasher {

  /** Default hash algorithm for pages. */
  public static final HashAlgorithm DEFAULT_ALGORITHM = HashAlgorithm.XXH3;
  
  /** Hash length in bytes for the default algorithm. */
  public static final int HASH_LENGTH = DEFAULT_ALGORITHM.getHashLength();

  private PageHasher() {
    // Utility class
  }
  
  // ==================== Convenience API: Default XXH3 algorithm ====================
  
  /**
   * Compute hash using default XXH3 algorithm.
   *
   * @param data the data to hash
   * @return hash bytes
   */
  public static byte[] compute(byte[] data) {
    return DEFAULT_ALGORITHM.computeHash(data);
  }
  
  /**
   * Compute hash as long using default XXH3 algorithm (zero allocation).
   *
   * @param data the data to hash
   * @return hash as long
   */
  public static long computeLong(byte[] data) {
    return DEFAULT_ALGORITHM.computeHashLong(data);
  }

  // ==================== Primary API: Long-based (zero-allocation hot path) ====================

  /**
   * Compute hash as long (zero allocation - preferred for writes).
   *
   * @param data          the data to hash
   * @param hashAlgorithm the algorithm to use
   * @return hash as long
   */
  public static long computeLong(byte[] data, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHashLong(data);
  }

  /**
   * Compute hash of range as long (zero allocation).
   *
   * @param data          the data to hash
   * @param offset        start offset
   * @param length        number of bytes to hash
   * @param hashAlgorithm the algorithm to use
   * @return hash as long
   */
  public static long computeLong(byte[] data, int offset, int length, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHashLong(data, offset, length);
  }

  /**
   * Compute hash of MemorySegment as long (zero-copy for native segments).
   *
   * @param segment       the memory segment to hash
   * @param hashAlgorithm the algorithm to use
   * @return hash as long
   */
  public static long computeLong(MemorySegment segment, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHashLong(segment);
  }

  /**
   * Verify data against expected hash (zero allocation).
   *
   * @param data          the data to verify
   * @param expectedHash  the expected hash as long
   * @param hashAlgorithm the algorithm to use
   * @return true if hash matches
   */
  public static boolean verifyLong(byte[] data, long expectedHash, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.verifyLong(data, expectedHash);
  }

  /**
   * Verify MemorySegment against expected hash (zero-copy for native segments).
   *
   * @param segment       the memory segment to verify
   * @param expectedHash  the expected hash as long
   * @param hashAlgorithm the algorithm to use
   * @return true if hash matches
   */
  public static boolean verifyLong(MemorySegment segment, long expectedHash, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.verifyLong(segment, expectedHash);
  }

  // ==================== Secondary API: Byte array-based (for storage) ====================

  /**
   * Compute hash of byte array.
   *
   * @param data          the data to hash
   * @param hashAlgorithm the algorithm to use
   * @return hash bytes
   */
  public static byte[] compute(byte[] data, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHash(data);
  }

  /**
   * Compute hash of byte array range.
   *
   * @param data          the data to hash
   * @param offset        start offset
   * @param length        number of bytes to hash
   * @param hashAlgorithm the algorithm to use
   * @return hash bytes
   */
  public static byte[] compute(byte[] data, int offset, int length, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHash(data, offset, length);
  }

  /**
   * Compute hash of a MemorySegment (zero-copy for native segments).
   *
   * @param segment       the memory segment to hash
   * @param hashAlgorithm the algorithm to use
   * @return hash bytes
   */
  public static byte[] compute(MemorySegment segment, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHash(segment);
  }

  /**
   * Verify data against expected hash.
   *
   * @param data          the data to verify
   * @param expectedHash  the expected hash (may be null)
   * @param hashAlgorithm the algorithm to use
   * @return true if hash matches or verification is skipped
   * @throws IllegalArgumentException if expectedHash length does not match algorithm's hash length
   */
  public static boolean verify(byte[] data, byte[] expectedHash, HashAlgorithm hashAlgorithm) {
    if (expectedHash == null || expectedHash.length == 0) {
      return true; // No hash to verify
    }

    if (expectedHash.length != hashAlgorithm.getHashLength()) {
      throw new IllegalArgumentException(
          "Expected hash length " + hashAlgorithm.getHashLength() + ", but got " + expectedHash.length);
    }

    return hashAlgorithm.verify(data, expectedHash);
  }

  /**
   * Verify MemorySegment against expected hash (zero-copy for native segments).
   *
   * @param segment       the memory segment to verify
   * @param expectedHash  the expected hash (may be null)
   * @param hashAlgorithm the algorithm to use
   * @return true if hash matches or verification is skipped
   * @throws IllegalArgumentException if expectedHash length does not match algorithm's hash length
   */
  public static boolean verify(MemorySegment segment, byte[] expectedHash, HashAlgorithm hashAlgorithm) {
    if (expectedHash == null || expectedHash.length == 0) {
      return true; // No hash to verify
    }

    if (expectedHash.length != hashAlgorithm.getHashLength()) {
      throw new IllegalArgumentException(
          "Expected hash length " + hashAlgorithm.getHashLength() + ", but got " + expectedHash.length);
    }

    return hashAlgorithm.verify(segment, expectedHash);
  }

  /**
   * Verify data range against expected hash.
   *
   * @param data          the data array
   * @param offset        start offset
   * @param length        number of bytes to verify
   * @param expectedHash  the expected hash (may be null)
   * @param hashAlgorithm the algorithm to use
   * @return true if hash matches or verification is skipped
   * @throws IllegalArgumentException if expectedHash length does not match algorithm's hash length
   */
  public static boolean verify(byte[] data, int offset, int length, byte[] expectedHash,
      HashAlgorithm hashAlgorithm) {
    if (expectedHash == null || expectedHash.length == 0) {
      return true;
    }

    if (expectedHash.length != hashAlgorithm.getHashLength()) {
      throw new IllegalArgumentException(
          "Expected hash length " + hashAlgorithm.getHashLength() + ", but got " + expectedHash.length);
    }

    long expected = HashAlgorithm.bytesToLong(expectedHash);
    long actual = hashAlgorithm.computeHashLong(data, offset, length);
    return actual == expected;
  }

  // ==================== Diagnostic utilities ====================

  /**
   * Compute actual hash for error reporting.
   *
   * @param data          the data that was hashed
   * @param hashAlgorithm the algorithm to use
   * @return the computed hash
   */
  public static byte[] computeActualHash(byte[] data, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHash(data);
  }

  /**
   * Compute actual hash from MemorySegment for error reporting.
   *
   * @param segment       the segment that was hashed
   * @param hashAlgorithm the algorithm to use
   * @return the computed hash
   */
  public static byte[] computeActualHash(MemorySegment segment, HashAlgorithm hashAlgorithm) {
    return hashAlgorithm.computeHash(segment);
  }

  /**
   * Format hash as hex string for logging.
   *
   * @param hash the hash bytes
   * @return hex string representation
   */
  public static String toHexString(byte[] hash) {
    if (hash == null) {
      return "null";
    }
    // Pre-sized StringBuilder for exact capacity (2 chars per byte)
    StringBuilder sb = new StringBuilder(hash.length << 1);
    for (byte b : hash) {
      sb.append(HEX_CHARS[(b >>> 4) & 0x0F]);
      sb.append(HEX_CHARS[b & 0x0F]);
    }
    return sb.toString();
  }

  /**
   * Format hash long as hex string for logging.
   *
   * @param hash the hash as long
   * @return hex string representation
   */
  public static String toHexString(long hash) {
    // 16 hex chars for a long
    char[] chars = new char[16];
    for (int i = 15; i >= 0; i--) {
      chars[i] = HEX_CHARS[(int) (hash & 0x0F)];
      hash >>>= 4;
    }
    return new String(chars);
  }

  // Lookup table for hex conversion (faster than String.format)
  private static final char[] HEX_CHARS = {
      '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };
}
