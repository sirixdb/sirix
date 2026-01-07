package io.sirix.io;

import net.openhft.hashing.LongHashFunction;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Hash algorithms for page checksum verification.
 * 
 * <p>The algorithm is identified by its hash length, enabling automatic detection
 * when reading pages written with different algorithms.</p>
 * 
 * <p>To add a new algorithm:
 * <ol>
 *   <li>Add a new enum constant with a unique hash length</li>
 *   <li>Implement {@link #computeHash(byte[])} and {@link #computeHash(MemorySegment)}</li>
 *   <li>Register the hash length in {@link #fromHashLength(int)}</li>
 * </ol>
 * </p>
 */
public enum HashAlgorithm {
  
  /**
   * XXH3 - Extremely fast non-cryptographic hash (~15 GB/s).
   * 
   * <p>Default algorithm. Uses zero-copy hashing for native memory segments.</p>
   */
  XXH3(8) {
    private static final LongHashFunction HASHER = LongHashFunction.xx3();
    
    @Override
    public byte[] computeHash(byte[] data) {
      long hash = HASHER.hashBytes(data);
      return longToBytes(hash);
    }
    
    @Override
    public byte[] computeHash(byte[] data, int offset, int length) {
      long hash = HASHER.hashBytes(data, offset, length);
      return longToBytes(hash);
    }
    
    @Override
    public byte[] computeHash(MemorySegment segment) {
      long hash = computeHashLong(segment);
      return longToBytes(hash);
    }
    
    @Override
    public long computeHashLong(MemorySegment segment) {
      if (segment.isNative()) {
        // Zero-copy: hash directly from native memory address
        return HASHER.hashMemory(segment.address(), segment.byteSize());
      } else {
        // Heap-backed segment
        return HASHER.hashBytes(segment.toArray(ValueLayout.JAVA_BYTE));
      }
    }
    
    @Override
    public long computeHashLong(byte[] data) {
      return HASHER.hashBytes(data);
    }
    
    @Override
    public boolean verify(byte[] data, byte[] expectedHash) {
      long actualHash = HASHER.hashBytes(data);
      long expectedHashLong = bytesToLong(expectedHash);
      return actualHash == expectedHashLong;
    }
    
    @Override
    public boolean verify(MemorySegment segment, byte[] expectedHash) {
      long actualHash = computeHashLong(segment);
      long expectedHashLong = bytesToLong(expectedHash);
      return actualHash == expectedHashLong;
    }
  };
  
  // Future algorithms can be added here, e.g.:
  // XXH128(16) { ... },
  // HIGHWAY_HASH(32) { ... },
  
  private final int hashLength;
  
  HashAlgorithm(int hashLength) {
    this.hashLength = hashLength;
  }
  
  /**
   * Get the hash length in bytes for this algorithm.
   * 
   * @return hash length in bytes
   */
  public int getHashLength() {
    return hashLength;
  }
  
  /**
   * Compute hash of byte array.
   * 
   * @param data the data to hash
   * @return hash bytes
   */
  public abstract byte[] computeHash(byte[] data);
  
  /**
   * Compute hash of byte array range.
   * 
   * @param data   the data to hash
   * @param offset start offset
   * @param length number of bytes to hash
   * @return hash bytes
   */
  public abstract byte[] computeHash(byte[] data, int offset, int length);
  
  /**
   * Compute hash of a MemorySegment (zero-copy for native segments where supported).
   * 
   * @param segment the memory segment to hash
   * @return hash bytes
   */
  public abstract byte[] computeHash(MemorySegment segment);
  
  /**
   * Compute hash as long (for algorithms with 8-byte hashes).
   * 
   * @param segment the memory segment to hash
   * @return hash as long
   * @throws UnsupportedOperationException if hash length != 8
   */
  public long computeHashLong(MemorySegment segment) {
    if (hashLength != 8) {
      throw new UnsupportedOperationException("computeHashLong only supported for 8-byte hashes");
    }
    return bytesToLong(computeHash(segment));
  }
  
  /**
   * Compute hash as long (for algorithms with 8-byte hashes).
   * 
   * @param data the data to hash
   * @return hash as long
   * @throws UnsupportedOperationException if hash length != 8
   */
  public long computeHashLong(byte[] data) {
    if (hashLength != 8) {
      throw new UnsupportedOperationException("computeHashLong only supported for 8-byte hashes");
    }
    return bytesToLong(computeHash(data));
  }
  
  /**
   * Verify data against expected hash.
   * 
   * @param data         the data to verify
   * @param expectedHash the expected hash
   * @return true if hash matches, false otherwise
   */
  public abstract boolean verify(byte[] data, byte[] expectedHash);
  
  /**
   * Verify MemorySegment against expected hash (zero-copy where supported).
   * 
   * @param segment      the memory segment to verify
   * @param expectedHash the expected hash
   * @return true if hash matches, false otherwise
   */
  public abstract boolean verify(MemorySegment segment, byte[] expectedHash);
  
  /**
   * Get the algorithm from a hash length.
   * 
   * <p>This enables automatic detection when reading pages written with different algorithms.</p>
   * 
   * @param hashLength the hash length in bytes
   * @return the algorithm, or null if unknown
   */
  public static HashAlgorithm fromHashLength(int hashLength) {
    for (HashAlgorithm algo : values()) {
      if (algo.hashLength == hashLength) {
        return algo;
      }
    }
    return null;
  }
  
  /**
   * Convert long to 8-byte array (big-endian).
   */
  protected static byte[] longToBytes(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
  }
  
  /**
   * Convert 8-byte array to long (big-endian).
   */
  protected static long bytesToLong(byte[] bytes) {
    if (bytes.length != 8) {
      throw new IllegalArgumentException("Expected 8 bytes, got " + bytes.length);
    }
    return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong();
  }
}

