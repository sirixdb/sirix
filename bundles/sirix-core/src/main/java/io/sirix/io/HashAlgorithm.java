package io.sirix.io;

import net.openhft.hashing.LongHashFunction;

import java.lang.ref.Reference;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Hash algorithms for page checksum verification.
 * 
 * <p>
 * The algorithm is identified by its hash length, enabling automatic detection when reading pages
 * written with different algorithms.
 * </p>
 * 
 * <p>
 * Performance notes (aligned with financial/HFT systems best practices):
 * <ul>
 * <li>Hash computation returns {@code long} to avoid byte[] allocation in hot paths</li>
 * <li>Verification uses primitive comparison instead of Arrays.equals()</li>
 * <li>Bit manipulation used instead of ByteBuffer for zero-allocation conversion</li>
 * <li>Native memory segments use zero-copy hashing</li>
 * </ul>
 * </p>
 * 
 * <p>
 * To add a new algorithm:
 * <ol>
 * <li>Add a new enum constant with a unique hash length</li>
 * <li>Implement the abstract methods</li>
 * <li>The hash length enables automatic detection via {@link #fromHashLength(int)}</li>
 * </ol>
 * </p>
 */
public enum HashAlgorithm {

  /**
   * XXH3 - Extremely fast non-cryptographic hash (~15 GB/s).
   * 
   * <p>
   * Default algorithm. Uses zero-copy hashing for native memory segments. Single-threaded throughput:
   * ~15 GB/s on modern hardware.
   * </p>
   */
  XXH3(8) {
    private static final LongHashFunction HASHER = LongHashFunction.xx3();

    @Override
    public long computeHashLong(byte[] data) {
      // The custom accesses feed the SAME xx3 function without sun.misc.Unsafe — since JDK 25 the
      // library's default access pays Unsafe.beforeMemoryAccess() per read, measured at up to ~10%
      // of a bulk load's CPU. Hash values are bit-identical (see HashAccesses); persisted checksums
      // stay valid.
      return HASHER.hash(data, HashAccesses.BYTES, 0, data.length);
    }

    @Override
    public long computeHashLong(byte[] data, int offset, int length) {
      return HASHER.hash(data, HashAccesses.BYTES, offset, length);
    }

    @Override
    public long computeHashLong(MemorySegment segment) {
      // This public API accepts arbitrary FFM segments, including closeable shared arenas. Keep the
      // checked segment access here: an isAlive() pre-check cannot pin a shared arena, so handing its
      // raw address to native code could race a concurrent close and dereference unmapped memory.
      if (segment.isNative()) {
        requireAccessibleNativeSegment(segment);
      }
      return HASHER.hash(segment, HashAccesses.SEGMENT, 0, segment.byteSize());
    }

    @Override
    public long computeHashLong(final ByteBuffer buffer) {
      final int length = buffer.remaining();
      if (buffer.hasArray()) {
        return HASHER.hash(buffer.array(), HashAccesses.BYTES, (long) buffer.arrayOffset() + buffer.position(), length);
      }
      // FileChannel readers exclusively own their borrowed direct buffer through checksum and decode.
      // The library's own ByteBuffer kernel is never entered for a buffer without an array: linking
      // it resolves sun.nio.ch.DirectBuffer, which java.base does not export, so outside a JVM
      // started with --add-opens (Gradle's test workers add it silently) the FIRST page read of every
      // resource failed with IllegalAccessError. The public FFM view of the buffer — native for a
      // direct buffer, a read-only heap view otherwise — feeds the same xx3 kernel through the
      // segment access: bit-identical values, no Unsafe, no module flags. The view spans
      // position..limit, so no extra offset applies.
      final MemorySegment view = MemorySegment.ofBuffer(buffer);
      if (view.isNative()) {
        requireAccessibleNativeSegment(view);
      }
      try {
        return HASHER.hash(view, HashAccesses.SEGMENT, 0L, length);
      } finally {
        // The kernel sees only an address. Keep the direct buffer (and therefore its Cleaner
        // attachment) strongly reachable until the final native load has completed.
        Reference.reachabilityFence(buffer);
      }
    }

    @Override
    public boolean verifyLong(byte[] data, long expectedHash) {
      return computeHashLong(data) == expectedHash;
    }

    @Override
    public boolean verifyLong(MemorySegment segment, long expectedHash) {
      return computeHashLong(segment) == expectedHash;
    }
  };

  // Future algorithms can be added here, e.g.:
  // XXH128(16) { ... }, // Would need computeHash128() methods

  private final int hashLength;

  /**
   * Fail deterministically even for a zero-length native segment, before the hash performs no load.
   */
  private static void requireAccessibleNativeSegment(final MemorySegment segment) {
    if (!segment.scope().isAlive()) {
      throw new IllegalStateException("Cannot hash a closed memory segment");
    }
    if (!segment.isAccessibleBy(Thread.currentThread())) {
      throw new WrongThreadException("Cannot hash a memory segment confined to another thread");
    }
  }

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

  // ==================== Primary API: Long-based (zero-allocation) ====================

  /**
   * Compute hash as long (primary API - zero allocation).
   * 
   * @param data the data to hash
   * @return hash as long
   */
  public abstract long computeHashLong(byte[] data);

  /**
   * Compute hash of range as long (zero allocation).
   * 
   * @param data the data to hash
   * @param offset start offset
   * @param length number of bytes
   * @return hash as long
   */
  public abstract long computeHashLong(byte[] data, int offset, int length);

  /**
   * Compute hash of MemorySegment as long (zero-copy for native segments).
   * 
   * @param segment the memory segment to hash
   * @return hash as long
   */
  public abstract long computeHashLong(MemorySegment segment);

  /**
   * Compute a hash over the buffer's remaining bytes without changing its position.
   *
   * @param buffer source buffer
   * @return hash as long
   */
  public abstract long computeHashLong(ByteBuffer buffer);

  /**
   * Verify data against expected hash (zero allocation).
   * 
   * @param data the data to verify
   * @param expectedHash the expected hash as long
   * @return true if hash matches
   */
  public abstract boolean verifyLong(byte[] data, long expectedHash);

  /**
   * Verify MemorySegment against expected hash (zero-copy for native segments).
   * 
   * @param segment the memory segment to verify
   * @param expectedHash the expected hash as long
   * @return true if hash matches
   */
  public abstract boolean verifyLong(MemorySegment segment, long expectedHash);

  // ==================== Convenience API: Byte array-based ====================

  /**
   * Compute hash as byte array (allocates - use long version in hot paths).
   * 
   * @param data the data to hash
   * @return hash bytes
   */
  public byte[] computeHash(byte[] data) {
    return longToBytes(computeHashLong(data));
  }

  /**
   * Compute hash of range as byte array.
   * 
   * @param data the data to hash
   * @param offset start offset
   * @param length number of bytes
   * @return hash bytes
   */
  public byte[] computeHash(byte[] data, int offset, int length) {
    return longToBytes(computeHashLong(data, offset, length));
  }

  /**
   * Compute hash of MemorySegment as byte array.
   * 
   * @param segment the memory segment to hash
   * @return hash bytes
   */
  public byte[] computeHash(MemorySegment segment) {
    return longToBytes(computeHashLong(segment));
  }

  /**
   * Verify data against expected hash bytes.
   * 
   * @param data the data to verify
   * @param expectedHash the expected hash bytes
   * @return true if hash matches
   */
  public boolean verify(byte[] data, byte[] expectedHash) {
    return verifyLong(data, bytesToLong(expectedHash));
  }

  /**
   * Verify MemorySegment against expected hash bytes.
   * 
   * @param segment the memory segment to verify
   * @param expectedHash the expected hash bytes
   * @return true if hash matches
   */
  public boolean verify(MemorySegment segment, byte[] expectedHash) {
    return verifyLong(segment, bytesToLong(expectedHash));
  }

  // ==================== Algorithm detection ====================

  /**
   * Get the algorithm from a hash length.
   * 
   * <p>
   * This enables automatic detection when reading pages written with different algorithms.
   * </p>
   * 
   * @param hashLength the hash length in bytes
   * @return the algorithm, or null if unknown
   */
  public static HashAlgorithm fromHashLength(int hashLength) {
    // Fast path for common case
    if (hashLength == 8)
      return XXH3;

    // General lookup for future algorithms
    for (HashAlgorithm algo : values()) {
      if (algo.hashLength == hashLength) {
        return algo;
      }
    }
    return null;
  }

  // ==================== Conversion utilities (zero-allocation bit manipulation) ====================

  /**
   * Convert long to 8-byte array (big-endian, no ByteBuffer allocation).
   * 
   * <p>
   * Uses direct bit manipulation for maximum performance.
   * </p>
   */
  public static byte[] longToBytes(long value) {
    return new byte[] {(byte) (value >>> 56), (byte) (value >>> 48), (byte) (value >>> 40), (byte) (value >>> 32),
        (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
  }

  /**
   * Convert 8-byte array to long (big-endian, no ByteBuffer allocation).
   * 
   * <p>
   * Uses direct bit manipulation for maximum performance.
   * </p>
   */
  public static long bytesToLong(byte[] bytes) {
    if (bytes.length != 8) {
      throw new IllegalArgumentException("Expected 8 bytes, got " + bytes.length);
    }
    return ((long) (bytes[0] & 0xFF) << 56) | ((long) (bytes[1] & 0xFF) << 48) | ((long) (bytes[2] & 0xFF) << 40)
        | ((long) (bytes[3] & 0xFF) << 32) | ((long) (bytes[4] & 0xFF) << 24) | ((long) (bytes[5] & 0xFF) << 16)
        | ((long) (bytes[6] & 0xFF) << 8) | ((long) (bytes[7] & 0xFF));
  }
}
