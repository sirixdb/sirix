package io.sirix.io.bytepipe;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface for the decorator, representing any byte representation to be serialized or to
 * serialize.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public interface ByteHandler {

  /**
   * Result of scoped decompression - must be closed after use to return buffer to pool.
   * 
   * <p>This is the Loom-friendly alternative to ThreadLocal buffer caching.
   * Memory is bounded by pool size (typically 2×CPU cores), not thread count.
   * 
   * <p>For zero-copy page deserialization, ownership of the backing buffer can be transferred
   * to the page via {@link #transferOwnership()}. When ownership is transferred, the buffer
   * is NOT returned to the pool when close() is called - the page becomes responsible for
   * releasing it when the page is closed/evicted.
   * 
   * <p>Usage (normal - buffer returned to pool):
   * <pre>{@code
   * try (var result = byteHandler.decompressScoped(compressed)) {
   *     Page page = deserialize(result.segment());
   *     return page;
   * } // Buffer automatically returned to pool
   * }</pre>
   * 
   * <p>Usage (zero-copy - buffer ownership transferred to page):
   * <pre>{@code
   * var result = byteHandler.decompressScoped(compressed);
   * try {
   *     // Page takes ownership via transferOwnership()
   *     Page page = deserializeWithZeroCopy(result);
   *     return page;
   * } finally {
   *     // Only releases if ownership wasn't transferred
   *     result.close();
   * }
   * }</pre>
   * 
   * @param segment Decompressed data (may be a slice of backingBuffer)
   * @param backingBuffer Full allocated buffer (for zero-copy ownership transfer)
   * @param releaser Returns buffer to allocator when called
   * @param ownershipTransferred Tracks whether ownership was transferred to prevent double-release
   */
  record DecompressionResult(
      MemorySegment segment,
      MemorySegment backingBuffer,
      Runnable releaser,
      AtomicBoolean ownershipTransferred
  ) implements AutoCloseable {
    
    /**
     * Compact constructor for backwards compatibility (creates non-transferable result).
     */
    public DecompressionResult(MemorySegment segment, Runnable releaser) {
      this(segment, segment, releaser, new AtomicBoolean(false));
    }
    
    /**
     * Transfer buffer ownership to caller (typically a page for zero-copy).
     * After this call, close() becomes a no-op and caller is responsible for releasing.
     * 
     * <p>This enables zero-copy page deserialization where the decompression buffer
     * becomes the page's slotMemory directly, avoiding a copy.
     * 
     * @return the releaser to call when done, or null if already transferred
     */
    public Runnable transferOwnership() {
      if (ownershipTransferred.compareAndSet(false, true)) {
        return releaser;
      }
      return null;  // Already transferred
    }
    
    @Override
    public void close() {
      // Only release if ownership wasn't transferred
      if (!ownershipTransferred.get() && releaser != null) {
        releaser.run();
      }
    }
  }

  /**
   * Method to serialize any byte-chunk.
   *
   * @param toSerialize byte to be serialized
   * @return result of the serialization
   */
  OutputStream serialize(OutputStream toSerialize);

  /**
   * Method to deserialize any byte-chunk.
   *
   * @param toDeserialize to deserialize
   * @return result of the deserialization
   */
  InputStream deserialize(InputStream toDeserialize);

  /**
   * Method to retrieve a new instance.
   *
   * @return new instance
   */
  ByteHandler getInstance();

  /**
   * Compress data using MemorySegment (zero-copy).
   * Default implementation throws UnsupportedOperationException.
   *
   * @param source uncompressed data
   * @return compressed data in a MemorySegment
   */
  default MemorySegment compress(MemorySegment source) {
    throw new UnsupportedOperationException("MemorySegment compression not supported by " + getClass().getName());
  }

  /**
   * Decompress data using MemorySegment (zero-copy).
   * Default implementation throws UnsupportedOperationException.
   *
   * @param compressed compressed data
   * @return decompressed data in a MemorySegment
   * @deprecated Use {@link #decompressScoped(MemorySegment)} for Loom compatibility
   */
  @Deprecated(forRemoval = true)
  default MemorySegment decompress(MemorySegment compressed) {
    throw new UnsupportedOperationException("MemorySegment decompression not supported by " + getClass().getName());
  }

  /**
   * Decompress data using a pooled buffer with explicit lifecycle (Loom-friendly).
   * 
   * <p>Unlike {@link #decompress(MemorySegment)} which uses ThreadLocal (problematic with
   * millions of virtual threads), this method uses a bounded pool. Memory is bounded by
   * pool size, not thread count.
   * 
   * <p>The returned result MUST be closed after use to return the buffer to the pool:
   * <pre>{@code
   * try (var result = handler.decompressScoped(compressed)) {
   *     // Use result.segment() here
   * } // Buffer returned to pool
   * }</pre>
   *
   * @param compressed compressed data
   * @return scoped result containing decompressed segment; must be closed after use
   */
  default DecompressionResult decompressScoped(MemorySegment compressed) {
    throw new UnsupportedOperationException("Scoped decompression not supported by " + getClass().getName());
  }

  /**
   * Check if this handler supports MemorySegment-based operations.
   *
   * @return true if compress/decompress MemorySegment methods are supported
   */
  default boolean supportsMemorySegments() {
    return false;
  }
}
