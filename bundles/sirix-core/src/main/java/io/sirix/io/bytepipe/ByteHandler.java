package io.sirix.io.bytepipe;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;

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
   * <p>Usage:
   * <pre>{@code
   * try (var result = byteHandler.decompressScoped(compressed)) {
   *     Page page = deserialize(result.segment());
   *     return page;
   * } // Buffer automatically returned to pool
   * }</pre>
   */
  record DecompressionResult(MemorySegment segment, Runnable releaser) implements AutoCloseable {
    @Override
    public void close() {
      if (releaser != null) {
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
