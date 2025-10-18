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
   */
  default MemorySegment decompress(MemorySegment compressed) {
    throw new UnsupportedOperationException("MemorySegment decompression not supported by " + getClass().getName());
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
