package io.sirix.node;

import java.lang.foreign.MemorySegment;

/**
 * Utility class to replace Chronicle Bytes factory methods. Provides factory methods for creating
 * BytesOut instances.
 * <p>
 * <b>Memory:</b> every buffer created here is a small, heap-backed, growing writer for a temporary
 * encode (a node's hash input, a record, a key), reclaimed by the GC; despite their historical
 * names, none of the {@code elastic…} factories allocates off-heap memory. A buffer that is large
 * or flushed to a file borrows allocator scratch instead ({@link MemorySegmentBytesOut#ofScratch}).
 * This class keeps no spare buffers.
 */
public final class Bytes {

  private static final ThreadLocal<MemorySegmentBytesOut> HASH_BUFFER =
      ThreadLocal.withInitial(() -> new MemorySegmentBytesOut(256));

  private Bytes() {
    // Utility class
  }

  /**
   * Returns a thread-local reusable buffer for hash computation. The buffer is cleared on each call.
   * Safe because: - ThreadLocal guarantees per-thread isolation - computeHash() never recurses into
   * another node's getHash() - The buffer content is consumed within the getHash() call
   *
   * @return a cleared, reusable BytesOut instance
   */
  public static BytesOut<MemorySegment> threadLocalHashBuffer() {
    final MemorySegmentBytesOut buf = HASH_BUFFER.get();
    buf.clear();
    return buf;
  }

  /**
   * Factory method to create a growing BytesOut over a heap {@code byte[]} (the name predates the
   * heap backing).
   *
   * @param initialCapacity the initial capacity
   * @return a new BytesOut instance
   */
  public static BytesOut<MemorySegment> elasticOffHeapByteBuffer(int initialCapacity) {
    return new MemorySegmentBytesOut(initialCapacity);
  }

  /**
   * Factory method to create a growing BytesOut over a heap {@code byte[]} (the name predates the
   * heap backing).
   *
   * @return a new BytesOut instance with default capacity
   */
  public static BytesOut<MemorySegment> elasticOffHeapByteBuffer() {
    return new MemorySegmentBytesOut();
  }

  /**
   * Alias for elasticOffHeapByteBuffer for backward compatibility.
   * 
   * @param initialCapacity the initial capacity
   * @return a new BytesOut instance that must be closed after use
   */
  public static BytesOut<MemorySegment> elasticHeapByteBuffer(int initialCapacity) {
    return elasticOffHeapByteBuffer(initialCapacity);
  }

  /**
   * Alias for elasticOffHeapByteBuffer for backward compatibility.
   * 
   * @return a new BytesOut instance with default capacity that must be closed after use
   */
  public static BytesOut<MemorySegment> elasticHeapByteBuffer() {
    return elasticOffHeapByteBuffer();
  }

  /**
   * Factory method to wrap a byte array for reading (zero-copy). Uses MemorySegment.ofArray() to
   * create a segment backed by the array directly, avoiding any data copying. The
   * MemorySegmentBytesIn uses unaligned access methods so alignment is not required.
   * 
   * @param data the byte array to wrap (must not be modified while reading)
   * @return a BytesIn instance for reading
   */
  public static BytesIn<MemorySegment> wrapForRead(byte[] data) {
    // Zero-copy: MemorySegment backed directly by the array
    // MemorySegmentBytesIn uses JAVA_*_UNALIGNED layouts, so alignment is not required
    return new MemorySegmentBytesIn(MemorySegment.ofArray(data));
  }

  /**
   * Factory method to wrap an existing MemorySegment for reading (zero-copy). This is the most
   * efficient path when the data is already in a MemorySegment.
   * 
   * @param segment the MemorySegment to wrap
   * @return a BytesIn instance for reading
   */
  public static BytesIn<MemorySegment> wrapForRead(MemorySegment segment) {
    return new MemorySegmentBytesIn(segment);
  }

  /**
   * Factory method to create an elastic heap BytesOut with initial capacity.
   * 
   * @param initialCapacity the initial capacity
   * @return a BytesOut instance
   */
  public static BytesOut<MemorySegment> allocateElasticOnHeap(int initialCapacity) {
    return elasticHeapByteBuffer(initialCapacity);
  }

  /**
   * Factory method to wrap a byte array for writing.
   * 
   * @param data the byte array to wrap
   * @return a BytesOut instance for writing
   */
  public static BytesOut<MemorySegment> wrapForWrite(byte[] data) {
    // Create a BytesOut and write the data to set position correctly
    BytesOut<MemorySegment> out = new MemorySegmentBytesOut(Math.max(data.length, 1024));
    out.write(data);
    return out;
  }
}
