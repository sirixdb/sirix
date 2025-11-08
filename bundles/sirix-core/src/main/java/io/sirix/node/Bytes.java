package io.sirix.node;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Utility class to replace Chronicle Bytes factory methods.
 * Provides factory methods for creating BytesOut instances.
 */
public final class Bytes {
    
    private Bytes() {
        // Utility class
    }
    
    /**
     * Factory method to create an elastic off-heap MemorySegment-based BytesOut.
     * Uses GrowingMemorySegment with 8-byte aligned memory for optimal performance.
     * The returned BytesOut should be used with try-with-resources to ensure proper cleanup.
     * @param initialCapacity the initial capacity
     * @return a new BytesOut instance that must be closed after use
     */
    public static BytesOut<MemorySegment> elasticOffHeapByteBuffer(int initialCapacity) {
        return new MemorySegmentBytesOut(initialCapacity);
    }
    
    /**
     * Factory method to create an elastic off-heap MemorySegment-based BytesOut.
     * Uses GrowingMemorySegment with 8-byte aligned memory for optimal performance.
     * The returned BytesOut should be used with try-with-resources to ensure proper cleanup.
     * @return a new BytesOut instance with default capacity that must be closed after use
     */
    public static BytesOut<MemorySegment> elasticOffHeapByteBuffer() {
        return new MemorySegmentBytesOut();
    }
    
    /**
     * Alias for elasticOffHeapByteBuffer for backward compatibility.
     * @param initialCapacity the initial capacity
     * @return a new BytesOut instance that must be closed after use
     */
    public static BytesOut<MemorySegment> elasticHeapByteBuffer(int initialCapacity) {
        return elasticOffHeapByteBuffer(initialCapacity);
    }
    
    /**
     * Alias for elasticOffHeapByteBuffer for backward compatibility.
     * @return a new BytesOut instance with default capacity that must be closed after use
     */
    public static BytesOut<MemorySegment> elasticHeapByteBuffer() {
        return elasticOffHeapByteBuffer();
    }
    
    /**
     * Factory method to wrap a byte array for reading.
     * @param data the byte array to wrap
     * @return a BytesIn instance for reading
     */
    public static BytesIn<MemorySegment> wrapForRead(byte[] data) {
        // Ensure alignment for long values (8 bytes)
        MemorySegment segment = Arena.ofAuto().allocate(data.length, 8);
        segment.asByteBuffer().put(data).flip();
        return new MemorySegmentBytesIn(segment);
    }
    
    /**
     * Factory method to wrap a ByteBuffer for reading.
     * @param buffer the ByteBuffer to wrap
     * @return a BytesIn instance for reading
     */
    public static BytesIn<MemorySegment> wrapForRead(ByteBuffer buffer) {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return wrapForRead(data);
    }
    
    /**
     * Factory method to create an elastic heap BytesOut with initial capacity.
     * @param initialCapacity the initial capacity
     * @return a BytesOut instance
     */
    public static BytesOut<MemorySegment> allocateElasticOnHeap(int initialCapacity) {
        return elasticHeapByteBuffer(initialCapacity);
    }
    
    /**
     * Factory method to wrap a byte array for writing.
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