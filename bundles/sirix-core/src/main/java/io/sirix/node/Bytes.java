package io.sirix.node;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Utility class to replace Chronicle Bytes factory methods.
 * Provides factory methods for creating BytesOut instances.
 * <p>
 * <b>Memory Optimization:</b> All BytesOut instances created here use a thread-local
 * shared Arena (via GrowingMemorySegment) to avoid Arena.ofAuto() proliferation
 * during high-throughput operations like JSON shredding.
 */
public final class Bytes {

    private static final ThreadLocal<MemorySegmentBytesOut> HASH_BUFFER =
        ThreadLocal.withInitial(() -> new MemorySegmentBytesOut(256));

    private Bytes() {
        // Utility class
    }

    /**
     * Returns a thread-local reusable buffer for hash computation.
     * The buffer is cleared on each call. Safe because:
     * - ThreadLocal guarantees per-thread isolation
     * - computeHash() never recurses into another node's getHash()
     * - The buffer content is consumed within the getHash() call
     *
     * @return a cleared, reusable BytesOut instance
     */
    public static BytesOut<MemorySegment> threadLocalHashBuffer() {
        final MemorySegmentBytesOut buf = HASH_BUFFER.get();
        buf.clear();
        return buf;
    }
    
    /**
     * Factory method to create an elastic off-heap MemorySegment-based BytesOut.
     * Uses GrowingMemorySegment with 8-byte aligned memory for optimal performance.
     * <p>
     * Memory is allocated from a thread-local shared Arena to minimize GC overhead.
     * 
     * @param initialCapacity the initial capacity
     * @return a new BytesOut instance
     */
    public static BytesOut<MemorySegment> elasticOffHeapByteBuffer(int initialCapacity) {
        return new MemorySegmentBytesOut(initialCapacity);
    }
    
    /**
     * Factory method to create an elastic off-heap MemorySegment-based BytesOut.
     * Uses GrowingMemorySegment with 8-byte aligned memory for optimal performance.
     * <p>
     * Memory is allocated from a thread-local shared Arena to minimize GC overhead.
     * 
     * @return a new BytesOut instance with default capacity
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
     * Factory method to wrap a byte array for reading (zero-copy).
     * Uses MemorySegment.ofArray() to create a segment backed by the array directly,
     * avoiding any data copying. The MemorySegmentBytesIn uses unaligned access methods
     * so alignment is not required.
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
     * Factory method to wrap an existing MemorySegment for reading (zero-copy).
     * This is the most efficient path when the data is already in a MemorySegment.
     * 
     * @param segment the MemorySegment to wrap
     * @return a BytesIn instance for reading
     */
    public static BytesIn<MemorySegment> wrapForRead(MemorySegment segment) {
        return new MemorySegmentBytesIn(segment);
    }
    
    /**
     * Factory method to wrap a ByteBuffer for reading.
     * If the buffer has a backing array, uses zero-copy wrapping.
     * Otherwise, copies the data to a new array.
     * 
     * @param buffer the ByteBuffer to wrap
     * @return a BytesIn instance for reading
     */
    public static BytesIn<MemorySegment> wrapForRead(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            // Zero-copy path for heap buffers with backing array
            int offset = buffer.arrayOffset() + buffer.position();
            int length = buffer.remaining();
            MemorySegment segment = MemorySegment.ofArray(buffer.array()).asSlice(offset, length);
            return new MemorySegmentBytesIn(segment);
        } else {
            // Fallback: copy data for direct buffers without backing array
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            return wrapForRead(data);
        }
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