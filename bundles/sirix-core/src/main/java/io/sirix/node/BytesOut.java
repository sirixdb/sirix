package io.sirix.node;

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Custom BytesOut interface for writing data to various destinations.
 * This interface replaces the Chronicle Bytes dependency and provides
 * a clean abstraction for writing serialized data.
 * 
 * @param <T> the underlying data destination type
 */
public interface BytesOut<T> extends AutoCloseable {
    
    /**
     * Write an integer value.
     * @param value the integer value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeInt(int value);
    
    /**
     * Write a long value.
     * @param value the long value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeLong(long value);
    
    /**
     * Write a byte value.
     * @param value the byte value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeByte(byte value);
    
    /**
     * Write a boolean value.
     * @param value the boolean value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeBoolean(boolean value);
    
    /**
     * Write a double value.
     * @param value the double value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeDouble(double value);
    
    /**
     * Write a float value.
     * @param value the float value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeFloat(float value);
    
    /**
     * Write a short value.
     * @param value the short value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeShort(short value);
    
    /**
     * Write a BigInteger value.
     * @param value the BigInteger value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeBigInteger(BigInteger value);
    
    /**
     * Write a BigDecimal value.
     * @param value the BigDecimal value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeBigDecimal(java.math.BigDecimal value);
    
    /**
     * Write a UTF-8 string.
     * @param value the string value to write, or null
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeUtf8(String value);
    
    /**
     * Write a variable-length long value using stop-bit encoding.
     * @param value the long value to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writeStopBit(long value);
    
    // ==================== BATCH WRITE METHODS ====================
    // These methods perform a single capacity check for multiple bytes,
    // eliminating per-byte overhead in hot paths like varint encoding.
    
    /**
     * Write two bytes with a single capacity check.
     * Useful for 2-byte VarInt values (128-16383 range).
     * 
     * @param b0 first byte
     * @param b1 second byte
     * @return this BytesOut for method chaining
     */
    default BytesOut<T> writeBytes2(byte b0, byte b1) {
        writeByte(b0);
        writeByte(b1);
        return this;
    }
    
    /**
     * Write three bytes with a single capacity check.
     * Useful for 3-byte VarInt values (16384-2097151 range).
     * 
     * @param b0 first byte
     * @param b1 second byte
     * @param b2 third byte
     * @return this BytesOut for method chaining
     */
    default BytesOut<T> writeBytes3(byte b0, byte b1, byte b2) {
        writeByte(b0);
        writeByte(b1);
        writeByte(b2);
        return this;
    }
    
    /**
     * Write a byte array.
     * @param bytes the byte array to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> write(byte[] bytes);
    
    /**
     * Write a portion of a byte array.
     * @param bytes the byte array to write from
     * @param offset the offset in the array
     * @param length the number of bytes to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> write(byte[] bytes, int offset, int length);
    
    /**
     * Write data from a MemorySegment directly, without intermediate byte[] allocation.
     * This is a high-performance method for bulk data transfer between segments.
     * 
     * <p>Uses {@link MemorySegment#copy(MemorySegment, long, MemorySegment, long, long)}
     * for efficient zero-copy transfer when possible.
     * 
     * <p><b>Performance note:</b> This default implementation allocates a temporary byte[]
     * as a fallback for implementations without native MemorySegment support. The key
     * implementations ({@link MemorySegmentBytesOut}, {@link PooledBytesOut}) override
     * this method to use direct segment-to-segment copy with zero allocation.
     * 
     * @param source the source segment to copy from
     * @param sourceOffset the offset in the source segment
     * @param length the number of bytes to copy (if <= 0, no-op)
     * @return this BytesOut for method chaining
     */
    default BytesOut<T> writeSegment(MemorySegment source, long sourceOffset, long length) {
        // Fallback for implementations without native MemorySegment support.
        // MemorySegmentBytesOut and PooledBytesOut override this for zero-allocation path.
        if (length <= 0) {
            return this;
        }
        byte[] temp = new byte[(int) length];
        MemorySegment.copy(source, java.lang.foreign.ValueLayout.JAVA_BYTE, sourceOffset, temp, 0, (int) length);
        return write(temp);
    }
    
    /**
     * Write from a ByteBuffer at a specific position.
     * @param position the position to write at
     * @param buffer the buffer to write from
     * @param bufferPosition the position in the buffer
     * @param length the number of bytes to write
     * @return this BytesOut for method chaining
     */
    BytesOut<T> write(long position, ByteBuffer buffer, int bufferPosition, int length);
    
    /**
     * Get the current write position.
     * @return the current position
     */
    long position();
    
    /**
     * Set the current write position.
     * @param newPosition the new position
     * @return this BytesOut for method chaining
     */
    BytesOut<T> position(long newPosition);
    
    /**
     * Get the write position.
     * @return the write position
     */
    long writePosition();
    
    /**
     * Set the write position.
     * @param position the write position
     * @return this BytesOut for method chaining
     */
    BytesOut<T> writePosition(long position);
    
    /**
     * Get the read limit.
     * @return the read limit
     */
    long readLimit();
    
    /**
     * Convert to byte array.
     * @return byte array representation of the data
     */
    byte[] toByteArray();
    
    /**
     * Get bytes for reading.
     * @return a BytesIn view for reading the written data
     */
    BytesIn<T> bytesForRead();
    
    /**
     * Get bytes for writing (returns self).
     * @return this BytesOut instance
     */
    BytesOut<T> bytesForWrite();
    
    /**
     * Clear the buffer/stream.
     * @return this BytesOut for method chaining
     */
    BytesOut<T> clear();
    
    /**
     * Get the underlying object/buffer.
     * @return the underlying object
     */
    Object underlyingObject();
    
    /**
     * Get the underlying data destination.
     * @return the data destination
     */
    T getDestination();
    
    /**
     * Get an OutputStream view of this BytesOut.
     * @return an OutputStream that writes to this BytesOut
     */
    java.io.OutputStream outputStream();
    
    /**
     * Factory method to create an elastic heap MemorySegment-based BytesOut.
     * @param initialCapacity the initial capacity
     * @return a new BytesOut instance
     */
    static BytesOut<MemorySegment> elasticHeapByteBuffer(int initialCapacity) {
      return Bytes.elasticOffHeapByteBuffer(initialCapacity);
    }
    
    /**
     * Factory method to create an elastic heap MemorySegment-based BytesOut.
     * @return a new BytesOut instance with default capacity
     */
    static BytesOut<MemorySegment> elasticHeapByteBuffer() {
        return elasticHeapByteBuffer(1024);
    }
    
    /**
     * Convert this BytesOut to a BytesIn for reading.
     * @return a BytesIn that can read the data written to this BytesOut
     */
    default BytesIn<T> asBytesIn() {
        if (this instanceof MemorySegmentBytesOut) {
            MemorySegment segment = ((MemorySegmentBytesOut) this).getDestination();
            return (BytesIn<T>) new MemorySegmentBytesIn(segment);
        }
        throw new UnsupportedOperationException("Cannot convert " + getClass().getSimpleName() + " to BytesIn");
    }
    
    /**
     * Close and release any resources held by this BytesOut.
     * Default implementation does nothing - implementations with
     * resources to release (like off-heap memory) should override this.
     */
    @Override
    default void close() {
        // Default: no-op for implementations without resources to release
    }
}