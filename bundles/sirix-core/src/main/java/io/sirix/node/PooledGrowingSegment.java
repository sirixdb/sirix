package io.sirix.node;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A resettable MemorySegment wrapper for pooled buffers.
 * Unlike GrowingMemorySegment, this does NOT own the Arena - the pool does.
 * Supports reset() for reuse without reallocation.
 * 
 * <p>Design: Keeps reference to original pooled buffer. On overflow, allocates
 * from a confined arena. On reset(), closes overflow arena and restores
 * original buffer pointer.</p>
 * 
 * <p>This class is designed for use with {@link io.sirix.io.SerializationBufferPool}
 * to enable efficient buffer reuse during parallel page serialization.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class PooledGrowingSegment {
    
    private static final long ALIGNMENT = 8; // 8-byte alignment for long fields
    
    private final MemorySegment originalBuffer; // Never changes - the pooled buffer
    private MemorySegment currentSegment;       // May point to overflow
    private long position;
    
    // Overflow arena for when buffer is too small (rare case)
    private Arena overflowArena;
    
    /**
     * Create a new PooledGrowingSegment wrapping a pooled buffer.
     * 
     * @param pooledBuffer the pre-allocated buffer from the pool
     */
    public PooledGrowingSegment(MemorySegment pooledBuffer) {
        this.originalBuffer = pooledBuffer;
        this.currentSegment = pooledBuffer;
        this.position = 0;
    }
    
    /**
     * Ensure the segment has at least the specified capacity.
     * If the current segment is too small, allocates from an overflow arena.
     * 
     * @param required the required capacity in bytes
     */
    public void ensureCapacity(long required) {
        if (required > currentSegment.byteSize()) {
            // Buffer too small - allocate overflow (rare, ~1% of cases)
            if (overflowArena == null) {
                overflowArena = Arena.ofConfined();
            }
            long newSize = Math.max(required, currentSegment.byteSize() * 2);
            MemorySegment newSeg = overflowArena.allocate(newSize, ALIGNMENT);
            if (position > 0) {
                MemorySegment.copy(currentSegment, 0, newSeg, 0, position);
            }
            currentSegment = newSeg;
        }
    }
    
    /**
     * Reset this segment for reuse. Closes any overflow arena and restores
     * the pointer to the original pooled buffer.
     */
    public void reset() {
        // Close overflow arena if we used it
        if (overflowArena != null) {
            overflowArena.close();
            overflowArena = null;
        }
        // Restore pointer to original pooled buffer
        currentSegment = originalBuffer;
        position = 0;
    }
    
    /**
     * Get the underlying pooled buffer. This is always the original buffer,
     * not any overflow allocation. Used when returning to the pool.
     * 
     * @return the original pooled buffer
     */
    public MemorySegment getUnderlyingSegment() {
        return originalBuffer;
    }
    
    /**
     * Get the current segment being written to. This may be the original
     * buffer or an overflow allocation.
     * 
     * @return the current segment
     */
    public MemorySegment getCurrentSegment() {
        return currentSegment;
    }
    
    /**
     * Get the current write position.
     * 
     * @return the position in bytes
     */
    public long position() {
        return position;
    }
    
    /**
     * Get a slice of the current segment from 0 to position.
     * 
     * @return the written portion of the segment
     */
    public MemorySegment getWrittenSlice() {
        return currentSegment.asSlice(0, position);
    }
    
    /**
     * Get the capacity of the current segment.
     * 
     * @return the capacity in bytes
     */
    public long capacity() {
        return currentSegment.byteSize();
    }
    
    // ==================== Write Methods ====================
    
    /**
     * Write a single byte.
     * 
     * @param value the byte to write
     */
    public void writeByte(byte value) {
        ensureCapacity(position + 1);
        currentSegment.set(ValueLayout.JAVA_BYTE, position++, value);
    }
    
    /**
     * Write a boolean as a single byte.
     * 
     * @param value the boolean to write
     */
    public void writeBoolean(boolean value) {
        writeByte(value ? (byte) 1 : (byte) 0);
    }
    
    /**
     * Write a short (2 bytes, unaligned).
     * 
     * @param value the short to write
     */
    public void writeShort(short value) {
        ensureCapacity(position + 2);
        currentSegment.set(ValueLayout.JAVA_SHORT_UNALIGNED, position, value);
        position += 2;
    }
    
    /**
     * Write an int (4 bytes, unaligned).
     * 
     * @param value the int to write
     */
    public void writeInt(int value) {
        ensureCapacity(position + 4);
        currentSegment.set(ValueLayout.JAVA_INT_UNALIGNED, position, value);
        position += 4;
    }
    
    /**
     * Write a long (8 bytes, unaligned).
     * 
     * @param value the long to write
     */
    public void writeLong(long value) {
        ensureCapacity(position + 8);
        currentSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, position, value);
        position += 8;
    }
    
    /**
     * Write a float (4 bytes, unaligned).
     * 
     * @param value the float to write
     */
    public void writeFloat(float value) {
        ensureCapacity(position + 4);
        currentSegment.set(ValueLayout.JAVA_FLOAT_UNALIGNED, position, value);
        position += 4;
    }
    
    /**
     * Write a double (8 bytes, unaligned).
     * 
     * @param value the double to write
     */
    public void writeDouble(double value) {
        ensureCapacity(position + 8);
        currentSegment.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, position, value);
        position += 8;
    }
    
    /**
     * Write a byte array.
     * 
     * @param data the bytes to write
     */
    public void write(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        ensureCapacity(position + data.length);
        MemorySegment.copy(data, 0, currentSegment, ValueLayout.JAVA_BYTE, position, data.length);
        position += data.length;
    }
    
    /**
     * Write a portion of a byte array.
     * 
     * @param data the source array
     * @param offset the offset in the source array
     * @param length the number of bytes to write
     */
    public void write(byte[] data, int offset, int length) {
        if (data == null || length == 0) {
            return;
        }
        ensureCapacity(position + length);
        MemorySegment.copy(data, offset, currentSegment, ValueLayout.JAVA_BYTE, position, length);
        position += length;
    }
    
    /**
     * Write from a MemorySegment.
     * 
     * @param source the source segment
     */
    public void write(MemorySegment source) {
        if (source == null) {
            return;
        }
        long size = source.byteSize();
        if (size == 0) {
            return;
        }
        ensureCapacity(position + size);
        MemorySegment.copy(source, 0, currentSegment, position, size);
        position += size;
    }
    
    /**
     * Write data from a MemorySegment at a specific offset, without intermediate byte[] allocation.
     * This is a high-performance method for bulk data transfer between segments.
     * 
     * <p>Uses {@link MemorySegment#copy(MemorySegment, long, MemorySegment, long, long)}
     * for efficient zero-copy transfer.
     * 
     * @param source the source segment to copy from
     * @param sourceOffset the offset in the source segment
     * @param length the number of bytes to copy
     */
    public void writeSegment(MemorySegment source, long sourceOffset, long length) {
        if (length <= 0) {
            return;  // Handle edge case: no-op for zero or negative length
        }
        ensureCapacity(position + length);
        MemorySegment.copy(source, sourceOffset, currentSegment, position, length);
        position += length;
    }
    
    /**
     * Write a UTF-8 encoded string with length prefix.
     * 
     * @param value the string to write
     */
    public void writeUtf8(String value) {
        if (value == null) {
            writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        writeInt(bytes.length);
        write(bytes);
    }
    
    // ==================== BATCH WRITE METHODS ====================
    // These methods perform a single ensureCapacity() call for multiple bytes,
    // eliminating per-byte capacity checks in hot paths.
    
    /**
     * Write two bytes with a single capacity check.
     * Useful for writing 2-byte VarInt values or type+flag combinations.
     * 
     * @param b0 first byte
     * @param b1 second byte
     */
    public void writeBytes2(byte b0, byte b1) {
        ensureCapacity(position + 2);
        currentSegment.set(ValueLayout.JAVA_BYTE, position, b0);
        currentSegment.set(ValueLayout.JAVA_BYTE, position + 1, b1);
        position += 2;
    }
    
    /**
     * Write three bytes with a single capacity check.
     * 
     * @param b0 first byte
     * @param b1 second byte
     * @param b2 third byte
     */
    public void writeBytes3(byte b0, byte b1, byte b2) {
        ensureCapacity(position + 3);
        currentSegment.set(ValueLayout.JAVA_BYTE, position, b0);
        currentSegment.set(ValueLayout.JAVA_BYTE, position + 1, b1);
        currentSegment.set(ValueLayout.JAVA_BYTE, position + 2, b2);
        position += 3;
    }
    
    /**
     * Write a byte followed by an int (5 bytes total) with a single capacity check.
     * Common pattern: type byte + 4-byte length/value.
     * 
     * @param b the byte to write
     * @param value the int value to write (unaligned)
     */
    public void writeByteAndInt(byte b, int value) {
        ensureCapacity(position + 5);
        currentSegment.set(ValueLayout.JAVA_BYTE, position, b);
        currentSegment.set(ValueLayout.JAVA_INT_UNALIGNED, position + 1, value);
        position += 5;
    }
    
    /**
     * Write a byte followed by a long (9 bytes total) with a single capacity check.
     * Common pattern: type byte + 8-byte value.
     * 
     * @param b the byte to write
     * @param value the long value to write (unaligned)
     */
    public void writeByteAndLong(byte b, long value) {
        ensureCapacity(position + 9);
        currentSegment.set(ValueLayout.JAVA_BYTE, position, b);
        currentSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, position + 1, value);
        position += 9;
    }
    
    /**
     * Write a variable-length unsigned long using varint encoding.
     * Uses 7 bits per byte, MSB indicates continuation.
     * 
     * <p>Optimized with fast paths for 1-byte (0-127) and 2-byte (128-16383) values,
     * which are the most common cases for delta-encoded node keys.
     * 
     * <p>This method performs a single ensureCapacity() call for the maximum possible
     * varint size (10 bytes), eliminating per-byte capacity checks.
     * 
     * @param value the unsigned long value to write
     * @return the number of bytes written (1-10)
     */
    public int writeVarLong(long value) {
        // Max varint size is 10 bytes (ceil(64/7))
        ensureCapacity(position + 10);
        
        // Fast path for 1-byte values (0-127) - most common case
        if ((value & ~0x7FL) == 0) {
            currentSegment.set(ValueLayout.JAVA_BYTE, position, (byte) value);
            position += 1;
            return 1;
        }
        
        // Fast path for 2-byte values (128-16383) - second most common
        if ((value & ~0x3FFFL) == 0) {
            currentSegment.set(ValueLayout.JAVA_BYTE, position, (byte) ((value & 0x7F) | 0x80));
            currentSegment.set(ValueLayout.JAVA_BYTE, position + 1, (byte) (value >>> 7));
            position += 2;
            return 2;
        }
        
        // General case for larger values
        int bytesWritten = 0;
        while ((value & ~0x7FL) != 0) {
            currentSegment.set(ValueLayout.JAVA_BYTE, position + bytesWritten, 
                        (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
            bytesWritten++;
        }
        currentSegment.set(ValueLayout.JAVA_BYTE, position + bytesWritten++, (byte) value);
        position += bytesWritten;
        return bytesWritten;
    }
    
    /**
     * Skip (advance position by) the specified number of bytes.
     * The skipped bytes are not initialized.
     * 
     * @param bytes the number of bytes to skip
     */
    public void skip(long bytes) {
        ensureCapacity(position + bytes);
        position += bytes;
    }
    
    /**
     * Set the write position.
     * 
     * @param newPosition the new position
     */
    public void position(long newPosition) {
        if (newPosition < 0) {
            throw new IllegalArgumentException("Position cannot be negative: " + newPosition);
        }
        ensureCapacity(newPosition);
        this.position = newPosition;
    }
    
    /**
     * Clear the segment (reset position to 0).
     */
    public void clear() {
        position = 0;
    }
}

