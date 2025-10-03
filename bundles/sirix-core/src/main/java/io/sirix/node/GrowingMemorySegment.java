package io.sirix.node;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A wrapper around MemorySegment that automatically grows when writes exceed the current capacity.
 * Uses off-heap memory with proper 8-byte alignment to enable efficient aligned access.
 * This ensures all long fields can be accessed using JAVA_LONG instead of JAVA_LONG_UNALIGNED.
 */
public class GrowingMemorySegment {
    private static final int INITIAL_CAPACITY = 1024; // 1KB initial size
    private static final int GROWTH_FACTOR = 2; // Double the size when growing
    private static final long ALIGNMENT = 8; // 8-byte alignment for long fields
    
    private final Arena arena;
    private MemorySegment segment;
    private long position;
    private long capacity;
    
    /**
     * Create a new GrowingMemorySegment with default initial capacity.
     * Uses off-heap memory with 8-byte alignment.
     */
    public GrowingMemorySegment() {
        this(INITIAL_CAPACITY);
    }
    
    /**
     * Create a new GrowingMemorySegment with specified initial capacity.
     * Uses off-heap memory with 8-byte alignment.
     * 
     * Uses Arena.ofAuto() for automatic GC-based memory management.
     * This is necessary because MemorySegments are stored in pages that can be
     * read by multiple read-only transactions that outlive write transactions.
     * 
     * @param initialCapacity the initial capacity in bytes
     */
    public GrowingMemorySegment(int initialCapacity) {
        this.arena = Arena.ofAuto();
        this.capacity = initialCapacity;
        // Allocate with explicit 8-byte alignment for optimal performance
        this.segment = arena.allocate(initialCapacity, ALIGNMENT);
        this.position = 0;
    }
    
    /**
     * Create a new GrowingMemorySegment from an existing MemorySegment.
     * Copies data from the existing segment to new off-heap aligned memory.
     * 
     * Uses Arena.ofAuto() for automatic GC-based memory management.
     * 
     * @param existingSegment the existing segment to copy data from
     */
    public GrowingMemorySegment(MemorySegment existingSegment) {
        long size = existingSegment.byteSize();
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Segment too large to convert to growing segment");
        }
        
        this.arena = Arena.ofAuto();
        this.capacity = size;
        // Allocate with explicit 8-byte alignment
        this.segment = arena.allocate(size, ALIGNMENT);
        this.position = 0;
        
        // Copy data from existing segment
        MemorySegment.copy(existingSegment, 0, this.segment, 0, size);
    }
    
    /**
     * Ensure that the segment has at least the specified capacity.
     * If the current capacity is insufficient, the segment will be grown.
     * 
     * @param requiredCapacity the minimum required capacity
     */
    public void ensureCapacity(long requiredCapacity) {
        if (requiredCapacity > capacity) {
            grow(requiredCapacity);
        }
    }
    
    /**
     * Grow the segment to accommodate the required capacity.
     * Maintains 8-byte alignment for all allocations.
     * 
     * @param requiredCapacity the minimum required capacity
     */
    private void grow(long requiredCapacity) {
        if (requiredCapacity > Integer.MAX_VALUE) {
            throw new OutOfMemoryError("Required capacity exceeds maximum segment size");
        }
        
        // Calculate new capacity - at least double the current size, or required capacity, whichever is larger
        long newCapacity = Math.max(capacity * GROWTH_FACTOR, requiredCapacity);
        if (newCapacity > Integer.MAX_VALUE) {
            newCapacity = Integer.MAX_VALUE;
        }
        
        // Allocate new larger segment with proper alignment
        MemorySegment newSegment = arena.allocate(newCapacity, ALIGNMENT);
        
        // Copy existing data to new segment
        MemorySegment.copy(segment, 0, newSegment, 0, position);
        
        // Update references
        this.segment = newSegment;
        this.capacity = newCapacity;
    }
    
    /**
     * Get the current MemorySegment. This may return a different segment instance
     * if the underlying storage has grown since the last call.
     * The returned segment is always 8-byte aligned.
     * 
     * @return the current MemorySegment
     */
    public MemorySegment getSegment() {
        return segment;
    }
    
    /**
     * Get the current capacity of the segment.
     * 
     * @return the current capacity in bytes
     */
    public long capacity() {
        return capacity;
    }
    
    /**
     * Get the current logical size (position) of the segment.
     * 
     * @return the current position
     */
    public long size() {
        return position;
    }
    
    /**
     * Set the logical size (position) of the segment.
     * 
     * @param newSize the new logical size
     */
    public void setSize(long newSize) {
        this.position = newSize;
    }
    
    /**
     * Get the current position for writing.
     * 
     * @return the current position
     */
    public long position() {
        return position;
    }
    
    /**
     * Set the current position for writing.
     * 
     * @param newPosition the new position
     */
    public void setPosition(long newPosition) {
        this.position = newPosition;
    }
    
    /**
     * Advance the position by the specified number of bytes, ensuring capacity is available.
     * 
     * @param bytes the number of bytes to advance
     */
    public void advance(long bytes) {
        ensureCapacity(position + bytes);
        position += bytes;
    }
    
    /**
     * Get a slice of the segment from 0 to the current logical size.
     * The returned segment maintains the 8-byte alignment of the base.
     * 
     * @return a MemorySegment slice containing only the used portion
     */
    public MemorySegment getUsedSegment() {
        if (position == 0) {
            // Return an empty aligned segment
            return arena.allocate(0, ALIGNMENT);
        }
        return segment.asSlice(0, position);
    }
    
    /**
     * Reset the segment to empty state while keeping the allocated capacity.
     */
    public void reset() {
        this.position = 0;
    }
    
    /**
     * Write an int value at the current position.
     * Uses unaligned access since position may not be aligned.
     */
    public void writeInt(int value) {
        ensureCapacity(position + 4);
        segment.set(ValueLayout.JAVA_INT_UNALIGNED, position, value);
        position += 4;
    }
    
    /**
     * Write a long value at the current position.
     * Uses unaligned access since position may not be aligned.
     */
    public void writeLong(long value) {
        ensureCapacity(position + 8);
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, position, value);
        position += 8;
    }
    
    /**
     * Write a byte value at the current position.
     */
    public void writeByte(byte value) {
        ensureCapacity(position + 1);
        segment.set(ValueLayout.JAVA_BYTE, position, value);
        position += 1;
    }
    
    /**
     * Write a double value at the current position.
     * Uses unaligned access since position may not be aligned.
     */
    public void writeDouble(double value) {
        ensureCapacity(position + 8);
        segment.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, position, value);
        position += 8;
    }
    
    /**
     * Write a float value at the current position.
     * Uses unaligned access since position may not be aligned.
     */
    public void writeFloat(float value) {
        ensureCapacity(position + 4);
        segment.set(ValueLayout.JAVA_FLOAT_UNALIGNED, position, value);
        position += 4;
    }
    
    /**
     * Write a short value at the current position.
     * Uses unaligned access since position may not be aligned.
     */
    public void writeShort(short value) {
        ensureCapacity(position + 2);
        segment.set(ValueLayout.JAVA_SHORT_UNALIGNED, position, value);
        position += 2;
    }
    
    /**
     * Write a byte array at the current position.
     */
    public void write(byte[] bytes, int offset, int length) {
        ensureCapacity(position + length);
        MemorySegment.copy(bytes, offset, segment, ValueLayout.JAVA_BYTE, position, length);
        position += length;
    }
    
    /**
     * Create a copy of the currently used data as a byte array.
     * 
     * @return a byte array containing the used portion of the segment
     */
    public byte[] toByteArray() {
        if (position == 0) {
            return new byte[0];
        }
        
        byte[] result = new byte[(int) position];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, result, 0, (int) position);
        return result;
    }
    
    /**
     * Close this GrowingMemorySegment.
     * 
     * This is a no-op since Arena.ofAuto() manages memory automatically via GC.
     * The off-heap memory will be freed automatically when this object is no longer referenced.
     * This method exists to maintain AutoCloseable contract from BytesOut.
     */
    public void close() {
        // No-op: Arena.ofAuto() is GC-managed, no manual close needed
    }
    
    /**
     * Check if this segment is still alive.
     * 
     * @return true (always true for Arena.ofAuto() which is GC-managed)
     */
    public boolean isAlive() {
        return true; // Arena.ofAuto() is always "alive" until GC'd
    }
}