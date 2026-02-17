package io.sirix.node;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A wrapper around MemorySegment that automatically grows when writes exceed the current capacity.
 * <p>
 * <b>Memory Management:</b> Uses heap-backed MemorySegments (via MemorySegment.ofArray) for
 * temporary buffers. This avoids Arena.ofAuto() proliferation while ensuring memory is properly
 * reclaimed by GC when the segment is no longer referenced.
 * <p>
 * All node classes use UNALIGNED ValueLayouts, so heap-backed segments work correctly without
 * alignment requirements.
 */
public class GrowingMemorySegment {
  private static final int INITIAL_CAPACITY = 1024; // 1KB initial size
  private static final int GROWTH_FACTOR = 2; // Double the size when growing
  private static final long ALIGNMENT = 8; // 8-byte alignment for arena-backed segments
  private static final MemorySegment EMPTY_SEGMENT = MemorySegment.ofArray(new byte[0]);

  private final Arena arena;
  private MemorySegment segment;
  private long position;
  private long capacity;

  // Flag to indicate if we're using heap-backed segments (no arena management needed)
  private final boolean heapBacked;

  /**
   * Create a new GrowingMemorySegment with default initial capacity. Uses heap-backed memory for
   * efficient GC reclamation.
   */
  public GrowingMemorySegment() {
    this(INITIAL_CAPACITY);
  }

  /**
   * Create a new GrowingMemorySegment with specified initial capacity. Uses heap-backed memory
   * (MemorySegment.ofArray) for efficient GC reclamation. This avoids Arena.ofAuto() proliferation
   * and direct buffer exhaustion.
   * 
   * @param initialCapacity the initial capacity in bytes
   */
  public GrowingMemorySegment(int initialCapacity) {
    this.arena = null; // No arena needed for heap-backed segments
    this.heapBacked = true;
    this.capacity = initialCapacity;
    // Use heap-backed segment - no Arena needed, GC handles cleanup
    this.segment = MemorySegment.ofArray(new byte[initialCapacity]);
    this.position = 0;
  }

  /**
   * Create a new GrowingMemorySegment with specified Arena and initial capacity. Uses off-heap memory
   * with 8-byte alignment. The caller is responsible for managing the arena's lifecycle.
   * 
   * @param arena the arena to use for memory allocation
   * @param initialCapacity the initial capacity in bytes
   */
  public GrowingMemorySegment(Arena arena, int initialCapacity) {
    this.arena = arena;
    this.heapBacked = false;
    this.capacity = initialCapacity;
    // Allocate with explicit 8-byte alignment for optimal performance
    this.segment = arena.allocate(initialCapacity, ALIGNMENT);
    this.position = 0;
  }

  /**
   * Create a new GrowingMemorySegment from an existing MemorySegment. Copies data to a heap-backed
   * segment for efficient GC reclamation.
   * 
   * @param existingSegment the existing segment to copy data from
   */
  public GrowingMemorySegment(MemorySegment existingSegment) {
    long size = existingSegment.byteSize();
    if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Segment too large to convert to growing segment");
    }

    this.arena = null;
    this.heapBacked = true;
    this.capacity = size;
    // Use heap-backed segment
    this.segment = MemorySegment.ofArray(new byte[(int) size]);
    this.position = 0;

    // Copy data from existing segment
    MemorySegment.copy(existingSegment, 0, this.segment, 0, size);
  }

  /**
   * Ensure that the segment has at least the specified capacity. If the current capacity is
   * insufficient, the segment will be grown.
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
   * 
   * @param requiredCapacity the minimum required capacity
   */
  private void grow(long requiredCapacity) {
    if (requiredCapacity > Integer.MAX_VALUE) {
      throw new OutOfMemoryError("Required capacity exceeds maximum segment size");
    }

    // Calculate new capacity - at least double the current size, or required capacity, whichever is
    // larger
    long newCapacity = Math.max(capacity * GROWTH_FACTOR, requiredCapacity);
    if (newCapacity > Integer.MAX_VALUE) {
      newCapacity = Integer.MAX_VALUE;
    }

    // Allocate new larger segment
    MemorySegment newSegment;
    if (heapBacked) {
      // Use heap-backed segment - old segment is GC'd automatically
      newSegment = MemorySegment.ofArray(new byte[(int) newCapacity]);
    } else {
      // Use arena-allocated segment with proper alignment
      newSegment = arena.allocate(newCapacity, ALIGNMENT);
    }

    // Copy existing data to new segment (copy up to current position)
    long bytesToCopy = Math.min(position, capacity);
    if (bytesToCopy > 0) {
      MemorySegment.copy(segment, 0, newSegment, 0, bytesToCopy);
    }

    // Update references (old segment is GC'd for heap-backed, or stays in arena)
    this.segment = newSegment;
    this.capacity = newCapacity;
  }

  /**
   * Get the current MemorySegment. This may return a different segment instance if the underlying
   * storage has grown since the last call. The returned segment is always 8-byte aligned.
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
   * Get a slice of the segment from 0 to the current logical size. The returned segment maintains the
   * 8-byte alignment of the base.
   * 
   * @return a MemorySegment slice containing only the used portion
   */
  public MemorySegment getUsedSegment() {
    if (position == 0) {
      return EMPTY_SEGMENT;
    }
    return segment.asSlice(0, position);
  }

  /**
   * Get heap backing array when this segment is heap-backed.
   *
   * @return heap backing byte array, or null for native segments
   */
  public byte[] getBackingArrayUnsafe() {
    if (!heapBacked) {
      return null;
    }
    final Object heapBase = segment.heapBase().orElse(null);
    return heapBase instanceof byte[] bytes
        ? bytes
        : null;
  }

  /**
   * Get used size in bytes.
   *
   * @return used bytes in the current segment
   */
  public int getUsedSize() {
    return (int) position;
  }

  /**
   * Reset the segment to empty state while keeping the allocated capacity.
   */
  public void reset() {
    this.position = 0;
  }

  /**
   * Write an int value at the current position. Uses unaligned access since position may not be
   * aligned.
   */
  public void writeInt(int value) {
    ensureCapacity(position + 4);
    segment.set(ValueLayout.JAVA_INT_UNALIGNED, position, value);
    position += 4;
  }

  /**
   * Write a long value at the current position. Uses unaligned access since position may not be
   * aligned.
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
   * Write a double value at the current position. Uses unaligned access since position may not be
   * aligned.
   */
  public void writeDouble(double value) {
    ensureCapacity(position + 8);
    segment.set(ValueLayout.JAVA_DOUBLE_UNALIGNED, position, value);
    position += 8;
  }

  /**
   * Write a float value at the current position. Uses unaligned access since position may not be
   * aligned.
   */
  public void writeFloat(float value) {
    ensureCapacity(position + 4);
    segment.set(ValueLayout.JAVA_FLOAT_UNALIGNED, position, value);
    position += 4;
  }

  /**
   * Write a short value at the current position. Uses unaligned access since position may not be
   * aligned.
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

  // ==================== BATCH WRITE METHODS ====================
  // These methods perform a single ensureCapacity() call for multiple bytes,
  // eliminating per-byte capacity checks in hot paths.

  /**
   * Write two bytes with a single capacity check. Useful for writing 2-byte VarInt values or
   * type+flag combinations.
   * 
   * @param b0 first byte
   * @param b1 second byte
   */
  public void writeBytes2(byte b0, byte b1) {
    ensureCapacity(position + 2);
    segment.set(ValueLayout.JAVA_BYTE, position, b0);
    segment.set(ValueLayout.JAVA_BYTE, position + 1, b1);
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
    segment.set(ValueLayout.JAVA_BYTE, position, b0);
    segment.set(ValueLayout.JAVA_BYTE, position + 1, b1);
    segment.set(ValueLayout.JAVA_BYTE, position + 2, b2);
    position += 3;
  }

  /**
   * Write a byte followed by an int (5 bytes total) with a single capacity check. Common pattern:
   * type byte + 4-byte length/value.
   * 
   * @param b the byte to write
   * @param value the int value to write (unaligned)
   */
  public void writeByteAndInt(byte b, int value) {
    ensureCapacity(position + 5);
    segment.set(ValueLayout.JAVA_BYTE, position, b);
    segment.set(ValueLayout.JAVA_INT_UNALIGNED, position + 1, value);
    position += 5;
  }

  /**
   * Write a byte followed by a long (9 bytes total) with a single capacity check. Common pattern:
   * type byte + 8-byte value.
   * 
   * @param b the byte to write
   * @param value the long value to write (unaligned)
   */
  public void writeByteAndLong(byte b, long value) {
    ensureCapacity(position + 9);
    segment.set(ValueLayout.JAVA_BYTE, position, b);
    segment.set(ValueLayout.JAVA_LONG_UNALIGNED, position + 1, value);
    position += 9;
  }

  /**
   * Write a variable-length unsigned long using varint encoding. Uses 7 bits per byte, MSB indicates
   * continuation.
   * 
   * <p>
   * Optimized with fast paths for 1-byte (0-127) and 2-byte (128-16383) values, which are the most
   * common cases for delta-encoded node keys.
   * 
   * <p>
   * This method performs a single ensureCapacity() call for the maximum possible varint size (10
   * bytes), eliminating per-byte capacity checks.
   * 
   * @param value the unsigned long value to write
   * @return the number of bytes written (1-10)
   */
  public int writeVarLong(long value) {
    // Max varint size is 10 bytes (ceil(64/7))
    ensureCapacity(position + 10);

    // Fast path for 1-byte values (0-127) - most common case
    if ((value & ~0x7FL) == 0) {
      segment.set(ValueLayout.JAVA_BYTE, position, (byte) value);
      position += 1;
      return 1;
    }

    // Fast path for 2-byte values (128-16383) - second most common
    if ((value & ~0x3FFFL) == 0) {
      segment.set(ValueLayout.JAVA_BYTE, position, (byte) ((value & 0x7F) | 0x80));
      segment.set(ValueLayout.JAVA_BYTE, position + 1, (byte) (value >>> 7));
      position += 2;
      return 2;
    }

    // General case for larger values
    int bytesWritten = 0;
    while ((value & ~0x7FL) != 0) {
      segment.set(ValueLayout.JAVA_BYTE, position + bytesWritten, (byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
      bytesWritten++;
    }
    segment.set(ValueLayout.JAVA_BYTE, position + bytesWritten++, (byte) value);
    position += bytesWritten;
    return bytesWritten;
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
   * Write data from a MemorySegment directly, without intermediate byte[] allocation. This is a
   * high-performance method for bulk data transfer between segments.
   * 
   * <p>
   * Uses {@link MemorySegment#copy(MemorySegment, long, MemorySegment, long, long)} for efficient
   * zero-copy transfer.
   * 
   * @param source the source segment to copy from
   * @param sourceOffset the offset in the source segment
   * @param length the number of bytes to copy
   */
  public void writeSegment(MemorySegment source, long sourceOffset, long length) {
    if (length <= 0) {
      return; // Handle edge case: no-op for zero or negative length
    }
    ensureCapacity(position + length);
    MemorySegment.copy(source, sourceOffset, segment, position, length);
    position += length;
  }

  /**
   * No-op close method for compatibility. Since this uses Arena.ofAuto(), memory is automatically
   * released by the garbage collector when the GrowingMemorySegment is no longer reachable. Explicit
   * closing is not possible with automatic arenas.
   */
  public void close() {
    // No-op: Arena.ofAuto() is managed by GC
  }

  /**
   * Check if this segment is still alive. For heap-backed segments, the segment is always alive as
   * long as it's reachable. For arena-backed segments, checks the arena's scope.
   * 
   * @return true if the segment is still valid
   */
  public boolean isAlive() {
    if (heapBacked) {
      // Heap-backed segments are always alive while reachable
      return true;
    }
    return arena.scope().isAlive();
  }
}
