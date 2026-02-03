package io.sirix.node;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Utility methods for reading data from MemorySegment with support for variable-length encoding.
 */
public final class MemorySegmentUtils {

  private MemorySegmentUtils() {
  }

  /**
   * Helper class to track current offset position during sequential reads from MemorySegment.
   */
  public static class MemorySegmentOffset {
    private long offset;

    public MemorySegmentOffset(long initialOffset) {
      this.offset = initialOffset;
    }

    public long get() {
      return offset;
    }

    public void advance(long bytes) {
      this.offset += bytes;
    }

    public void set(long newOffset) {
      this.offset = newOffset;
    }
  }

  /**
   * Result class for variable-length reads that includes the value and the number of bytes consumed.
   */
  public static class VarLongResult {
    public final long value;
    public final int bytesConsumed;

    public VarLongResult(long value, int bytesConsumed) {
      this.value = value;
      this.bytesConsumed = bytesConsumed;
    }
  }

  /**
   * Read a variable-length long value from MemorySegment using stop-bit encoding.
   * This mimics Chronicle Bytes' readStopBit() functionality.
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to start reading from
   * @return VarLongResult containing the value and bytes consumed
   */
  public static VarLongResult readVarLong(final MemorySegment memorySegment, final long offset) {
    long value = 0;
    int shift = 0;
    int bytesRead = 0;

    while (true) {
      byte b = memorySegment.get(ValueLayout.JAVA_BYTE, offset + bytesRead);
      bytesRead++;

      // Stop-bit encoding: if MSB is 0, this is the last byte
      if ((b & 0x80) == 0) {
        value |= (long)(b & 0x7F) << shift;
        break;
      } else {
        value |= (long)(b & 0x7F) << shift;
        shift += 7;
      }
    }

    return new VarLongResult(value, bytesRead);
  }

  /**
   * Read a variable-length long value from MemorySegment, advancing the offset tracker.
   * <p>
   * This is the zero-allocation variant that updates the offset in-place instead of
   * returning a result object. Use this on hot paths where allocation must be avoided.
   * </p>
   *
   * @param memorySegment the MemorySegment to read from
   * @param offsetTracker the offset tracker (will be advanced by bytes consumed)
   * @return the decoded long value
   */
  public static long readVarLongDirect(final MemorySegment memorySegment, final MemorySegmentOffset offsetTracker) {
    long value = 0;
    int shift = 0;
    long offset = offsetTracker.get();

    while (true) {
      byte b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++);

      // Stop-bit encoding: if MSB is 0, this is the last byte
      if ((b & 0x80) == 0) {
        value |= (long)(b & 0x7F) << shift;
        break;
      } else {
        value |= (long)(b & 0x7F) << shift;
        shift += 7;
      }
    }

    offsetTracker.set(offset);
    return value;
  }

  /**
   * Read a variable-length int value from MemorySegment, advancing the offset tracker.
   * <p>
   * Optimized for reading values that fit in an int. Use this when you know
   * the value won't exceed Integer.MAX_VALUE.
   * </p>
   *
   * @param memorySegment the MemorySegment to read from
   * @param offsetTracker the offset tracker (will be advanced by bytes consumed)
   * @return the decoded int value
   */
  public static int readVarIntDirect(final MemorySegment memorySegment, final MemorySegmentOffset offsetTracker) {
    int value = 0;
    int shift = 0;
    long offset = offsetTracker.get();

    while (true) {
      byte b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++);

      if ((b & 0x80) == 0) {
        value |= (b & 0x7F) << shift;
        break;
      } else {
        value |= (b & 0x7F) << shift;
        shift += 7;
      }
    }

    offsetTracker.set(offset);
    return value;
  }

  /**
   * Read a boolean from MemorySegment at the given offset.
   *
   * @param segment the MemorySegment to read from
   * @param offset the offset to read from
   * @return the boolean value
   */
  public static boolean readBoolean(MemorySegment segment, long offset) {
    return segment.get(ValueLayout.JAVA_BOOLEAN, offset);
  }

  /**
   * Read an int from MemorySegment at the given offset (handles unaligned access).
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to read from
   * @return the int value
   */
  public static int readInt(final MemorySegment memorySegment, final long offset) {
    return memorySegment.get(ValueLayout.JAVA_INT, offset);
  }

  /**
   * Read a long from MemorySegment at the given offset (handles unaligned access).
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to read from
   * @return the long value
   */
  public static long readLong(final MemorySegment memorySegment, final long offset) {
    return memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
  }

  /**
   * Read a byte from MemorySegment at the given offset.
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to read from
   * @return the byte value
   */
  public static byte readByte(final MemorySegment memorySegment, final long offset) {
    return memorySegment.get(ValueLayout.JAVA_BYTE, offset);
  }

  /**
   * Read a byte array from MemorySegment at the given offset.
   * <p>
   * Uses bulk copy for optimal performance instead of byte-by-byte iteration.
   * </p>
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to read from
   * @param length the number of bytes to read
   * @return the byte array
   */
  public static byte[] readByteArray(final MemorySegment memorySegment, final long offset, final int length) {
    byte[] result = new byte[length];
    // Bulk copy is significantly faster than byte-by-byte iteration
    MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, offset,
                       result, 0, length);
    return result;
  }

  /**
   * Read a byte array from MemorySegment into an existing array (zero-allocation read).
   * <p>
   * Uses bulk copy for optimal performance. Caller provides the destination array
   * to avoid allocation on hot paths.
   * </p>
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to read from
   * @param dest the destination byte array
   * @param destOffset the offset in the destination array
   * @param length the number of bytes to read
   */
  public static void readByteArrayInto(final MemorySegment memorySegment, final long offset,
                                       final byte[] dest, final int destOffset, final int length) {
    MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, offset,
                       dest, destOffset, length);
  }

  /**
   * Read a double from MemorySegment at the given offset (handles unaligned access).
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to read from
   * @return the double value
   */
  public static double readDouble(final MemorySegment memorySegment, final long offset) {
    return memorySegment.get(ValueLayout.JAVA_DOUBLE, offset);
  }

  /**
   * Read a float from MemorySegment at the given offset (handles unaligned access).
   *
   * @param memorySegment the MemorySegment to read from
   * @param offset the offset to read from
   * @return the float value
   */
  public static float readFloat(final MemorySegment memorySegment, final long offset) {
    return memorySegment.get(ValueLayout.JAVA_FLOAT, offset);
  }

  /**
   * Write a variable-length long value to MemorySegment using stop-bit encoding.
   * This mimics Chronicle Bytes' writeStopBit() functionality.
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to start writing at
   * @param value the long value to write
   * @return the number of bytes written
   */
  public static int writeVarLong(final MemorySegment memorySegment, final long offset, final long value) {
    long val = value;
    int bytesWritten = 0;
    
    while (val >= 0x80) {
      memorySegment.set(ValueLayout.JAVA_BYTE, offset + bytesWritten, (byte) (val | 0x80));
      val >>>= 7;
      bytesWritten++;
    }
    
    memorySegment.set(ValueLayout.JAVA_BYTE, offset + bytesWritten, (byte) val);
    bytesWritten++;
    
    return bytesWritten;
  }

  /**
   * Write a boolean to MemorySegment at the given offset.
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the boolean value to write
   */
  public static void writeBoolean(final MemorySegment memorySegment, final long offset, final boolean value) {
    memorySegment.set(ValueLayout.JAVA_BOOLEAN, offset, value);
  }

  /**
   * Write an int to MemorySegment at the given offset.
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the int value to write
   */
  public static void writeInt(final MemorySegment memorySegment, final long offset, final int value) {
    memorySegment.set(ValueLayout.JAVA_INT, offset, value);
  }

  /**
   * Write a long to MemorySegment at the given offset.
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the long value to write
   */
  public static void writeLong(final MemorySegment memorySegment, final long offset, final long value) {
    memorySegment.set(ValueLayout.JAVA_LONG, offset, value);
  }

  /**
   * Write a byte to MemorySegment at the given offset.
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the byte value to write
   */
  public static void writeByte(final MemorySegment memorySegment, final long offset, final byte value) {
    memorySegment.set(ValueLayout.JAVA_BYTE, offset, value);
  }

  /**
   * Write a byte array to MemorySegment at the given offset.
   * <p>
   * Uses bulk copy for optimal performance instead of byte-by-byte iteration.
   * </p>
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the byte array to write
   */
  public static void writeByteArray(final MemorySegment memorySegment, final long offset, final byte[] value) {
    // Bulk copy is significantly faster than byte-by-byte iteration
    MemorySegment.copy(value, 0, memorySegment, ValueLayout.JAVA_BYTE, offset, value.length);
  }

  /**
   * Write a portion of a byte array to MemorySegment at the given offset.
   * <p>
   * Uses bulk copy for optimal performance.
   * </p>
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the byte array to write from
   * @param srcOffset the offset in the source array
   * @param length the number of bytes to write
   */
  public static void writeByteArray(final MemorySegment memorySegment, final long offset,
                                    final byte[] value, final int srcOffset, final int length) {
    MemorySegment.copy(value, srcOffset, memorySegment, ValueLayout.JAVA_BYTE, offset, length);
  }

  /**
   * Write a double to MemorySegment at the given offset.
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the double value to write
   */
  public static void writeDouble(final MemorySegment memorySegment, final long offset, final double value) {
    memorySegment.set(ValueLayout.JAVA_DOUBLE, offset, value);
  }

  /**
   * Write a float to MemorySegment at the given offset.
   *
   * @param memorySegment the MemorySegment to write to
   * @param offset the offset to write at
   * @param value the float value to write
   */
  public static void writeFloat(final MemorySegment memorySegment, final long offset, final float value) {
    memorySegment.set(ValueLayout.JAVA_FLOAT, offset, value);
  }
}