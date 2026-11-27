package io.sirix.index.hot;

import io.sirix.node.SegmentAccess;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.function.IntConsumer;

/** Byte-width frame of reference for an immutable chunk-local HOT posting. */
public final class HOTPostingFrame {
  public static final byte TAG = 1;
  private static final byte RAW = 0;
  private static final int HEADER_BYTES = 5;
  private static final int MAX_COUNT = 64;
  private static final int MAX_BIT = 0xffff;
  private static final ValueLayout.OfShort SHORT_BE = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
  private static final ValueLayout.OfLong LONG_BE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

  private HOTPostingFrame() {}

  /**
   * Convert an ordinary packed posting when every key is an ascending chunk-local ordinal. A zero
   * return leaves the source form selected.
   */
  public static int encodeIfSmaller(final MemorySegment source, final int start, final int length,
      final MemorySegment destination, final int output) {
    if (length < 2 || SegmentAccess.getByte(source, start) != RAW) {
      return 0;
    }
    final int count = unsignedByte(source, start + 1);
    if (count == 0 || count > MAX_COUNT || length != 2 + count * Long.BYTES) {
      return 0;
    }
    final long first = rawOrdinal(source, start, 0);
    final long last = rawOrdinal(source, start, count - 1);
    if (first < 0 || last < first || last > MAX_BIT) {
      return 0;
    }
    final int base = (int) first;
    final int width = count == 1
        ? 0
        : last - first <= 255
            ? 1
            : 2;
    final int packedLength = HEADER_BYTES + (count - 1) * width;
    if (packedLength >= length) {
      return 0;
    }

    destination.set(ValueLayout.JAVA_BYTE, output, TAG);
    destination.set(ValueLayout.JAVA_BYTE, output + 1L, (byte) count);
    destination.set(SHORT_BE, output + 2L, (short) base);
    destination.set(ValueLayout.JAVA_BYTE, output + 4L, (byte) width);
    long previous = first;
    for (int ordinal = 1; ordinal < count; ordinal++) {
      final long bit = rawOrdinal(source, start, ordinal);
      if (bit <= previous || bit > last) {
        return 0;
      }
      final int offset = (int) bit - base;
      final int at = output + HEADER_BYTES + (ordinal - 1) * width;
      if (width == 1) {
        destination.set(ValueLayout.JAVA_BYTE, at, (byte) offset);
      } else {
        destination.set(SHORT_BE, at, (short) offset);
      }
      previous = bit;
    }
    return packedLength;
  }

  /** Validate the exact wire span before publishing a leaf that contains it. */
  public static void validate(final MemorySegment source, final int start, final int length) {
    if (length < HEADER_BYTES || SegmentAccess.getByte(source, start) != TAG) {
      throw malformed();
    }
    final int count = unsignedByte(source, start + 1);
    final int base = unsignedShort(source, start + 2);
    final int width = unsignedByte(source, start + 4);
    if (count == 0 || count > MAX_COUNT || width > 2 || (count == 1) != (width == 0)
        || length != HEADER_BYTES + (count - 1) * width) {
      throw malformed();
    }
    int previous = base;
    for (int ordinal = 1; ordinal < count; ordinal++) {
      final int value = frameOrdinal(source, start, base, width, ordinal);
      if (value <= previous || value > MAX_BIT) {
        throw malformed();
      }
      previous = value;
    }
    if (width == 2 && previous - base <= 255) {
      throw malformed();
    }
  }

  private static IllegalStateException malformed() {
    return new IllegalStateException("Malformed HOT frame-of-reference posting");
  }

  /** Number of ordinals in a validated packed posting, in either representation. */
  static int count(final MemorySegment source, final int start) {
    return unsignedByte(source, start + 1);
  }

  /** Read one ordinal from a validated packed posting, in either representation. */
  public static int ordinalAt(final MemorySegment source, final int start, final int ordinal) {
    if (SegmentAccess.getByte(source, start) == RAW) {
      return (int) rawOrdinal(source, start, ordinal);
    }
    return frameOrdinal(source, start, unsignedShort(source, start + 2), unsignedByte(source, start + 4), ordinal);
  }

  /**
   * The index of a chunk-local ordinal, with the same insertion point convention as binary search.
   */
  public static int indexOf(final MemorySegment source, final int start, final int wanted) {
    final int shape = shape(source, start);
    int low = 0;
    int high = count(source, start);
    while (low < high) {
      final int middle = (low + high) >>> 1;
      final int bit = ordinalAt(source, start, shape, middle);
      if (bit == wanted) {
        return middle;
      }
      if (bit < wanted) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return -low - 1;
  }

  /** Intersect two validated postings from the same chunk, emitting only matching ordinals. */
  public static void intersect(final MemorySegment left, final int leftStart, final MemorySegment right,
      final int rightStart, final IntConsumer result) {
    final int leftCount = count(left, leftStart);
    final int rightCount = count(right, rightStart);
    final int leftShape = shape(left, leftStart);
    final int rightShape = shape(right, rightStart);
    int i = 0;
    int j = 0;
    int a = ordinalAt(left, leftStart, leftShape, 0);
    int b = ordinalAt(right, rightStart, rightShape, 0);
    for (;;) {
      if (a == b) {
        result.accept(a);
      }
      if (a <= b) {
        if (++i == leftCount) {
          return;
        }
        if (a == b) {
          if (++j == rightCount) {
            return;
          }
          b = ordinalAt(right, rightStart, rightShape, j);
        }
        a = ordinalAt(left, leftStart, leftShape, i);
      } else {
        if (++j == rightCount) {
          return;
        }
        b = ordinalAt(right, rightStart, rightShape, j);
      }
    }
  }

  /** Emit ordinary mutable-slot bytes for a validated frame. */
  public static int copyOrdinary(final MemorySegment source, final int start, final MemorySegment destination,
      final int output) {
    final int count = count(source, start);
    final int shape = shape(source, start);
    destination.set(ValueLayout.JAVA_BYTE, output, RAW);
    destination.set(ValueLayout.JAVA_BYTE, output + 1L, (byte) count);
    for (int ordinal = 0; ordinal < count; ordinal++) {
      destination.set(LONG_BE, output + 2L + (long) Long.BYTES * ordinal, ordinalAt(source, start, shape, ordinal));
    }
    return 2 + Long.BYTES * count;
  }

  public static int ordinaryLength(final MemorySegment source, final int start) {
    return 2 + Long.BYTES * count(source, start);
  }

  static int shape(final MemorySegment source, final int start) {
    return SegmentAccess.getByte(source, start) == RAW
        ? -1
        : unsignedShort(source, start + 2) | unsignedByte(source, start + 4) << Short.SIZE;
  }

  static int ordinalAt(final MemorySegment source, final int start, final int shape, final int ordinal) {
    return shape < 0
        ? (int) rawOrdinal(source, start, ordinal)
        : frameOrdinal(source, start, shape & MAX_BIT, shape >>> Short.SIZE, ordinal);
  }

  private static long rawOrdinal(final MemorySegment source, final int start, final int ordinal) {
    return SegmentAccess.getLongBE(source, start + 2L + (long) Long.BYTES * ordinal);
  }

  private static int frameOrdinal(final MemorySegment source, final int start, final int base, final int width,
      final int ordinal) {
    if (ordinal == 0) {
      return base;
    }
    final int offset = start + HEADER_BYTES + (ordinal - 1) * width;
    return base + (width == 1
        ? unsignedByte(source, offset)
        : unsignedShort(source, offset));
  }

  private static int unsignedByte(final MemorySegment source, final int offset) {
    return Byte.toUnsignedInt(SegmentAccess.getByte(source, offset));
  }

  private static int unsignedShort(final MemorySegment source, final int offset) {
    return Short.toUnsignedInt(SegmentAccess.getShortBE(source, offset));
  }
}
