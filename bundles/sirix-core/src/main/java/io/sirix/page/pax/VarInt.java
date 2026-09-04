/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * LEB128 variable-length integers for PAX region headers.
 *
 * <h2>Why region headers stopped using fixed widths</h2>
 *
 * <p>
 * A region header is a run of small parallel arrays — a tag dictionary, a value count per tag, a
 * frame-of-reference base per tag. Written as {@code int}/{@code long} those arrays cost 4 and 8
 * bytes for numbers that are almost always below 128, and on a page holding a wide record schema
 * the per-tag framing came to as much as the values it framed. A varint spends one byte on those
 * and pays a byte back only on the rare wide value, so the framing shrinks with the data rather
 * than with the declared type.
 *
 * <h2>Canonical by contract</h2>
 *
 * <p>
 * {@link #readUnsigned} rejects a non-canonical encoding (a continuation group whose payload bits
 * are all zero, e.g. {@code 0x80 0x00} for zero). That is not pedantry: it makes
 * {@link #sizeOfUnsigned(long)} of the decoded value <em>exactly</em> the number of bytes consumed,
 * so a sequential parse advances with arithmetic instead of a second scan, and a corrupt payload
 * fails at the byte that is wrong instead of desynchronising the rest of the header.
 *
 * <p>
 * Signed values go through zig-zag first, so a small negative costs one byte rather than ten.
 *
 * <p>
 * All methods are static, allocation-free and branch-light; nothing here boxes.
 */
public final class VarInt {

  /** Bytes a 64-bit value can occupy: {@code ceil(64 / 7)}. */
  public static final int MAX_BYTES = 10;

  private VarInt() {}

  /** Map a signed value onto an unsigned one that is small when the magnitude is small. */
  public static long zigZagEncode(final long value) {
    return (value << 1) ^ (value >> 63);
  }

  /** Inverse of {@link #zigZagEncode(long)}. */
  public static long zigZagDecode(final long encoded) {
    return (encoded >>> 1) ^ -(encoded & 1L);
  }

  /**
   * Encoded length of {@code value} read as UNSIGNED — a negative long is a 64-bit magnitude and
   * costs {@link #MAX_BYTES}.
   */
  public static int sizeOfUnsigned(final long value) {
    if (value == 0L) {
      return 1;
    }
    final int bits = 64 - Long.numberOfLeadingZeros(value);
    return (bits + 6) / 7;
  }

  /** Encoded length of {@code value} after zig-zag. */
  public static int sizeOfSigned(final long value) {
    return sizeOfUnsigned(zigZagEncode(value));
  }

  /**
   * Write {@code value} as an unsigned varint.
   *
   * @return the offset one past the last byte written
   */
  public static int writeUnsigned(final byte[] out, final int offset, final long value) {
    int position = offset;
    long remaining = value;
    while ((remaining & ~0x7FL) != 0L) {
      out[position++] = (byte) ((remaining & 0x7FL) | 0x80L);
      remaining >>>= 7;
    }
    out[position++] = (byte) remaining;
    return position;
  }

  /**
   * Write {@code value} zig-zagged.
   *
   * @return the offset one past the last byte written
   */
  public static int writeSigned(final byte[] out, final int offset, final long value) {
    return writeUnsigned(out, offset, zigZagEncode(value));
  }

  /**
   * Read the canonical unsigned varint at {@code offset}.
   *
   * <p>
   * Advance the cursor by {@link #sizeOfUnsigned(long)} of the result: the canonicality check makes
   * that identity hold for every payload this method returns from.
   *
   * @throws IllegalArgumentException when the varint runs past the payload, exceeds
   *         {@link #MAX_BYTES}, or is encoded non-canonically
   */
  public static long readUnsigned(final MemorySegment payload, final long offset) {
    final long limit = payload.byteSize();
    long value = 0L;
    int shift = 0;
    long position = offset;
    for (int consumed = 0; consumed < MAX_BYTES; consumed++) {
      if (position >= limit) {
        throw new IllegalArgumentException("varint at " + offset + " runs past the payload end " + limit);
      }
      final int b = payload.get(ValueLayout.JAVA_BYTE, position++) & 0xFF;
      if (shift == 63 && (b & 0x7E) != 0) {
        // Only bit 63 is left; a group claiming more bits would overflow the long silently.
        throw new IllegalArgumentException("varint at " + offset + " overflows 64 bits");
      }
      value |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        if (consumed > 0 && (b & 0x7F) == 0) {
          throw new IllegalArgumentException("non-canonical varint at " + offset + ": trailing zero group");
        }
        return value;
      }
      shift += 7;
    }
    throw new IllegalArgumentException("varint at " + offset + " is longer than " + MAX_BYTES + " bytes");
  }

  /** {@link #readUnsigned} followed by {@link #zigZagDecode}. */
  public static long readSigned(final MemorySegment payload, final long offset) {
    return zigZagDecode(readUnsigned(payload, offset));
  }
}
