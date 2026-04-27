/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.node;

import io.sirix.settings.Fixed;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Page-level column codec for a single structural-key column
 * (parentKey, firstChildKey, leftSiblingKey, rightSiblingKey) across all slots in a
 * {@link io.sirix.page.KeyValueLeafPage}.
 *
 * <p>Replaces per-slot {@code delta(value, nodeKey)} varint encoding with a
 * column-wide scheme that exploits DFS-order locality: sibling groups share the
 * same {@code parentKey}, so most column values repeat the previous slot.
 *
 * <h2>Encoding</h2>
 * <ul>
 *   <li>{@code FLAG_ALL_NULL}: column is entirely {@link Fixed#NULL_NODE_KEY}. Fixed 3 bytes.</li>
 *   <li>{@code FLAG_CONSTANT}: all slots hold the same value. 11 bytes regardless of N.</li>
 *   <li>{@code FLAG_SEQUENTIAL_PLUS1}: slot i equals {@code base + i}. 11 bytes regardless of N.</li>
 *   <li>{@code FLAG_HAS_BITMAP} (general): 1 bit per slot indicates "equal to predictor"; for
 *       slots where the bit is zero, a zig-zag varint of {@code delta = value - predictor}
 *       follows in an override stream. The predictor is the previous slot's decoded value
 *       (or {@code NULL} for slot 0). In DFS insert order this pattern is dense: sibling
 *       chains, null-child columns, and repeated sentinel values all collapse to 1 bit.</li>
 * </ul>
 *
 * <h2>Random access</h2>
 * The general case decodes {@code O(slotIndex)} bytes from the override stream.
 * For scan workloads this is negligible; for random-access OLTP a per-column
 * index (one entry every 16 slots) can be bolted on top without changing the wire format.
 *
 * <h2>Design choices</h2>
 * The predictor is "previous slot's value" rather than "this slot's nodeKey" because
 * across a DFS shred {@code parentKey[i] == parentKey[i-1]} is the dominant case;
 * delta-from-nodeKey can't encode that in fewer than one varint byte.
 *
 * <p>Format byte is exactly one of the four flags (not a combination) for now;
 * reserved bits are for future format-version extensions.
 */
public final class StructuralKeyColumnCodec {

  public static final int FLAG_ALL_NULL = 0x01;
  public static final int FLAG_CONSTANT = 0x02;
  public static final int FLAG_SEQUENTIAL_PLUS1 = 0x04;
  public static final int FLAG_HAS_BITMAP = 0x08;

  /** Maximum supported slots per column; fits in an unsigned 16-bit length field. */
  public static final int MAX_SLOTS = 0xFFFF;

  private static final long NULL = Fixed.NULL_NODE_KEY.getStandardProperty();

  private StructuralKeyColumnCodec() {}

  /**
   * Compute the encoded size in bytes without materializing the output.
   *
   * @param values one value per slot
   * @return encoded size in bytes
   */
  public static int encodedSize(final long[] values) {
    return encodeByteArray(null, 0, values, values.length);
  }

  /**
   * Compute the encoded size over {@code values[0..n)} without materializing
   * the output. Allows pre-sized scratch arrays with {@code length > n}.
   */
  public static int encodedSize(final long[] values, final int n) {
    return encodeByteArray(null, 0, values, n);
  }

  /**
   * Encode the column into a {@code byte[]} at {@code offset}. Pass {@code target == null}
   * to dry-run and only compute the size.
   *
   * @return bytes written
   */
  public static int encodeByteArray(final byte[] target, final int offset, final long[] values) {
    return encodeByteArray(target, offset, values, values.length);
  }

  /**
   * Encode the first {@code n} values of {@code values} into {@code target} at
   * {@code offset}. Pass {@code target == null} to dry-run.
   */
  public static int encodeByteArray(final byte[] target, final int offset, final long[] values,
      final int n) {
    if (n > MAX_SLOTS) {
      throw new IllegalArgumentException("Column too large: " + n + " > " + MAX_SLOTS);
    }
    if (n < 0 || n > values.length) {
      throw new IllegalArgumentException("Invalid n=" + n + " for values.length=" + values.length);
    }

    // 3-byte header: tag (1) + slotCount (2, big-endian).
    if (n == 0) {
      if (target != null) {
        target[offset] = 0;
        writeUnsignedShort(target, offset + 1, 0);
      }
      return 3;
    }

    // Pattern detection: all-null, constant, monotonic +1.
    boolean allNull = true;
    boolean constant = true;
    boolean monotonic = true;
    final long v0 = values[0];
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != NULL) allNull = false;
      if (v != v0) constant = false;
      if (v != v0 + i) monotonic = false;
    }

    if (allNull) {
      if (target != null) {
        target[offset] = FLAG_ALL_NULL;
        writeUnsignedShort(target, offset + 1, n);
      }
      return 3;
    }
    if (constant) {
      if (target != null) {
        target[offset] = FLAG_CONSTANT;
        writeUnsignedShort(target, offset + 1, n);
        writeLong(target, offset + 3, v0);
      }
      return 11;
    }
    if (monotonic) {
      if (target != null) {
        target[offset] = FLAG_SEQUENTIAL_PLUS1;
        writeUnsignedShort(target, offset + 1, n);
        writeLong(target, offset + 3, v0);
      }
      return 11;
    }

    // General: bitmap + override-varint stream.
    final int bitmapBytes = (n + 7) >>> 3;

    // Dry-run to size override stream.
    int overrideBytes = 0;
    long predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != predictor) {
        overrideBytes += zigzagVarintSize(v - predictor);
      }
      predictor = v;
    }

    final int totalBytes = 1 + 2 + bitmapBytes + overrideBytes;
    if (target == null) {
      return totalBytes;
    }

    target[offset] = FLAG_HAS_BITMAP;
    writeUnsignedShort(target, offset + 1, n);
    // zero bitmap
    for (int i = 0; i < bitmapBytes; i++) {
      target[offset + 3 + i] = 0;
    }

    int writePos = offset + 3 + bitmapBytes;
    predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v == predictor) {
        target[offset + 3 + (i >>> 3)] |= (byte) (1 << (i & 7));
      } else {
        writePos = writeZigzagVarintToBytes(target, writePos, v - predictor);
      }
      predictor = v;
    }
    return totalBytes;
  }

  /**
   * Random access decode of a single slot from a byte-array-encoded column.
   * Worst case {@code O(slotIndex)}; amortized O(1) for scan-then-decode.
   */
  public static long decodeSlot(final byte[] src, final int columnOffset, final int slotIndex) {
    final int tag = src[columnOffset] & 0xFF;
    final int n = readUnsignedShort(src, columnOffset + 1);
    if (slotIndex < 0 || slotIndex >= n) {
      throw new IndexOutOfBoundsException("slotIndex " + slotIndex + " out of [0," + n + ")");
    }
    if (tag == FLAG_ALL_NULL) {
      return NULL;
    }
    if (tag == FLAG_CONSTANT) {
      return readLong(src, columnOffset + 3);
    }
    if (tag == FLAG_SEQUENTIAL_PLUS1) {
      return readLong(src, columnOffset + 3) + slotIndex;
    }
    if (tag == FLAG_HAS_BITMAP) {
      final int bitmapBytes = (n + 7) >>> 3;
      int readPos = columnOffset + 3 + bitmapBytes;
      long predictor = NULL;
      for (int i = 0; i <= slotIndex; i++) {
        final int bit = (src[columnOffset + 3 + (i >>> 3)] >>> (i & 7)) & 1;
        final long value;
        if (bit == 1) {
          value = predictor;
        } else {
          final long delta = readZigzagVarintFromBytes(src, readPos);
          readPos += zigzagVarintSize(delta);
          value = predictor + delta;
        }
        if (i == slotIndex) {
          return value;
        }
        predictor = value;
      }
    }
    throw new IllegalStateException("Unknown column format tag: 0x" + Integer.toHexString(tag));
  }

  // ==================== MemorySegment variants ====================

  /**
   * Encode directly into a {@link MemorySegment}. Same layout as the byte-array variant.
   */
  public static int encode(final MemorySegment target, final long offset, final long[] values) {
    final int n = values.length;
    if (n > MAX_SLOTS) {
      throw new IllegalArgumentException("Column too large: " + n + " > " + MAX_SLOTS);
    }

    if (n == 0) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) 0);
        putUnsignedShort(target, offset + 1, 0);
      }
      return 3;
    }

    boolean allNull = true;
    boolean constant = true;
    boolean monotonic = true;
    final long v0 = values[0];
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != NULL) allNull = false;
      if (v != v0) constant = false;
      if (v != v0 + i) monotonic = false;
    }

    if (allNull) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_ALL_NULL);
        putUnsignedShort(target, offset + 1, n);
      }
      return 3;
    }
    if (constant) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_CONSTANT);
        putUnsignedShort(target, offset + 1, n);
        putLong(target, offset + 3, v0);
      }
      return 11;
    }
    if (monotonic) {
      if (target != null) {
        target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_SEQUENTIAL_PLUS1);
        putUnsignedShort(target, offset + 1, n);
        putLong(target, offset + 3, v0);
      }
      return 11;
    }

    final int bitmapBytes = (n + 7) >>> 3;
    int overrideBytes = 0;
    long predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v != predictor) {
        overrideBytes += zigzagVarintSize(v - predictor);
      }
      predictor = v;
    }
    final int totalBytes = 1 + 2 + bitmapBytes + overrideBytes;
    if (target == null) {
      return totalBytes;
    }

    target.set(ValueLayout.JAVA_BYTE, offset, (byte) FLAG_HAS_BITMAP);
    putUnsignedShort(target, offset + 1, n);
    for (int i = 0; i < bitmapBytes; i++) {
      target.set(ValueLayout.JAVA_BYTE, offset + 3 + i, (byte) 0);
    }

    long writePos = offset + 3 + bitmapBytes;
    predictor = NULL;
    for (int i = 0; i < n; i++) {
      final long v = values[i];
      if (v == predictor) {
        final long bmByte = offset + 3 + (i >>> 3);
        final int cur = target.get(ValueLayout.JAVA_BYTE, bmByte) & 0xFF;
        target.set(ValueLayout.JAVA_BYTE, bmByte, (byte) (cur | (1 << (i & 7))));
      } else {
        writePos = writeZigzagVarint(target, writePos, v - predictor);
      }
      predictor = v;
    }
    return totalBytes;
  }

  /** Random-access decode of a single slot from a MemorySegment-encoded column. */
  public static long decodeSlot(final MemorySegment src, final long columnOffset, final int slotIndex) {
    final int tag = src.get(ValueLayout.JAVA_BYTE, columnOffset) & 0xFF;
    final int n = getUnsignedShort(src, columnOffset + 1);
    if (slotIndex < 0 || slotIndex >= n) {
      throw new IndexOutOfBoundsException("slotIndex " + slotIndex + " out of [0," + n + ")");
    }
    if (tag == FLAG_ALL_NULL) return NULL;
    if (tag == FLAG_CONSTANT) return getLong(src, columnOffset + 3);
    if (tag == FLAG_SEQUENTIAL_PLUS1) return getLong(src, columnOffset + 3) + slotIndex;
    if (tag == FLAG_HAS_BITMAP) {
      final int bitmapBytes = (n + 7) >>> 3;
      long readPos = columnOffset + 3 + bitmapBytes;
      long predictor = NULL;
      for (int i = 0; i <= slotIndex; i++) {
        final int bit =
            (src.get(ValueLayout.JAVA_BYTE, columnOffset + 3 + (i >>> 3)) >>> (i & 7)) & 1;
        final long value;
        if (bit == 1) {
          value = predictor;
        } else {
          final long delta = readZigzagVarint(src, readPos);
          readPos += zigzagVarintSize(delta);
          value = predictor + delta;
        }
        if (i == slotIndex) return value;
        predictor = value;
      }
    }
    throw new IllegalStateException("Unknown column format tag: 0x" + Integer.toHexString(tag));
  }

  // ==================== Helpers ====================

  private static int zigzagVarintSize(final long v) {
    final long zz = (v << 1) ^ (v >> 63);
    if (zz == 0) return 1;
    // Number of 7-bit groups needed.
    final int bits = 64 - Long.numberOfLeadingZeros(zz);
    return (bits + 6) / 7;
  }

  private static int writeZigzagVarintToBytes(final byte[] target, final int offset, final long v) {
    long zz = (v << 1) ^ (v >> 63);
    int pos = offset;
    while ((zz & ~0x7FL) != 0L) {
      target[pos++] = (byte) (zz | 0x80L);
      zz >>>= 7;
    }
    target[pos++] = (byte) zz;
    return pos;
  }

  private static long readZigzagVarintFromBytes(final byte[] src, final int offset) {
    long zz = 0;
    int shift = 0;
    int pos = offset;
    while (true) {
      final byte b = src[pos++];
      zz |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    return (zz >>> 1) ^ -(zz & 1);
  }

  private static long writeZigzagVarint(final MemorySegment target, final long offset, final long v) {
    long zz = (v << 1) ^ (v >> 63);
    long pos = offset;
    while ((zz & ~0x7FL) != 0L) {
      target.set(ValueLayout.JAVA_BYTE, pos++, (byte) (zz | 0x80L));
      zz >>>= 7;
    }
    target.set(ValueLayout.JAVA_BYTE, pos++, (byte) zz);
    return pos;
  }

  private static long readZigzagVarint(final MemorySegment src, final long offset) {
    long zz = 0;
    int shift = 0;
    long pos = offset;
    while (true) {
      final byte b = src.get(ValueLayout.JAVA_BYTE, pos++);
      zz |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    return (zz >>> 1) ^ -(zz & 1);
  }

  private static void writeUnsignedShort(final byte[] target, final int offset, final int v) {
    target[offset] = (byte) ((v >>> 8) & 0xFF);
    target[offset + 1] = (byte) (v & 0xFF);
  }

  private static int readUnsignedShort(final byte[] src, final int offset) {
    return ((src[offset] & 0xFF) << 8) | (src[offset + 1] & 0xFF);
  }

  private static void writeLong(final byte[] target, final int offset, final long v) {
    for (int i = 0; i < 8; i++) {
      target[offset + i] = (byte) (v >>> (56 - 8 * i));
    }
  }

  private static long readLong(final byte[] src, final int offset) {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) | (src[offset + i] & 0xFF);
    }
    return v;
  }

  private static void putUnsignedShort(final MemorySegment target, final long offset, final int v) {
    target.set(ValueLayout.JAVA_BYTE, offset, (byte) ((v >>> 8) & 0xFF));
    target.set(ValueLayout.JAVA_BYTE, offset + 1, (byte) (v & 0xFF));
  }

  private static int getUnsignedShort(final MemorySegment src, final long offset) {
    return ((src.get(ValueLayout.JAVA_BYTE, offset) & 0xFF) << 8)
        | (src.get(ValueLayout.JAVA_BYTE, offset + 1) & 0xFF);
  }

  private static void putLong(final MemorySegment target, final long offset, final long v) {
    for (int i = 0; i < 8; i++) {
      target.set(ValueLayout.JAVA_BYTE, offset + i, (byte) (v >>> (56 - 8 * i)));
    }
  }

  private static long getLong(final MemorySegment src, final long offset) {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) | (src.get(ValueLayout.JAVA_BYTE, offset + i) & 0xFF);
    }
    return v;
  }
}
