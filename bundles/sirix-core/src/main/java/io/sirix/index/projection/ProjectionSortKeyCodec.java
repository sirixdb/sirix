/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;
import java.util.Objects;

/**
 * Order-preserving keys for a projection's independently sorted access path.
 *
 * <p>Every declared field has one fixed type in the index definition. The caller appends fields in
 * that order and finishes a row with its stable record key. Missing sorts before present. Numeric
 * values use sign-flipped big-endian bytes; strings use unsigned UTF-8 bytes with zero escaping and
 * a zero-zero terminator. The resulting unsigned byte order is the tuple order, and a complete
 * field prefix can be used directly as a lower-bound seek key.</p>
 *
 * <p>A dictionary id is never a sort key: dictionary ids follow insertion order, not value order.
 * Builders and mutation listeners must resolve a string's UTF-8 bytes before calling
 * {@link Writer#appendUtf8(byte[], int, int)}. The writer reuses its buffer across rows and copies
 * only when the caller retains a finished key.</p>
 */
final class ProjectionSortKeyCodec {

  private static final int DEFAULT_CAPACITY = 128;
  private static final int MAX_KEY_BYTES = Integer.MAX_VALUE - 8;
  private static final byte MISSING = 0;
  private static final byte PRESENT = 1;

  private ProjectionSortKeyCodec() {}

  /** Smallest key strictly above every key beginning with {@code prefix}, or null if unbounded. */
  static byte[] prefixUpperExclusive(final byte[] prefix) {
    Objects.requireNonNull(prefix, "prefix");
    for (int i = prefix.length - 1; i >= 0; i--) {
      if ((prefix[i] & 0xFF) != 0xFF) {
        final byte[] upper = Arrays.copyOf(prefix, i + 1);
        upper[i]++;
        return upper;
      }
    }
    return null;
  }

  /** Transaction-confined scratch for encoding one key at a time without per-field objects. */
  static final class Writer {

    private byte[] bytes = new byte[DEFAULT_CAPACITY];
    private int length;

    void reset() {
      length = 0;
    }

    int length() {
      return length;
    }

    void appendMissing() {
      ensureCapacity(1);
      bytes[length++] = MISSING;
    }

    void appendLong(final long value) {
      ensureCapacity(9);
      bytes[length++] = PRESENT;
      appendBigEndian(value ^ Long.MIN_VALUE);
    }

    void appendBoolean(final boolean value) {
      ensureCapacity(2);
      bytes[length++] = PRESENT;
      bytes[length++] = value ? (byte) 1 : (byte) 0;
    }

    void appendUtf8(final byte[] value, final int offset, final int valueLength) {
      Objects.requireNonNull(value, "value");
      Objects.checkFromIndexSize(offset, valueLength, value.length);
      int zeroCount = 0;
      for (int i = offset, end = offset + valueLength; i < end; i++) {
        if (value[i] == 0) {
          zeroCount++;
        }
      }
      if ((long) valueLength + zeroCount + 3L > MAX_KEY_BYTES - length) {
        throw new IllegalArgumentException("projection sort key is too large");
      }
      ensureCapacity(valueLength + zeroCount + 3);
      bytes[length++] = PRESENT;
      if (zeroCount == 0) {
        System.arraycopy(value, offset, bytes, length, valueLength);
        length += valueLength;
      } else {
        for (int i = offset, end = offset + valueLength; i < end; i++) {
          final byte next = value[i];
          bytes[length++] = next;
          if (next == 0) {
            bytes[length++] = (byte) 0xFF;
          }
        }
      }
      bytes[length++] = 0;
      bytes[length++] = 0;
    }

    /** Unique row suffix after all declared sort fields. */
    void appendRecordKey(final long recordKey) {
      if (recordKey < 0) {
        throw new IllegalArgumentException("record key must be nonnegative");
      }
      ensureCapacity(Long.BYTES);
      appendBigEndian(recordKey ^ Long.MIN_VALUE);
    }

    byte[] copyKey() {
      return Arrays.copyOf(bytes, length);
    }

    /** Borrowed until the next append or reset; a run accumulator copies it immediately. */
    byte[] bytesRef() {
      return bytes;
    }

    private void appendBigEndian(final long value) {
      bytes[length++] = (byte) (value >>> 56);
      bytes[length++] = (byte) (value >>> 48);
      bytes[length++] = (byte) (value >>> 40);
      bytes[length++] = (byte) (value >>> 32);
      bytes[length++] = (byte) (value >>> 24);
      bytes[length++] = (byte) (value >>> 16);
      bytes[length++] = (byte) (value >>> 8);
      bytes[length++] = (byte) value;
    }

    private void ensureCapacity(final int additional) {
      if (additional < 0 || additional > MAX_KEY_BYTES - length) {
        throw new IllegalArgumentException("projection sort key is too large");
      }
      final int required = length + additional;
      if (required <= bytes.length) {
        return;
      }
      final int doubled = bytes.length <= MAX_KEY_BYTES / 2 ? bytes.length << 1 : MAX_KEY_BYTES;
      bytes = Arrays.copyOf(bytes, Math.max(required, doubled));
    }
  }
}
