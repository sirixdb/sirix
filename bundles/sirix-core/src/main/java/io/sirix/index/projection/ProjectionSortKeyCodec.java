/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;
import java.util.Objects;

/**
 * Order-preserving keys for a projection's independently sorted access path.
 *
 * <p>
 * Every declared field has one fixed type in the index definition. The caller appends fields in
 * that order and finishes a row with its stable record key. Missing sorts before present. Numeric
 * values use sign-flipped big-endian bytes; strings use unsigned UTF-8 bytes with zero escaping and
 * a zero-zero terminator. The resulting unsigned byte order is the tuple order, and a complete
 * field prefix can be used directly as a lower-bound seek key.
 * </p>
 *
 * <p>
 * A row whose sort field cannot be represented exactly (an unrepresentable or non-integral cell),
 * or whose whole key would exceed {@link #MAX_KEY_BYTES}, is kept under the reserved
 * {@link #UNENCODABLE} lead byte followed by its record key. Such keys sort after every ordinary
 * key, and the view counts them so readers can decline it while any exist.
 * </p>
 *
 * <p>
 * A dictionary id is never a sort key: dictionary ids follow insertion order, not value order.
 * Builders and mutation listeners must resolve a string's UTF-8 bytes before calling
 * {@link Writer#appendUtf8(byte[], int, int)}. The writer reuses its buffer across rows and copies
 * only when the caller retains a finished key.
 * </p>
 */
final class ProjectionSortKeyCodec {

  static final byte MISSING = 0;
  static final byte PRESENT = 1;
  static final byte UNENCODABLE = (byte) 0xFF;

  /**
   * Longest row key, record key included, kept in a view. Every leaf and directory node therefore has
   * room for at least fifteen keys, so eight directory levels address far more rows than a resource
   * can hold, and every key stays within a run's two-byte length prefix.
   */
  static final int MAX_KEY_BYTES = 4096;

  static final byte FIELD_STRING = 1;
  static final byte FIELD_LONG = 2;
  static final byte FIELD_BOOLEAN = 3;

  private static final int DEFAULT_CAPACITY = 128;
  private static final int MAX_BUFFER_BYTES = Integer.MAX_VALUE - 8;

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

  static boolean isUnencodable(final byte[] key, final int length) {
    return length > 0 && key[0] == UNENCODABLE;
  }

  /** Whether {@code key[0, length)} starts with the complete {@code prefix}. */
  static boolean startsWith(final byte[] key, final int length, final byte[] prefix) {
    return length >= prefix.length && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
  }

  /**
   * The field shapes of one sorted view, in key order. Persisted with the view so every reader and
   * writer parses keys identically without consulting the index definition.
   */
  static final class Layout {

    private final byte[] fields;

    Layout(final byte[] fields) {
      Objects.requireNonNull(fields, "fields");
      if (fields.length < 1 || fields.length > 0xFF) {
        throw new IllegalArgumentException("sorted key layout needs 1..255 fields");
      }
      for (final byte field : fields) {
        if (field != FIELD_STRING && field != FIELD_LONG && field != FIELD_BOOLEAN) {
          throw new IllegalArgumentException("invalid sorted key field shape " + field);
        }
      }
      this.fields = fields.clone();
    }

    /** Field shapes for {@code keyColumns} of a projection whose columns have {@code columnKinds}. */
    static Layout of(final byte[] columnKinds, final int[] keyColumns) {
      final byte[] shapes = new byte[keyColumns.length];
      for (int i = 0; i < keyColumns.length; i++) {
        final byte kind = columnKinds[keyColumns[i]];
        if (isStringKind(kind)) {
          shapes[i] = FIELD_STRING;
        } else if (ProjectionIndexRowGroupPage.isOrderedLongKind(kind)) {
          shapes[i] = FIELD_LONG;
        } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
          shapes[i] = FIELD_BOOLEAN;
        } else {
          throw new IllegalArgumentException(
              "sorted projection column " + keyColumns[i] + " has unsupported kind " + kind);
        }
      }
      return new Layout(shapes);
    }

    static boolean isStringKind(final byte kind) {
      return kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
          || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT;
    }

    int fieldCount() {
      return fields.length;
    }

    byte field(final int index) {
      return fields[index];
    }

    byte[] toBytes() {
      return fields.clone();
    }

    /** Grouped extrema apply when the last field is an ordered long and every other field groups. */
    boolean groupsByLastLong() {
      return fields[fields.length - 1] == FIELD_LONG;
    }

    /** End of field {@code index} starting at {@code at}, bounded by {@code end}; -1 if malformed. */
    int fieldEnd(final byte[] key, final int at, final int end, final int index) {
      if (at >= end) {
        return -1;
      }
      final byte marker = key[at];
      if (marker == MISSING) {
        return at + 1;
      }
      if (marker != PRESENT) {
        return -1;
      }
      return switch (fields[index]) {
        case FIELD_LONG -> at + 1 + Long.BYTES <= end
            ? at + 1 + Long.BYTES
            : -1;
        case FIELD_BOOLEAN -> at + 2 <= end && (key[at + 1] == 0 || key[at + 1] == 1)
            ? at + 2
            : -1;
        default -> stringFieldEnd(key, at, end);
      };
    }

    /** End of the first {@code count} fields, or -1 when they are not all well formed. */
    int prefixEnd(final byte[] key, final int length, final int count) {
      if (count < 0 || count > fields.length) {
        throw new IllegalArgumentException("sorted key prefix field count is outside the layout");
      }
      int at = 0;
      for (int field = 0; field < count; field++) {
        at = fieldEnd(key, at, length, field);
        if (at < 0) {
          return -1;
        }
      }
      return at;
    }

    /**
     * Offset of the last field of a well-formed row key (every field, then the eight-byte record key),
     * or -1. Unencodable keys are never well formed.
     */
    int lastFieldOffset(final byte[] key, final int length) {
      final int fieldsEnd = length - Long.BYTES;
      if (fieldsEnd < fields.length) {
        return -1;
      }
      int at = 0;
      for (int field = 0; field < fields.length - 1; field++) {
        at = fieldEnd(key, at, fieldsEnd, field);
        if (at < 0) {
          return -1;
        }
      }
      return fieldEnd(key, at, fieldsEnd, fields.length - 1) == fieldsEnd
          ? at
          : -1;
    }

    /** Whether {@code key[0, length)} is exactly a group: every field but the last. */
    boolean isGroup(final byte[] key, final int length) {
      return prefixEnd(key, length, fields.length - 1) == length;
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof Layout layout && Arrays.equals(fields, layout.fields);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(fields);
    }
  }

  /**
   * End of an escaped, zero-zero terminated present string field starting at {@code at}; -1 if
   * malformed.
   */
  static int stringFieldEnd(final byte[] key, final int at, final int end) {
    for (int i = at + 1; i + 1 < end; i++) {
      if (key[i] == 0) {
        if (key[i + 1] == 0) {
          return i + 2;
        }
        if ((key[i + 1] & 0xFF) != 0xFF) {
          return -1;
        }
        i++;
      }
    }
    return -1;
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
      bytes[length++] = value
          ? (byte) 1
          : (byte) 0;
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
      if ((long) valueLength + zeroCount + 3L > MAX_BUFFER_BYTES - length) {
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

    /** Replace the current key by the reserved key of a row whose fields cannot be represented. */
    void writeUnencodable(final long recordKey) {
      length = 0;
      ensureCapacity(1);
      bytes[length++] = UNENCODABLE;
      appendRecordKey(recordKey);
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
      if (additional < 0 || additional > MAX_BUFFER_BYTES - length) {
        throw new IllegalArgumentException("projection sort key is too large");
      }
      final int required = length + additional;
      if (required <= bytes.length) {
        return;
      }
      final int doubled = bytes.length <= MAX_BUFFER_BYTES / 2
          ? bytes.length << 1
          : MAX_BUFFER_BYTES;
      bytes = Arrays.copyOf(bytes, Math.max(required, doubled));
    }
  }
}
