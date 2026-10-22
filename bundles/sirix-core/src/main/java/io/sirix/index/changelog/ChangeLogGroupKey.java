package io.sirix.index.changelog;

import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical typed group bytes shared by log addressing and aggregate maintenance. */
public final class ChangeLogGroupKey {
  private ChangeLogGroupKey() {}

  public static byte @Nullable [] encode(final ChangeLogSlab.Row row, final byte[] kinds, final List<Integer> columns) {
    Objects.requireNonNull(row);
    Objects.requireNonNull(kinds);
    Objects.requireNonNull(columns);
    if (row.flags.length != kinds.length) {
      throw new IllegalArgumentException("Group row and schema must have the same columns");
    }
    if (!row.object) {
      return null;
    }
    // Numeric groupings need only their returned key: no stream wrappers or scratch copy.
    byte[] bytes = new byte[(int) Math.min(ChangeLogStorage.CHUNK_BYTES, 1L + columns.size() * 9L)];
    bytes[0] = 1;
    int offset = 1;
    for (final int column : columns) {
      if (!supported(row, kinds, column, offset)) {
        return null;
      }
      if (!row.present(column)) {
        bytes = capacity(bytes, offset + 1);
        bytes[offset++] = 0;
        continue;
      }
      if (kinds[column] == ChangeLogSlab.STRING) {
        final byte[] utf8 = stringBytes((String) row.text[column], offset);
        if (utf8 == null) {
          return null;
        }
        bytes = capacity(bytes, offset + 5 + utf8.length);
        bytes[offset++] = 2;
        offset = number(bytes, offset, utf8.length, 24);
        System.arraycopy(utf8, 0, bytes, offset, utf8.length);
        offset += utf8.length;
        continue;
      }
      final int size = scalarSize(kinds[column], offset);
      if (size == 0) {
        return null;
      }
      bytes = capacity(bytes, offset + size);
      offset = scalar(bytes, offset, kinds[column], row.numbers[column]);
    }
    return offset == bytes.length
        ? bytes
        : Arrays.copyOf(bytes, offset);
  }

  private static boolean supported(final ChangeLogSlab.Row row, final byte[] kinds, final int column,
      final int offset) {
    if (column < 0 || column >= kinds.length) {
      throw new IllegalArgumentException("Invalid group column");
    }
    return row.supported(column) && offset != ChangeLogStorage.CHUNK_BYTES;
  }

  private static byte @Nullable [] stringBytes(final String value, final int offset) {
    if (value.length() > ChangeLogStorage.CHUNK_BYTES) {
      return null;
    }
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    return (long) offset + 5 + utf8.length > ChangeLogStorage.CHUNK_BYTES
        ? null
        : utf8;
  }

  private static int scalarSize(final byte kind, final int offset) {
    final int size = switch (kind) {
      case ChangeLogSlab.LONG -> 9;
      case ChangeLogSlab.BOOLEAN -> 2;
      default -> 0;
    };
    return offset > ChangeLogStorage.CHUNK_BYTES - size
        ? 0
        : size;
  }

  private static int scalar(final byte[] bytes, int offset, final byte kind, final long value) {
    if (kind == ChangeLogSlab.LONG) {
      bytes[offset++] = 1;
      return number(bytes, offset, value, 56);
    }
    bytes[offset++] = 3;
    bytes[offset++] = (byte) (value == 0
        ? 0
        : 1);
    return offset;
  }

  private static int number(final byte[] bytes, int offset, final long value, final int firstShift) {
    for (int shift = firstShift; shift >= 0; shift -= 8) {
      bytes[offset++] = (byte) (value >>> shift);
    }
    return offset;
  }

  private static byte[] capacity(final byte[] bytes, final int required) {
    return required <= bytes.length
        ? bytes
        : Arrays.copyOf(bytes, Math.min(ChangeLogStorage.CHUNK_BYTES, Math.max(required, bytes.length * 2)));
  }
}
