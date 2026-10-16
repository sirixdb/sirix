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
      if (column < 0 || column >= kinds.length) {
        throw new IllegalArgumentException("Invalid group column");
      }
      if (!row.supported(column) || offset == ChangeLogStorage.CHUNK_BYTES) {
        return null;
      }
      if (!row.present(column)) {
        bytes = capacity(bytes, offset + 1);
        bytes[offset++] = 0;
        continue;
      }
      if (kinds[column] == ChangeLogSlab.STRING) {
        final String value = (String) row.text[column];
        if (value.length() > ChangeLogStorage.CHUNK_BYTES) {
          return null;
        }
        final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        final long end = (long) offset + 5 + utf8.length;
        if (end > ChangeLogStorage.CHUNK_BYTES) {
          return null;
        }
        bytes = capacity(bytes, (int) end);
        bytes[offset++] = 2;
        for (int shift = 24; shift >= 0; shift -= 8) {
          bytes[offset++] = (byte) (utf8.length >>> shift);
        }
        System.arraycopy(utf8, 0, bytes, offset, utf8.length);
        offset += utf8.length;
      } else if (kinds[column] == ChangeLogSlab.LONG) {
        if (offset > ChangeLogStorage.CHUNK_BYTES - 9) {
          return null;
        }
        bytes = capacity(bytes, offset + 9);
        bytes[offset++] = 1;
        final long value = row.numbers[column];
        for (int shift = 56; shift >= 0; shift -= 8) {
          bytes[offset++] = (byte) (value >>> shift);
        }
      } else if (kinds[column] == ChangeLogSlab.BOOLEAN) {
        if (offset > ChangeLogStorage.CHUNK_BYTES - 2) {
          return null;
        }
        bytes = capacity(bytes, offset + 2);
        bytes[offset++] = 3;
        bytes[offset++] = (byte) (row.numbers[column] == 0
            ? 0
            : 1);
      } else {
        return null;
      }
    }
    return offset == bytes.length
        ? bytes
        : Arrays.copyOf(bytes, offset);
  }

  private static byte[] capacity(final byte[] bytes, final int required) {
    return required <= bytes.length
        ? bytes
        : Arrays.copyOf(bytes, Math.min(ChangeLogStorage.CHUNK_BYTES, Math.max(required, bytes.length * 2)));
  }
}
