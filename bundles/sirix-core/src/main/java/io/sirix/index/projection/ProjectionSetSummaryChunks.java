/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import it.unimi.dsi.fastutil.objects.Object2LongLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

final class ProjectionSetSummaryChunks {

  private static final long SLOT_BASE = 1L << 44;
  private static final int MAGIC = 0x43534950;
  private static final byte VERSION = 0;
  private static final int MAX_VALUES =
      Math.min(0xFFFF, Math.max(0, Integer.getInteger("sirix.projection.metadataSetCountsValues", 256)));
  private static final int MAX_BYTES = Math.max(7, Integer.getInteger("sirix.projection.metadataSetCountsBytes", 1024));

  private ProjectionSetSummaryChunks() {}

  static int maxValuesForTesting() {
    return MAX_VALUES;
  }

  static Accessor open(final ProjectionIndexHOTStorage storage,
      final @Nullable Map<Integer, Map<String, Long>> capabilities) {
    return new Accessor(storage, capabilities);
  }

  /**
   * Owner-confined full-build collector whose retained state is bounded by the persisted summary
   * capability itself. A set column is useful only while its complete distinct-value map fits in one
   * summary chunk. The first new value that would exceed either wire bound permanently disables that
   * column, drops its retained keys immediately, and makes all later leaves a no-op for that column.
   * Counts use primitive storage, so repeated values do not allocate boxed {@link Long}s.
   */
  static final class BuildAccumulator {
    private Object2LongLinkedOpenHashMap<String> @Nullable [] valuesByColumn;
    private boolean @Nullable [] disabledColumns;
    private int @Nullable [] encodedBytesByColumn;
    private int @Nullable [] peakRetainedValuesByColumn;

    /** Fold one borrowed leaf into the bounded resource-wide summaries. */
    void append(final ProjectionIndexRowGroupPage leaf) {
      if (leaf == null) {
        throw new NullPointerException("projection leaf is required");
      }
      ensureShape(leaf.getColumnCount());
      if (leaf.getRowCount() == 0) {
        return;
      }
      final boolean[] disabled = disabledColumns;
      final int[] encodedBytes = encodedBytesByColumn;
      if (disabled == null || encodedBytes == null) {
        throw new IllegalStateException("set-summary accumulator shape was not initialized");
      }
      for (int column = 0; column < leaf.getColumnCount(); column++) {
        if (leaf.columnKind(column) != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET || disabled[column]) {
          continue;
        }
        final int dictionarySize = leaf.stringDictionarySize(column);
        final long[] rowCounts = ProjectionIndexColumnSegmentCodec.valueRowCounts(dictionarySize,
            leaf.stringSetCountColumn(column), leaf.stringSetIdColumn(column), leaf.getRowCount());
        if (rowCounts == null) {
          continue;
        }
        Object2LongLinkedOpenHashMap<String> values = values(column);
        for (int dictionaryId = 0; dictionaryId < rowCounts.length; dictionaryId++) {
          final long rows = rowCounts[dictionaryId];
          if (rows == 0) {
            continue;
          }
          final int utf8Length = leaf.stringDictionaryEntryLength(column, dictionaryId);
          if (utf8Length > 0xFFFF) {
            disable(column, values);
            break;
          }
          final String value = new String(leaf.stringDictionaryEntryBacking(column, dictionaryId),
              leaf.stringDictionaryEntryOffset(column, dictionaryId), utf8Length, StandardCharsets.UTF_8);
          if (values.containsKey(value)) {
            final long updated;
            try {
              updated = Math.addExact(values.getLong(value), rows);
            } catch (final ArithmeticException overflow) {
              throw new IllegalStateException("projection set summary overflow for column " + column, overflow);
            }
            values.put(value, updated);
            continue;
          }
          final long nextEncodedBytes = (long) encodedBytes[column] + Short.BYTES + utf8Length + Long.BYTES;
          if (values.size() >= MAX_VALUES || nextEncodedBytes > MAX_BYTES) {
            disable(column, values);
            break;
          }
          values.put(value, rows);
          encodedBytes[column] = (int) nextEncodedBytes;
          final int[] peaks = peakRetainedValuesByColumn;
          if (peaks != null) {
            peaks[column] = Math.max(peaks[column], values.size());
          }
        }
      }
    }

    /**
     * Persist every still-complete set summary and return metadata capability markers. Disabled columns
     * are tombstoned and deliberately omitted; an untouched set column publishes an explicit empty
     * summary so later incremental inserts can revive it.
     */
    Map<Integer, Map<String, Long>> writeAll(final ProjectionIndexHOTStorage storage, final byte[] columnKinds) {
      if (storage == null || columnKinds == null) {
        throw new NullPointerException("storage and column kinds are required");
      }
      ensureShape(columnKinds.length);
      final boolean[] disabled = disabledColumns;
      if (disabled == null) {
        throw new IllegalStateException("set-summary accumulator shape was not initialized");
      }
      final Map<Integer, Map<String, Long>> capabilities = new LinkedHashMap<>();
      for (int column = 0; column < columnKinds.length; column++) {
        if (columnKinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          continue;
        }
        if (disabled[column]) {
          storage.tombstoneBlob(slotKey(column));
          continue;
        }
        final Object2LongLinkedOpenHashMap<String> values = existingValues(column);
        final byte[] encoded = encode(values);
        if (encoded == null) {
          throw new IllegalStateException(
              "bounded set-summary accumulator exceeded its wire limits at column " + column);
        }
        storage.putBlob(slotKey(column), encoded);
        // Metadata V0 serializes only the column key. Keep no boxed copy of the already-persisted
        // counts merely to publish that capability marker.
        capabilities.put(column, Map.of());
      }
      return capabilities;
    }

    /** Drop every retained bounded key after successful publication or abort. */
    void release() {
      final Object2LongLinkedOpenHashMap<String>[] values = valuesByColumn;
      if (values != null) {
        for (final Object2LongLinkedOpenHashMap<String> columnValues : values) {
          if (columnValues != null) {
            columnValues.clear();
          }
        }
      }
      valuesByColumn = null;
      disabledColumns = null;
      encodedBytesByColumn = null;
      peakRetainedValuesByColumn = null;
    }

    /** Current retained distinct values for one column; package-visible test/diagnostic telemetry. */
    int retainedValueCount(final int column) {
      final Object2LongLinkedOpenHashMap<String> values = existingValues(column);
      return values == null
          ? 0
          : values.size();
    }

    /** Peak retained distinct values for one column; package-visible test/diagnostic telemetry. */
    int peakRetainedValueCount(final int column) {
      final int[] peaks = peakRetainedValuesByColumn;
      checkColumn(column, peaks == null
          ? -1
          : peaks.length);
      return peaks[column];
    }

    /** Whether the complete summary for one column was proven too large to publish. */
    boolean disabled(final int column) {
      final boolean[] disabled = disabledColumns;
      checkColumn(column, disabled == null
          ? -1
          : disabled.length);
      return disabled[column];
    }

    private void ensureShape(final int columnCount) {
      if (columnCount < 0 || columnCount > RowGroupDescriptor.MAX_COLUMNS) {
        throw new IllegalArgumentException("set-summary column count out of range: " + columnCount);
      }
      final Object2LongLinkedOpenHashMap<String>[] existing = valuesByColumn;
      if (existing != null) {
        if (existing.length != columnCount) {
          throw new IllegalStateException(
              "Projection column count changed during summary collection: " + existing.length + " -> " + columnCount);
        }
        return;
      }
      @SuppressWarnings("unchecked")
      final Object2LongLinkedOpenHashMap<String>[] created =
          (Object2LongLinkedOpenHashMap<String>[]) new Object2LongLinkedOpenHashMap<?>[columnCount];
      valuesByColumn = created;
      disabledColumns = new boolean[columnCount];
      encodedBytesByColumn = new int[columnCount];
      peakRetainedValuesByColumn = new int[columnCount];
    }

    private Object2LongLinkedOpenHashMap<String> values(final int column) {
      final Object2LongLinkedOpenHashMap<String>[] values = valuesByColumn;
      if (values == null) {
        throw new IllegalStateException("set-summary accumulator shape was not initialized");
      }
      Object2LongLinkedOpenHashMap<String> columnValues = values[column];
      if (columnValues == null) {
        columnValues = new Object2LongLinkedOpenHashMap<>(Math.min(MAX_VALUES, 64));
        values[column] = columnValues;
        final int[] encodedBytes = encodedBytesByColumn;
        if (encodedBytes != null) {
          encodedBytes[column] = 7;
        }
      }
      return columnValues;
    }

    private @Nullable Object2LongLinkedOpenHashMap<String> existingValues(final int column) {
      final Object2LongLinkedOpenHashMap<String>[] values = valuesByColumn;
      checkColumn(column, values == null
          ? -1
          : values.length);
      return values[column];
    }

    private void disable(final int column, final Object2LongLinkedOpenHashMap<String> values) {
      values.clear();
      final Object2LongLinkedOpenHashMap<String>[] byColumn = valuesByColumn;
      final boolean[] disabled = disabledColumns;
      final int[] encodedBytes = encodedBytesByColumn;
      if (byColumn == null || disabled == null || encodedBytes == null) {
        throw new IllegalStateException("set-summary accumulator shape was not initialized");
      }
      byColumn[column] = null;
      disabled[column] = true;
      encodedBytes[column] = 0;
    }
  }

  static @Nullable Map<Integer, Map<String, Long>> readAll(final ProjectionIndexHOTStorage storage,
      final @Nullable Map<Integer, Map<String, Long>> capabilities) {
    if (capabilities == null) {
      return null;
    }
    final Map<Integer, Map<String, Long>> summaries = new LinkedHashMap<>(capabilities.size());
    for (final int column : capabilities.keySet()) {
      final Map<String, Long> values = decode(storage.getBlob(slotKey(column)));
      if (values == null) {
        throw new IllegalStateException("missing set-summary chunk for column " + column);
      }
      summaries.put(column, values);
    }
    return summaries;
  }

  static @Nullable Map<Integer, Map<String, Long>> readAll(final StorageEngineReader reader, final int indexNumber,
      final @Nullable Map<Integer, Map<String, Long>> capabilities) {
    if (capabilities == null) {
      return null;
    }
    final Map<Integer, Map<String, Long>> summaries = new LinkedHashMap<>(capabilities.size());
    for (final int column : capabilities.keySet()) {
      final Map<String, Long> values = decode(ProjectionIndexHOTStorage.readBlob(reader, indexNumber, slotKey(column)));
      if (values == null) {
        throw new IllegalStateException("missing set-summary chunk for column " + column);
      }
      summaries.put(column, values);
    }
    return summaries;
  }

  static long slotKey(final int column) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("set-summary column out of range: " + column);
    }
    return SLOT_BASE + column;
  }

  static final class Accessor {
    private final ProjectionIndexHOTStorage storage;
    private final Set<Integer> capabilities;
    private final Map<Integer, Map<String, Long>> loaded = new LinkedHashMap<>();
    private final Set<Integer> changed = new LinkedHashSet<>();
    private int chunksRead;
    private int chunksWritten;
    private long bytesRead;
    private long bytesWritten;

    private Accessor(final ProjectionIndexHOTStorage storage,
        final @Nullable Map<Integer, Map<String, Long>> capabilities) {
      if (storage == null) {
        throw new NullPointerException("storage is required");
      }
      this.storage = storage;
      this.capabilities = new LinkedHashSet<>();
      if (capabilities != null) {
        for (final int column : capabilities.keySet()) {
          slotKey(column);
          this.capabilities.add(column);
        }
      }
    }

    void adjust(final int column, final Map<String, Long> deltas, final long sign) {
      if (sign != -1L && sign != 1L) {
        throw new IllegalArgumentException("set-summary adjustment sign must be -1 or 1");
      }
      if (deltas == null) {
        throw new NullPointerException("set-summary deltas are required");
      }
      if (!capabilities.contains(column) || deltas.isEmpty()) {
        return;
      }
      final Map<String, Long> target = values(column);
      for (final Map.Entry<String, Long> delta : deltas.entrySet()) {
        if (delta.getValue() < 0) {
          throw new IllegalArgumentException("set-summary deltas must not be negative");
        }
        final long adjusted;
        try {
          adjusted = Math.addExact(target.getOrDefault(delta.getKey(), 0L), Math.multiplyExact(sign, delta.getValue()));
        } catch (final ArithmeticException overflow) {
          throw new IllegalStateException("projection set summary overflow for column " + column, overflow);
        }
        if (adjusted < 0) {
          throw new IllegalStateException("projection set summary underflow for column " + column);
        }
        if (adjusted == 0) {
          target.remove(delta.getKey());
        } else {
          target.put(delta.getKey(), adjusted);
        }
      }
      changed.add(column);
    }

    Map<Integer, Map<String, Long>> flush(final byte[] columnKinds) {
      if (columnKinds == null) {
        throw new NullPointerException("column kinds are required");
      }
      final Map<Integer, Map<String, Long>> persisted = new LinkedHashMap<>(capabilities.size());
      for (final int column : capabilities) {
        if (column >= columnKinds.length || columnKinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          throw new IllegalStateException("set-summary capability names non-set column " + column);
        }
        persisted.put(column, new LinkedHashMap<>());
      }
      for (final int column : changed) {
        final byte[] encoded = encode(loaded.get(column));
        if (encoded == null) {
          storage.tombstoneBlob(slotKey(column));
          persisted.remove(column);
        } else {
          storage.putBlob(slotKey(column), encoded);
          chunksWritten++;
          bytesWritten += encoded.length;
        }
      }
      return persisted;
    }

    int chunksRead() {
      return chunksRead;
    }

    int chunksWritten() {
      return chunksWritten;
    }

    long bytesRead() {
      return bytesRead;
    }

    long bytesWritten() {
      return bytesWritten;
    }

    private Map<String, Long> values(final int column) {
      final Map<String, Long> existing = loaded.get(column);
      if (existing != null) {
        return existing;
      }
      final byte[] bytes = storage.getBlob(slotKey(column));
      if (bytes == null) {
        throw new IllegalStateException("missing set-summary chunk for column " + column);
      }
      final Map<String, Long> decoded = decode(bytes);
      if (decoded == null) {
        throw new IllegalStateException("missing set-summary chunk for column " + column);
      }
      loaded.put(column, decoded);
      chunksRead++;
      bytesRead += bytes.length;
      return decoded;
    }
  }

  private static byte @Nullable [] encode(final Map<String, Long> values) {
    if (values == null || values.size() > MAX_VALUES) {
      return null;
    }
    final ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(MAX_BYTES, 256));
    putInt(out, MAGIC);
    out.write(VERSION);
    putShort(out, values.size());
    for (final Map.Entry<String, Long> entry : values.entrySet()) {
      final byte[] value = entry.getKey().getBytes(StandardCharsets.UTF_8);
      if (value.length > 0xFFFF || entry.getValue() < 0) {
        return null;
      }
      putShort(out, value.length);
      out.write(value, 0, value.length);
      putLong(out, entry.getValue());
      if (out.size() > MAX_BYTES) {
        return null;
      }
    }
    return out.toByteArray();
  }

  /** Primitive-count build form; insertion order remains the deterministic wire order. */
  private static byte @Nullable [] encode(final @Nullable Object2LongLinkedOpenHashMap<String> values) {
    final int size = values == null
        ? 0
        : values.size();
    if (size > MAX_VALUES) {
      return null;
    }
    final ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(MAX_BYTES, 256));
    putInt(out, MAGIC);
    out.write(VERSION);
    putShort(out, size);
    if (values != null) {
      for (final Object2LongMap.Entry<String> entry : values.object2LongEntrySet()) {
        final byte[] value = entry.getKey().getBytes(StandardCharsets.UTF_8);
        final long rows = entry.getLongValue();
        if (value.length > 0xFFFF || rows < 0) {
          return null;
        }
        putShort(out, value.length);
        out.write(value, 0, value.length);
        putLong(out, rows);
        if (out.size() > MAX_BYTES) {
          return null;
        }
      }
    }
    return out.toByteArray();
  }

  private static void checkColumn(final int column, final int columnCount) {
    if (column < 0 || column >= columnCount) {
      throw new IndexOutOfBoundsException(
          "set-summary column " + column + " outside [0, " + Math.max(columnCount, 0) + ")");
    }
  }

  private static @Nullable Map<String, Long> decode(final byte @Nullable [] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      if (bytes.length < 7 || getInt(bytes, 0) != MAGIC || bytes[4] != VERSION) {
        throw new IllegalStateException("malformed set-summary chunk");
      }
      int offset = 5;
      final int count = getShort(bytes, offset);
      offset += 2;
      if (count > MAX_VALUES) {
        throw new IllegalStateException("implausible set-summary value count " + count);
      }
      final Map<String, Long> values = new LinkedHashMap<>(Math.max(4, count * 2));
      for (int i = 0; i < count; i++) {
        final int length = getShort(bytes, offset);
        offset += 2;
        final String value = new String(bytes, offset, length, StandardCharsets.UTF_8);
        offset += length;
        final long rows = getLong(bytes, offset);
        offset += Long.BYTES;
        if (rows < 0 || values.put(value, rows) != null) {
          throw new IllegalStateException("invalid set-summary value entry");
        }
      }
      if (offset != bytes.length) {
        throw new IllegalStateException("trailing set-summary bytes");
      }
      return values;
    } catch (final IndexOutOfBoundsException truncated) {
      throw new IllegalStateException("truncated set-summary chunk", truncated);
    }
  }

  private static void putShort(final ByteArrayOutputStream out, final int value) {
    out.write(value);
    out.write(value >>> 8);
  }

  private static int getShort(final byte[] bytes, final int offset) {
    return (bytes[offset] & 0xFF) | (bytes[offset + 1] & 0xFF) << 8;
  }

  private static void putInt(final ByteArrayOutputStream out, final int value) {
    for (int shift = 0; shift < Integer.SIZE; shift += Byte.SIZE) {
      out.write(value >>> shift);
    }
  }

  private static int getInt(final byte[] bytes, final int offset) {
    return (bytes[offset] & 0xFF) | (bytes[offset + 1] & 0xFF) << 8 | (bytes[offset + 2] & 0xFF) << 16
        | (bytes[offset + 3] & 0xFF) << 24;
  }

  private static void putLong(final ByteArrayOutputStream out, final long value) {
    for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
      out.write((int) (value >>> shift));
    }
  }

  private static long getLong(final byte[] bytes, final int offset) {
    long value = 0L;
    for (int i = 0; i < Long.BYTES; i++) {
      value |= (long) (bytes[offset + i] & 0xFF) << (i * Byte.SIZE);
    }
    return value;
  }
}
