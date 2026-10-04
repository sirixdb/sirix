/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.jspecify.annotations.Nullable;

/**
 * Open-row-group row tail.
 *
 * <p>
 * While a row group is open (fewer than {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows) and
 * maintenance only APPENDS rows to it, the appended rows are not folded into its column segments on
 * every commit. They are stored row-major in this namespace — one blob per maintenance commit
 * holding that commit's rows — and the descriptor slot is rewritten as the
 * {@link RowGroupDescriptor#VERSION_TAILED tailed} descriptor of the MERGED row group (base rows
 * plus tail rows: row count, fences, zone maps and every merged segment's byte length and content
 * hash). The persisted segment slots keep the base rows; their own descriptor is retained in the
 * tail header. When the live reference limit is reached, the row group completes or a non-append
 * edit touches it, the merged segments are written, the descriptor returns to
 * {@link RowGroupDescriptor#VERSION} and the tail slots are tombstoned, all in the same
 * transaction.
 *
 * <p>
 * <b>Readers see identical row groups at every revision.</b> Every reader that consumes a row
 * group's segment bytes obtains, for a tailed descriptor, the MERGED segments from
 * {@link #materialize}: the base segments are hydrated, the tail rows are re-appended through the
 * one page method the writer used ({@link ProjectionIndexRowGroupPage#appendTailRow}), and the page
 * is re-encoded. The result is checked against the published descriptor byte for byte, so a merge
 * that disagrees with what the writer published is a loud corruption rather than a silently
 * different row group. Merged results are memoized per (database, resource, index, row group,
 * descriptor hash).
 *
 * <p>
 * <b>Format.</b> Slot namespace {@link #SLOT_BASE} = 2^46 + 2^45 (below the 2^47 side-page owner
 * limit, above every sorted-view sub-namespace). {@link #headerSlot}: {@code PIXH} magic, version,
 * base-descriptor length + bytes, tail blob count, tail row count. {@link #rowsSlot}: {@code PIXT}
 * magic, version, row count, column count, column kinds, then per row: record key, order-exception
 * flag, order label, and per column a flag byte (present, unrepresentable, non-integral,
 * non-double-source) followed by the lane value (8-byte value or resolved STRING_GLOBAL id, boolean
 * byte, UTF-8 string, or UTF-8 string set). Only the row-group-major slot layout carries tails.
 */
final class ProjectionOpenRowGroupTail {
  /** First slot of the tail namespace: above every sorted-view sub-namespace, below 2^47. */
  static final long SLOT_BASE = (1L << 46) + (1L << 45);

  static final int HEADER_MAGIC = 0x48584950; // "PIXH" little-endian
  static final int ROWS_MAGIC = 0x54584950; // "PIXT" little-endian
  static final byte VERSION = 1;
  /** Tail blobs per row group: one per maintenance commit, at most one per row. */
  static final int MAX_TAIL_BLOBS = ProjectionIndexRowGroupPage.MAX_ROWS;
  static final int MAX_ROW_ORDER_LABEL_BYTES = 0xFFFF;

  private static final int FLAG_PRESENT = 1;
  private static final int FLAG_UNREPRESENTABLE = 2;
  private static final int FLAG_NON_INTEGRAL = 4;
  private static final int FLAG_NON_DOUBLE_SOURCE = 8;
  private static final int FLAG_ORDER_EXCEPTION = 1;
  private static final long MAX_CACHED_BYTES = 64L << 20;
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final String[] EMPTY_SET = new String[0];

  private ProjectionOpenRowGroupTail() {}

  /** The tail header slot of {@code rowGroupId}. */
  static long headerSlot(final long rowGroupId) {
    if (rowGroupId < 1 || rowGroupId > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("rowGroupId out of range: " + rowGroupId);
    }
    return SLOT_BASE + (rowGroupId << 16);
  }

  /** The {@code seq}-th (1-based) tail rows blob slot of {@code rowGroupId}. */
  static long rowsSlot(final long rowGroupId, final int seq) {
    if (seq < 1 || seq > MAX_TAIL_BLOBS) {
      throw new IllegalArgumentException("tail sequence out of range: " + seq);
    }
    return headerSlot(rowGroupId) + seq;
  }

  /** Whether {@code slotKey} lies in the tail namespace. */
  static boolean isTailSlot(final long slotKey) {
    return slotKey >= SLOT_BASE && slotKey < SLOT_BASE + (((long) ProjectionIndexHOTStorage.MAX_ROW_GROUPS + 1) << 16);
  }

  /** The tail header: the base descriptor of the persisted segments and the tail's extent. */
  @SuppressWarnings("ArrayRecordComponent") // Descriptor content is compared by RowGroupDescriptor, not record
                                            // equality.
  record Header(byte[] baseDescriptor, int blobCount, int rowCount) {
    Header {
      Objects.requireNonNull(baseDescriptor, "baseDescriptor");
      RowGroupDescriptor.validate(baseDescriptor);
      if (RowGroupDescriptor.isTailed(baseDescriptor)) {
        throw new IllegalArgumentException("a tail header carries the base (untailed) descriptor");
      }
      if (blobCount < 0 || blobCount > MAX_TAIL_BLOBS || rowCount < blobCount || (blobCount == 0 && rowCount != 0)
          || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS - RowGroupDescriptor.rowCount(baseDescriptor)) {
        throw new IllegalArgumentException("tail extent out of range: blobs=" + blobCount + " rows=" + rowCount);
      }
    }

    byte[] encode() {
      final byte[] out = new byte[4 + 1 + 4 + baseDescriptor.length + 4 + 4];
      RowGroupDescriptor.putIntLE(out, 0, HEADER_MAGIC);
      out[4] = VERSION;
      RowGroupDescriptor.putIntLE(out, 5, baseDescriptor.length);
      System.arraycopy(baseDescriptor, 0, out, 9, baseDescriptor.length);
      RowGroupDescriptor.putIntLE(out, 9 + baseDescriptor.length, blobCount);
      RowGroupDescriptor.putIntLE(out, 13 + baseDescriptor.length, rowCount);
      return out;
    }

    static Header decode(final byte @Nullable [] bytes, final long rowGroupId) {
      if (bytes == null) {
        throw new IllegalStateException("projection row group " + rowGroupId + " is tailed but has no tail header");
      }
      if (bytes.length < 17 || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != HEADER_MAGIC || bytes[4] != VERSION) {
        throw new IllegalStateException("projection row group " + rowGroupId + " has a malformed tail header");
      }
      final int length = ProjectionIndexRowGroupCodec.getIntLE(bytes, 5);
      if (length < 0 || length != bytes.length - 17) {
        throw new IllegalStateException("projection row group " + rowGroupId + " has a truncated tail header");
      }
      final byte[] base = Arrays.copyOfRange(bytes, 9, 9 + length);
      return new Header(base, ProjectionIndexRowGroupCodec.getIntLE(bytes, 9 + length),
          ProjectionIndexRowGroupCodec.getIntLE(bytes, 13 + length));
    }
  }

  /**
   * One appended row: the writer's inputs to {@link ProjectionIndexRowGroupPage#appendTailRow} with
   * every STRING_GLOBAL id already resolved in {@code longs}.
   */
  @SuppressWarnings("ArrayRecordComponent") // Primitive lanes are replayed directly; record equality is not used.
  record Row(long recordKey, boolean orderException, byte[] orderLabel, long[] longs, boolean[] bools, byte[][] strings,
      String[][] sets, boolean[] present, boolean[] unrepresentable, boolean[] nonIntegral, boolean[] nonDoubleSource) {
    Row {
      Objects.requireNonNull(orderLabel, "orderLabel");
      Objects.requireNonNull(longs, "longs");
      Objects.requireNonNull(bools, "bools");
      Objects.requireNonNull(strings, "strings");
      Objects.requireNonNull(sets, "sets");
      Objects.requireNonNull(present, "present");
      Objects.requireNonNull(unrepresentable, "unrepresentable");
      Objects.requireNonNull(nonIntegral, "nonIntegral");
      Objects.requireNonNull(nonDoubleSource, "nonDoubleSource");
      if (orderLabel.length == 0 || orderLabel.length > MAX_ROW_ORDER_LABEL_BYTES) {
        throw new IllegalArgumentException("tail order labels require 1..65535 bytes");
      }
      final int columns = longs.length;
      if (bools.length != columns || strings.length != columns || sets.length != columns || present.length != columns
          || unrepresentable.length != columns || nonIntegral.length != columns || nonDoubleSource.length != columns) {
        throw new IllegalArgumentException("tail row lanes disagree on the column count");
      }
    }
  }

  private static boolean isStringDictKind(final byte kind) {
    return kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  }

  /** Encode a maintenance commit's appended rows as one tail blob. */
  static byte[] encodeRows(final byte[] kinds, final List<Row> rows) {
    Objects.requireNonNull(kinds, "kinds");
    if (rows == null || rows.isEmpty() || rows.size() > ProjectionIndexRowGroupPage.MAX_ROWS) {
      throw new IllegalArgumentException("a tail blob holds between one and MAX_ROWS rows");
    }
    final byte[] out = new byte[encodedRowsSize(kinds, rows)];
    int pos = 0;
    RowGroupDescriptor.putIntLE(out, pos, ROWS_MAGIC);
    pos += 4;
    out[pos++] = VERSION;
    RowGroupDescriptor.putIntLE(out, pos, rows.size());
    pos += 4;
    out[pos++] = (byte) kinds.length;
    out[pos++] = (byte) (kinds.length >>> 8);
    System.arraycopy(kinds, 0, out, pos, kinds.length);
    pos += kinds.length;
    for (final Row row : rows) {
      RowGroupDescriptor.putLongLE(out, pos, row.recordKey());
      pos += 8;
      out[pos++] = (byte) (row.orderException()
          ? FLAG_ORDER_EXCEPTION
          : 0);
      final byte[] label = row.orderLabel();
      out[pos++] = (byte) label.length;
      out[pos++] = (byte) (label.length >>> 8);
      System.arraycopy(label, 0, out, pos, label.length);
      pos += label.length;
      pos = encodeColumnLanes(out, kinds, pos, row);
    }
    if (pos != out.length) {
      throw new IllegalStateException("tail blob size mismatch: " + pos + " != " + out.length);
    }
    return out;
  }

  private static int encodedRowsSize(final byte[] kinds, final List<Row> rows) {
    long size = 4L + 1 + 4 + 2 + kinds.length;
    for (final Row row : rows) {
      if (row.longs().length != kinds.length) {
        throw new IllegalArgumentException("tail row column count disagrees with the projection");
      }
      size += 8 + 1 + 2 + row.orderLabel().length;
      for (int c = 0; c < kinds.length; c++) {
        size += 1;
        final byte kind = kinds[c];
        if (isStringDictKind(kind)) {
          final byte[] value = row.strings()[c];
          size += 4L + (value == null
              ? 0
              : value.length);
        } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          final String[] set = row.sets()[c];
          size += 4;
          if (set != null) {
            for (final String element : set) {
              size += 4L + element.getBytes(StandardCharsets.UTF_8).length;
            }
          }
        } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
          size += 1;
        } else {
          size += 8;
        }
      }
    }
    if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("tail rows blob exceeds the byte-array size limit");
    }
    return (int) size;
  }

  private static int encodeColumnLanes(final byte[] out, final byte[] kinds, int pos, final Row row) {
    for (int c = 0; c < kinds.length; c++) {
      final byte kind = kinds[c];
      out[pos++] = (byte) ((row.present()[c]
          ? FLAG_PRESENT
          : 0)
          | (row.unrepresentable()[c]
              ? FLAG_UNREPRESENTABLE
              : 0)
          | (row.nonIntegral()[c]
              ? FLAG_NON_INTEGRAL
              : 0)
          | (row.nonDoubleSource()[c]
              ? FLAG_NON_DOUBLE_SOURCE
              : 0));
      if (isStringDictKind(kind)) {
        final byte[] value = row.strings()[c] == null
            ? EMPTY_BYTES
            : row.strings()[c];
        RowGroupDescriptor.putIntLE(out, pos, value.length);
        pos += 4;
        System.arraycopy(value, 0, out, pos, value.length);
        pos += value.length;
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        final String[] set = row.sets()[c] == null
            ? EMPTY_SET
            : row.sets()[c];
        RowGroupDescriptor.putIntLE(out, pos, set.length);
        pos += 4;
        for (final String element : set) {
          final byte[] bytes = element.getBytes(StandardCharsets.UTF_8);
          RowGroupDescriptor.putIntLE(out, pos, bytes.length);
          pos += 4;
          System.arraycopy(bytes, 0, out, pos, bytes.length);
          pos += bytes.length;
        }
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        out[pos++] = (byte) (row.bools()[c]
            ? 1
            : 0);
      } else {
        RowGroupDescriptor.putLongLE(out, pos, row.longs()[c]);
        pos += 8;
      }
    }
    return pos;
  }

  /** Check an append's row count and kinds without decoding or allocating row lanes. */
  static void validateAppendHeader(final byte[] blob, final byte[] descriptor, final int rows) {
    final int columns = RowGroupDescriptor.columnCount(descriptor);
    if (rows < 1 || rows > ProjectionIndexRowGroupPage.MAX_ROWS || blob == null || blob.length < 11 + columns
        || ProjectionIndexRowGroupCodec.getIntLE(blob, 0) != ROWS_MAGIC || blob[4] != VERSION
        || ProjectionIndexRowGroupCodec.getIntLE(blob, 5) != rows
        || ((blob[9] & 0xFF) | ((blob[10] & 0xFF) << 8)) != columns) {
      throw new IllegalArgumentException("tail rows header disagrees with the append");
    }
    for (int column = 0; column < columns; column++) {
      if (blob[11 + column] != RowGroupDescriptor.kind(descriptor, column)) {
        throw new IllegalArgumentException("tail rows kinds disagree with the merged descriptor");
      }
    }
  }

  /** Decode one tail blob; the column kinds must match the projection's persisted kinds. */
  static List<Row> decodeRows(final byte[] blob, final byte[] kinds, final long rowGroupId) {
    if (blob == null || blob.length < 11 || ProjectionIndexRowGroupCodec.getIntLE(blob, 0) != ROWS_MAGIC
        || blob[4] != VERSION) {
      throw new IllegalStateException("projection row group " + rowGroupId + " has a malformed tail rows blob");
    }
    int pos = 5;
    final int rowCount = ProjectionIndexRowGroupCodec.getIntLE(blob, pos);
    pos += 4;
    final int columns = (blob[pos] & 0xFF) | ((blob[pos + 1] & 0xFF) << 8);
    pos += 2;
    if (rowCount < 1 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS || columns != kinds.length
        || blob.length < pos + columns || !Arrays.equals(blob, pos, pos + columns, kinds, 0, columns)) {
      throw new IllegalStateException("projection row group " + rowGroupId + " tail rows disagree with its columns");
    }
    pos += columns;
    final List<Row> rows = new ArrayList<>(rowCount);
    for (int r = 0; r < rowCount; r++) {
      requireBytes(blob, pos, 11, rowGroupId);
      final long recordKey = ProjectionIndexRowGroupCodec.getLongLE(blob, pos);
      pos += 8;
      final int orderFlags = blob[pos++] & 0xFF;
      if ((orderFlags & ~FLAG_ORDER_EXCEPTION) != 0) {
        throw new IllegalStateException("invalid tail order flags at row group " + rowGroupId);
      }
      final boolean orderException = (orderFlags & FLAG_ORDER_EXCEPTION) != 0;
      final int labelLength = (blob[pos] & 0xFF) | ((blob[pos + 1] & 0xFF) << 8);
      pos += 2;
      requireBytes(blob, pos, labelLength, rowGroupId);
      if (labelLength == 0) {
        throw new IllegalStateException("empty tail order label at row group " + rowGroupId);
      }
      final byte[] label = Arrays.copyOfRange(blob, pos, pos + labelLength);
      pos += labelLength;
      final Row row = new Row(recordKey, orderException, label, new long[columns], new boolean[columns],
          new byte[columns][], new String[columns][], new boolean[columns], new boolean[columns], new boolean[columns],
          new boolean[columns]);
      pos = decodeColumnLanes(blob, kinds, pos, rowGroupId, row);
      rows.add(row);
    }
    if (pos != blob.length) {
      throw new IllegalStateException("projection row group " + rowGroupId + " tail rows blob has trailing bytes");
    }
    return rows;
  }

  private static int decodeColumnLanes(final byte[] blob, final byte[] kinds, int pos, final long rowGroupId,
      final Row row) {
    for (int c = 0; c < kinds.length; c++) {
      requireBytes(blob, pos, 1, rowGroupId);
      final int flags = blob[pos++] & 0xFF;
      if ((flags & ~15) != 0) {
        throw new IllegalStateException("invalid tail column flags at row group " + rowGroupId);
      }
      row.present()[c] = (flags & FLAG_PRESENT) != 0;
      row.unrepresentable()[c] = (flags & FLAG_UNREPRESENTABLE) != 0;
      row.nonIntegral()[c] = (flags & FLAG_NON_INTEGRAL) != 0;
      row.nonDoubleSource()[c] = (flags & FLAG_NON_DOUBLE_SOURCE) != 0;
      final byte kind = kinds[c];
      if (isStringDictKind(kind)) {
        requireBytes(blob, pos, 4, rowGroupId);
        final int length = ProjectionIndexRowGroupCodec.getIntLE(blob, pos);
        pos += 4;
        requireBytes(blob, pos, length, rowGroupId);
        row.strings()[c] = Arrays.copyOfRange(blob, pos, pos + length);
        pos += length;
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        requireBytes(blob, pos, 4, rowGroupId);
        final int count = ProjectionIndexRowGroupCodec.getIntLE(blob, pos);
        pos += 4;
        if (count < 0 || count > (blob.length - pos) / 4) {
          throw new IllegalStateException("invalid tail set size at row group " + rowGroupId);
        }
        final String[] set = new String[count];
        for (int i = 0; i < count; i++) {
          requireBytes(blob, pos, 4, rowGroupId);
          final int length = ProjectionIndexRowGroupCodec.getIntLE(blob, pos);
          pos += 4;
          requireBytes(blob, pos, length, rowGroupId);
          set[i] = new String(blob, pos, length, StandardCharsets.UTF_8);
          pos += length;
        }
        row.sets()[c] = set;
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        requireBytes(blob, pos, 1, rowGroupId);
        if (blob[pos] != 0 && blob[pos] != 1) {
          throw new IllegalStateException("invalid tail boolean at row group " + rowGroupId);
        }
        row.bools()[c] = blob[pos++] != 0;
      } else {
        requireBytes(blob, pos, 8, rowGroupId);
        row.longs()[c] = ProjectionIndexRowGroupCodec.getLongLE(blob, pos);
        pos += 8;
      }
    }
    return pos;
  }

  private static void requireBytes(final byte[] blob, final int offset, final int length, final long rowGroupId) {
    if (length < 0 || offset < 0 || length > blob.length - offset) {
      throw new IllegalStateException("truncated tail row at row group " + rowGroupId);
    }
  }

  /** Re-append {@code rows} to {@code page} exactly as the writer did. */
  static void appendRows(final ProjectionIndexRowGroupPage page, final List<Row> rows, final long rowGroupId) {
    for (final Row row : rows) {
      final int[] lengths = new int[row.strings().length];
      final byte[][] strings = new byte[row.strings().length][];
      for (int c = 0; c < strings.length; c++) {
        strings[c] = row.strings()[c] == null
            ? EMPTY_BYTES
            : row.strings()[c];
        lengths[c] = strings[c].length;
      }
      if (!page.appendTailRow(row.recordKey(), row.longs(), row.bools(), strings, lengths, row.sets(), row.present(),
          row.unrepresentable(), row.nonIntegral(), row.nonDoubleSource(), row.orderException(), row.orderLabel())) {
        throw new IllegalStateException(
            "projection row group " + rowGroupId + " cannot take back its own tail row " + row.recordKey());
      }
    }
  }

  /** The merged row group: its (untailed) encoding and its raw scan form. */
  @SuppressWarnings("ArrayRecordComponent") // Encoded bytes are served directly; record equality is not used.
  record Materialized(ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, byte[] raw) {
    byte @Nullable [] segment(final int columnSegmentId) {
      final int[] ids = encoded.columnSegmentIds();
      for (int i = 0; i < ids.length; i++) {
        if (ids[i] == columnSegmentId) {
          return encoded.segments()[i];
        }
      }
      return null;
    }
  }

  /**
   * Merge the base segments and the tail rows into the row group every reader sees, and prove that it
   * is the row group the writer published: the re-encoded descriptor must equal
   * {@code virtualDescriptor} apart from the version byte.
   */
  static Materialized materialize(final long rowGroupId, final byte[] virtualDescriptor, final Header header,
      final List<byte[]> rowBlobs, final ProjectionIndexColumnSegmentCodec.SegmentResolver baseResolver) {
    Objects.requireNonNull(virtualDescriptor, "virtualDescriptor");
    Objects.requireNonNull(header, "header");
    if (!RowGroupDescriptor.isTailed(virtualDescriptor)) {
      throw new IllegalArgumentException("projection row group " + rowGroupId + " is not tailed");
    }
    if (rowBlobs.size() != header.blobCount()) {
      throw new IllegalStateException("projection row group " + rowGroupId + " tail header declares "
          + header.blobCount() + " blobs but " + rowBlobs.size() + " were read");
    }
    final Runnable observer = mergeObserver;
    if (observer != null) {
      observer.run();
    }
    final byte[] baseRaw = ProjectionIndexColumnSegmentCodec.assembleRaw(header.baseDescriptor(), baseResolver);
    final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(baseRaw);
    final byte[] kinds = new byte[page.getColumnCount()];
    for (int c = 0; c < kinds.length; c++) {
      kinds[c] = page.columnKind(c);
    }
    int tailRows = 0;
    for (final byte[] blob : rowBlobs) {
      final List<Row> rows = decodeRows(blob, kinds, rowGroupId);
      appendRows(page, rows, rowGroupId);
      tailRows += rows.size();
    }
    if (tailRows != header.rowCount()) {
      throw new IllegalStateException("projection row group " + rowGroupId + " tail header declares "
          + header.rowCount() + " rows but its blobs hold " + tailRows);
    }
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encodePooled(page);
    if (!RowGroupDescriptor.sameContentIgnoringVersion(encoded.descriptor(), virtualDescriptor)) {
      throw new IllegalStateException("projection row group " + rowGroupId
          + ": the open-row-group tail merge disagrees with the published descriptor");
    }
    return new Materialized(encoded, page.serialize());
  }

  /** Memo key: one merge per (database, resource, index, row group, published descriptor). */
  record CacheKey(long databaseId, long resourceId, int indexNumber, long rowGroupId, long descriptorHash,
      int descriptorLength) {
  }

  private static final Cache<CacheKey, Materialized> MERGES =
      Caffeine.newBuilder().maximumWeight(MAX_CACHED_BYTES).weigher((CacheKey key, Materialized value) -> {
        long bytes = 128L + value.raw().length + value.encoded().descriptor().length;
        for (final byte[] segment : value.encoded().segments()) {
          bytes += 24L + segment.length;
        }
        return (int) Math.min(Integer.MAX_VALUE, bytes);
      }).build();

  static CacheKey cacheKey(final long databaseId, final long resourceId, final int indexNumber, final long rowGroupId,
      final byte[] virtualDescriptor) {
    return new CacheKey(databaseId, resourceId, indexNumber, rowGroupId,
        ProjectionIndexColumnSegmentCodec.contentHash(virtualDescriptor), virtualDescriptor.length);
  }

  /** Memoize merges by published descriptor; concurrent misses may compute independently. */
  static Materialized cached(final CacheKey key, final Supplier<Materialized> merge) {
    final Materialized hit = MERGES.getIfPresent(key);
    if (hit != null) {
      return hit;
    }
    final Materialized computed = merge.get();
    MERGES.put(key, computed);
    return computed;
  }

  /**
   * The writer seeds the memo with the state it just published: the merged encoding it computed for
   * the tailed descriptor and the merged page's raw form. The next commit's hydration and
   * verified-segment reads of that row group then hit the memo instead of replaying every tail blob
   * and re-encoding — the writer's own data needs no verification.
   */
  static void seed(final CacheKey key, final Materialized materialized) {
    Objects.requireNonNull(materialized, "materialized");
    MERGES.put(key, materialized);
  }

  private static volatile @Nullable Runnable mergeObserver;

  /** Observe cold merges in isolated tests; return the displaced observer so it can be restored. */
  static @Nullable Runnable observeMergesForTesting(final @Nullable Runnable observer) {
    final Runnable previous = mergeObserver;
    mergeObserver = observer;
    return previous;
  }

  /** Drop every memoized merge — for tests. */
  static void clearCacheForTesting() {
    MERGES.invalidateAll();
  }
}
