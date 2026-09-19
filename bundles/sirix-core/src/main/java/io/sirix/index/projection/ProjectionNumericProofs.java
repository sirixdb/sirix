/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.NumericBucketDecoder;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Optional revisioned evidence that an integral BODY has no missing cells. Each entry binds its
 * canonical extrema and shape to the BODY content hash. An ordinary column update needs no extra
 * write: a changed hash or row count makes the old entry unusable, preserving the exact BODY path.
 * These opaque slots do not change descriptors, segment ids, or old-reader compatibility.
 */
final class ProjectionNumericProofs {
  static final long SLOT_BASE = ProjectionSortedLeafBounds.SLOT_BASE + (1L << 32);
  static final long HEADER_SLOT = SLOT_BASE + (1L << 32) - 1;
  static final int CHUNK_SHIFT = 6;
  static final int CHUNK_ROWS = 1 << CHUNK_SHIFT;
  static final int CHUNKS_PER_COLUMN = ProjectionIndexHOTStorage.MAX_ROW_GROUPS >>> CHUNK_SHIFT;
  private static final int MAGIC = 0x31504E50; // PNP1
  private static final int HEADER_BYTES = 24;
  private static final int ENTRY_BYTES = 32;
  static final int CHUNK_BYTES = HEADER_BYTES + CHUNK_ROWS * ENTRY_BYTES;

  private ProjectionNumericProofs() {}

  static long slot(final int column, final int chunk) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS || chunk < 0 || chunk >= CHUNKS_PER_COLUMN) {
      throw new IllegalArgumentException("numeric proof column or chunk out of range");
    }
    return SLOT_BASE + (long) column * CHUNKS_PER_COLUMN + chunk;
  }

  static int chunk(final long rowGroupId) {
    if (rowGroupId < 1 || rowGroupId > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("numeric proof row group out of range");
    }
    return (int) ((rowGroupId - 1) >>> CHUNK_SHIFT);
  }

  static boolean available(final byte @Nullable [] header, final int column) {
    if (header == null) {
      return false;
    }
    if (header.length != 12 || ProjectionIndexRowGroupCodec.getIntLE(header, 0) != MAGIC
        || ProjectionIndexRowGroupCodec.getIntLE(header, 4) != CHUNK_SHIFT) {
      throw new IllegalStateException("invalid numeric proof header");
    }
    final int columns = ProjectionIndexRowGroupCodec.getIntLE(header, 8);
    if (columns < 1 || columns > RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalStateException("invalid numeric proof column count");
    }
    return column >= 0 && column < columns;
  }

  /** One bounded pending chunk per column; publication transfers ownership of its byte array. */
  static final class Builder {
    private byte[] @Nullable [] pending;
    private int lastRowGroup;
    private boolean anyProof;
    private boolean finished;

    void append(final ProjectionIndexHOTStorage storage, final int rowGroupId, final EncodedRowGroup encoded) {
      Objects.requireNonNull(storage, "storage");
      Objects.requireNonNull(encoded, "encoded");
      if (finished || rowGroupId != lastRowGroup + 1) {
        throw new IllegalStateException("numeric proof builder requires consecutive row groups");
      }
      final int chunk = chunk(rowGroupId);
      final byte[] descriptor = encoded.descriptor();
      final int columns = RowGroupDescriptor.columnCount(descriptor);
      if (pending == null) {
        RowGroupDescriptor.validate(descriptor);
        pending = new byte[columns][];
      } else if (pending.length != columns) {
        throw new IllegalStateException("numeric proof build changed column shape");
      }
      final int rows = RowGroupDescriptor.rowCount(descriptor);
      for (int column = 0; column < columns; column++) {
        if (rows == 0
            || RowGroupDescriptor.kind(descriptor, column) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
          continue;
        }
        final int id = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column);
        final int index = Arrays.binarySearch(encoded.columnSegmentIds(), id);
        if (index < 0) {
          throw new IllegalStateException("numeric proof source has no BODY");
        }
        final byte[] body = encoded.segments()[index];
        // Also used by maintenance/backfill callers: never trust descriptor mirrors alone.
        ProjectionIndexColumnSegmentCodec.verifyColumnSegment(descriptor, body, id,
            ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY);
        if (body[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES] != 0 || !fullPresence(body, rows)) {
          continue;
        }
        byte[] bytes = pending[column];
        if (bytes == null) {
          bytes = new byte[CHUNK_BYTES];
          ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 0, MAGIC);
          ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 4, CHUNK_SHIFT);
          ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 8, column);
          ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 12, chunk);
          pending[column] = bytes;
        }
        final int position = (rowGroupId - 1) & (CHUNK_ROWS - 1);
        final int offset = HEADER_BYTES + position * ENTRY_BYTES;
        final int entry = RowGroupDescriptor.entryIndexOf(descriptor, id);
        ProjectionIndexRowGroupCodec.putLongLEAt(bytes, offset, RowGroupDescriptor.entryContentHash(descriptor, entry));
        final int bounds = ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES + 1;
        ProjectionIndexRowGroupCodec.putLongLEAt(bytes, offset + 8,
            ProjectionIndexRowGroupCodec.getLongLE(body, bounds));
        ProjectionIndexRowGroupCodec.putLongLEAt(bytes, offset + 16,
            ProjectionIndexRowGroupCodec.getLongLE(body, bounds + 8));
        ProjectionIndexRowGroupCodec.putIntLEAt(bytes, offset + 24, rows);
        ProjectionIndexRowGroupCodec.putIntLEAt(bytes, offset + 28, body.length);
        ProjectionIndexRowGroupCodec.putLongLEAt(bytes, 16,
            ProjectionIndexRowGroupCodec.getLongLE(bytes, 16) | 1L << position);
        anyProof = true;
      }
      lastRowGroup = rowGroupId;
      if ((rowGroupId & (CHUNK_ROWS - 1)) == 0) {
        flush(storage);
      }
    }

    void finish(final ProjectionIndexHOTStorage storage) {
      if (finished) {
        throw new IllegalStateException("numeric proof builder is finished");
      }
      flush(storage);
      if (anyProof) {
        final byte[] header = new byte[12];
        ProjectionIndexRowGroupCodec.putIntLEAt(header, 0, MAGIC);
        ProjectionIndexRowGroupCodec.putIntLEAt(header, 4, CHUNK_SHIFT);
        ProjectionIndexRowGroupCodec.putIntLEAt(header, 8, pending.length);
        storage.putBlob(HEADER_SLOT, header);
      }
      finished = true;
    }

    private void flush(final ProjectionIndexHOTStorage storage) {
      if (pending == null) {
        return;
      }
      for (int column = 0; column < pending.length; column++) {
        final byte[] bytes = pending[column];
        if (bytes != null) {
          storage.putBlob(slot(column, ProjectionIndexRowGroupCodec.getIntLE(bytes, 12)), bytes);
          pending[column] = null;
        }
      }
    }
  }

  private static boolean fullPresence(final byte[] body, final int rows) {
    final int offset = ProjectionIndexColumnSegmentCodec.BODY_PRESENCE_MARKER_OFFSET;
    if (offset >= body.length) {
      throw new IllegalStateException("truncated numeric proof presence");
    }
    final int mode = body[offset] & 0xFF;
    if (mode == 0) {
      return true;
    }
    if (mode == 1) {
      return false;
    }
    final int words = (rows + 63) >>> 6;
    if (mode != 2 || offset + 1 + words * Long.BYTES > body.length) {
      throw new IllegalStateException("invalid numeric proof presence");
    }
    for (int word = 0; word < words; word++) {
      final int remaining = rows - word * 64;
      final long expected = remaining >= 64
          ? -1L
          : (1L << remaining) - 1;
      if (ProjectionIndexRowGroupCodec.getLongLE(body, offset + 1 + word * Long.BYTES) != expected) {
        return false;
      }
    }
    return true;
  }

  /** Validated immutable chunk. Its entry gate is the only caller of the metadata-only decoder. */
  static final class Chunk {
    private final byte[] bytes;
    private final int column;
    private final int chunk;

    Chunk(final byte[] bytes, final int column, final int chunk) {
      slot(column, chunk);
      if (bytes.length != CHUNK_BYTES || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != MAGIC
          || ProjectionIndexRowGroupCodec.getIntLE(bytes, 4) != CHUNK_SHIFT
          || ProjectionIndexRowGroupCodec.getIntLE(bytes, 8) != column
          || ProjectionIndexRowGroupCodec.getIntLE(bytes, 12) != chunk) {
        throw new IllegalStateException("invalid numeric proof chunk");
      }
      this.bytes = bytes;
      this.column = column;
      this.chunk = chunk;
    }

    @Nullable
    ColumnSlice decode(final byte[] descriptor, final long rowGroupId, final NumericBucketDecoder decoder) {
      if (chunk(rowGroupId) != chunk
          || RowGroupDescriptor.kind(descriptor, column) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        throw new IllegalStateException("numeric proof source identity mismatch");
      }
      final int position = (int) ((rowGroupId - 1) & (CHUNK_ROWS - 1));
      if ((ProjectionIndexRowGroupCodec.getLongLE(bytes, 16) & 1L << position) == 0) {
        return null;
      }
      final int offset = HEADER_BYTES + position * ENTRY_BYTES;
      final long min = ProjectionIndexRowGroupCodec.getLongLE(bytes, offset + 8);
      final long max = ProjectionIndexRowGroupCodec.getLongLE(bytes, offset + 16);
      final int rows = ProjectionIndexRowGroupCodec.getIntLE(bytes, offset + 24);
      final int length = ProjectionIndexRowGroupCodec.getIntLE(bytes, offset + 28);
      if (rows < 1 || rows > ProjectionIndexRowGroupPage.MAX_ROWS || min > max
          || length <= ProjectionIndexColumnSegmentCodec.BODY_PRESENCE_MARKER_OFFSET) {
        throw new IllegalStateException("invalid numeric proof entry");
      }
      final int entry =
          RowGroupDescriptor.entryIndexOf(descriptor, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column));
      if (entry < 0) {
        throw new IllegalStateException("numeric proof source has no BODY descriptor");
      }
      if (RowGroupDescriptor.entryContentHash(descriptor, entry) != ProjectionIndexRowGroupCodec.getLongLE(bytes,
          offset) || RowGroupDescriptor.rowCount(descriptor) != rows) {
        return null; // fine-grained writes invalidate old evidence without rewriting a proof chunk
      }
      if (RowGroupDescriptor.entryByteLen(descriptor, entry) != length
          || RowGroupDescriptor.entryColFlags(descriptor, entry) != 0
          || RowGroupDescriptor.entryMin(descriptor, entry) != min
          || RowGroupDescriptor.entryMax(descriptor, entry) != max) {
        throw new IllegalStateException("numeric proof disagrees with BODY descriptor mirrors");
      }
      return decoder.decodeFullPresenceProof(rows, min, max);
    }
  }
}
