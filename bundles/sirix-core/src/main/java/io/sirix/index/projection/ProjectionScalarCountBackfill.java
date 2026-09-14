/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.json.JsonNodeTrx;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Adds an exact low-cardinality scalar count summary to an existing revisioned projection.
 * Reads only the target column's BODY and DICT segments; slot 0 is published after the summary.
 */
public final class ProjectionScalarCountBackfill {

  public record Result(boolean published, String reason, int leaves, long rows, int groups, int revision) {}

  private ProjectionScalarCountBackfill() {
    throw new AssertionError("no instances");
  }

  /** The caller commits the write transaction only when {@link Result#published()} is true. */
  public static Result run(final JsonNodeTrx wtx, final int indexNumber, final int column) {
    if (wtx == null) {
      throw new NullPointerException("write transaction is required");
    }
    if (indexNumber < 0 || column < 0) {
      throw new IllegalArgumentException("index number and column must be non-negative");
    }
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), indexNumber);
    final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(storage.getBlob(0L));
    if (metadata == null || metadata.isStale()) {
      throw new IllegalStateException("projection " + indexNumber + " has no usable metadata");
    }
    final byte[] kinds = metadata.columnKinds();
    if (column >= kinds.length || kinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalArgumentException("column " + column + " is not a scalar local-dictionary string");
    }
    final Map<Integer, Map<String, Long>> existing = metadata.setValueRowCounts();
    if (existing != null && existing.containsKey(column)) {
      return new Result(false, "already summarized", 0, 0L, 0, wtx.getRevisionNumber());
    }

    final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(storage, metadata.rowGroupCount());
    final int maxValues = ProjectionSetSummaryChunks.maxValues();
    final long[] localCounts = new long[maxValues];
    final Map<String, Long> counts = new LinkedHashMap<>(Math.min(maxValues * 2, 64));
    long rows = 0L;
    for (final int slot : physicalOrder) {
      final byte[] descriptor = storage.getVerifiedRowGroupDescriptor(slot);
      if (descriptor == null || RowGroupDescriptor.kind(descriptor, column) != kinds[column]) {
        throw new IllegalStateException("live leaf " + slot + " does not match scalar column " + column);
      }
      final byte[] body = storage.getVerifiedColumnSegment(slot, descriptor,
          ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column),
          ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY);
      final byte[] dictionary = storage.getVerifiedColumnSegment(slot, descriptor,
          ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(column),
          ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT);
      final ProjectionColumnStore.ColumnSlice slice =
          ProjectionIndexColumnSegmentCodec.decodeStringSlice(descriptor, body, dictionary, column);
      if ((slice.flags() & ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE) != 0) {
        return new Result(false, "unrepresentable value", physicalOrder.length, rows, counts.size(),
            wtx.getRevisionNumber());
      }
      final int dictionarySize = slice.dictSize();
      if (dictionarySize > maxValues) {
        return new Result(false, "dictionary exceeds summary value bound", physicalOrder.length, rows,
            counts.size(), wtx.getRevisionNumber());
      }
      Arrays.fill(localCounts, 0, dictionarySize, 0L);
      final int[] ids = slice.stringDictIds();
      final long[] presence = slice.presenceWords();
      long missing = 0L;
      for (int row = 0; row < slice.rowCount(); row++) {
        if ((presence[row >>> 6] & (1L << (row & 63))) == 0L) {
          missing++;
        } else {
          final int id = ids[row];
          if (id < 0 || id >= dictionarySize) {
            throw new IllegalStateException("invalid dictionary id " + id + " in live leaf " + slot);
          }
          localCounts[id]++;
        }
      }
      rows = Math.addExact(rows, slice.rowCount());
      if (missing > 0) {
        counts.merge(null, missing, Math::addExact);
      }
      final byte[] bytes = slice.dictBytes();
      final int[] offsets = slice.dictOffsets();
      for (int id = 0; id < dictionarySize; id++) {
        if (localCounts[id] == 0) {
          continue;
        }
        final String value = new String(bytes, offsets[id], offsets[id + 1] - offsets[id], StandardCharsets.UTF_8);
        counts.merge(value, localCounts[id], Math::addExact);
      }
      if (counts.size() > maxValues) {
        return new Result(false, "summary exceeds value bound", physicalOrder.length, rows, counts.size(),
            wtx.getRevisionNumber());
      }
    }
    if (!ProjectionSetSummaryChunks.publishColumn(storage, column, counts)) {
      return new Result(false, "summary exceeds byte bound", physicalOrder.length, rows, counts.size(),
          wtx.getRevisionNumber());
    }
    ProjectionFlagSummaryChunks.retagUnchanged(storage, metadata.rowGroupCount(), kinds.length,
        metadata.buildRevision(), wtx.getRevisionNumber());
    storage.putBlob(0L, metadata.withValueSummaryColumn(column, wtx.getRevisionNumber()).serialize());
    return new Result(true, "published", physicalOrder.length, rows, counts.size(), wtx.getRevisionNumber());
  }

  /**
   * Repair an earlier metadata-only backfill whose flag header was left at the preceding revision.
   * No leaf or summary count is changed; the caller commits the retag in a new revision.
   */
  public static void repairFlagSummaryRevision(final JsonNodeTrx wtx, final int indexNumber,
      final int expectedHeaderRevision) {
    if (wtx == null) {
      throw new NullPointerException("write transaction is required");
    }
    if (indexNumber < 0 || expectedHeaderRevision < 0) {
      throw new IllegalArgumentException("index and expected header revision must be non-negative");
    }
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), indexNumber);
    final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(storage.getBlob(0L));
    if (metadata == null || metadata.isStale() || metadata.buildRevision() <= expectedHeaderRevision
        || !ProjectionFlagSummaryChunks.retagUnchanged(storage, metadata.rowGroupCount(),
            metadata.columnKinds().length, expectedHeaderRevision, metadata.buildRevision())) {
      throw new IllegalStateException("projection has no exact prior flag-summary header to retag");
    }
  }
}
