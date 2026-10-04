/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;

/** Key-range packing policy for encoded inline chunks and their bounded summaries. */
final class ProjectionSortedChunks {
  static final int MAX_BYTES = ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES;
  static final int TARGET_BYTES = MAX_BYTES * 3 / 4;
  static final int MERGE_BELOW_BYTES = MAX_BYTES / 4;

  private ProjectionSortedChunks() {}

  /** Slice initial-build runs into bounded chunks with canonical encodings. */
  static ProjectionSortedLeaf[] pack(final ProjectionSortedLeaf leaf, final ProjectionSortKeyCodec.Layout layout,
      final int target) {
    final ProjectionSortedDataBlock block = leaf.dataBlock();
    if (block != null && block.hasSingleDictionaryEntry()) {
      final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.encode(leaf, layout);
      if (summary == null || summary.encodedBytes().length <= MAX_BYTES) {
        final ArrayList<ProjectionSortedLeaf> chunks = new ArrayList<>();
        for (int from = 0; from < leaf.rowCount();) {
          final ProjectionSortedLeaf chunk = leaf.fittingSliceBlock(from, target);
          chunks.add(chunk);
          from += chunk.rowCount();
        }
        return chunks.toArray(ProjectionSortedLeaf[]::new);
      }
    }
    final byte[][] keys = new byte[leaf.rowCount()][];
    final byte[][] payloads = new byte[leaf.rowCount()][];
    for (int row = 0; row < leaf.rowCount(); row++) {
      keys[row] = leaf.copyKey(row);
      payloads[row] = leaf.copyPayload(row);
    }
    return pack(keys, payloads, keys.length, layout, target);
  }

  static boolean fits(final ProjectionSortedLeaf leaf, final ProjectionSortKeyCodec.Layout layout, final int limit) {
    if (leaf.rowCount() == 1) {
      // A genuinely oversized tuple uses the generic blob side-page representation.
      return true;
    }
    if (leaf.encodedBytes().length > limit) {
      return false;
    }
    final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.encode(leaf, layout);
    return summary == null || summary.encodedBytes().length <= MAX_BYTES;
  }

  /** Pack only the range being edited. No neighboring range is read or renumbered. */
  static ProjectionSortedLeaf[] pack(final byte[][] keys, final byte @Nullable [][] payloads, final int count,
      final ProjectionSortKeyCodec.Layout layout, final int target) {
    final ArrayList<ProjectionSortedLeaf> chunks = new ArrayList<>();
    for (int from = 0; from < count;) {
      int low = 1;
      int high = Math.min(ProjectionSortedLeaf.MAX_ROWS, count - from);
      // Most maintenance ranges fit unchanged. Encode that complete range once before sizing
      // a split; the binary search otherwise encodes every progressively larger prefix.
      ProjectionSortedLeaf best = ProjectionSortedLeaf.encode(keys, payloads, from, high);
      if (best != null && fits(best, layout, target)) {
        chunks.add(best);
        from += high;
        continue;
      }
      best = null;
      high--;
      while (low <= high) {
        final int rows = (low + high) >>> 1;
        final ProjectionSortedLeaf candidate = ProjectionSortedLeaf.encode(keys, payloads, from, rows);
        if (candidate != null && fits(candidate, layout, target)) {
          best = candidate;
          low = rows + 1;
        } else {
          high = rows - 1;
        }
      }
      if (best == null) {
        throw new IllegalArgumentException("sorted projection row exceeds a bounded page");
      }
      chunks.add(best);
      from += best.rowCount();
    }
    return chunks.toArray(ProjectionSortedLeaf[]::new);
  }
}
