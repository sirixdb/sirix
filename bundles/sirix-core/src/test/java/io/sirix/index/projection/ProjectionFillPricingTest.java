/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The fill-budget pricing walks (projected column fill bytes, and the masked variant) are reduced on
 * the common pool above a leaf-count threshold. For a small store (serial route) and a large one
 * (parallel route) the sums must equal an independent serial fold of the same per-leaf formula, and
 * a masked walk must price exactly the surviving leaves.
 */
final class ProjectionFillPricingTest {

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN};

  @Test
  void pricingMatchesTheSerialFoldOnBothRoutes() {
    for (final int leaves : new int[] {257, 5_003}) {
      final List<RowGroupDirectory> directories = directories(leaves);
      final ProjectionColumnStore store = new ProjectionColumnStore(directories);
      final long[] keep = new long[(leaves + 63) >>> 6];
      for (int leaf = 0; leaf < leaves; leaf++) {
        if (leaf % 3 != 1) {
          keep[leaf >>> 6] |= 1L << (leaf & 63);
        }
      }
      for (int col = 0; col < KINDS.length; col++) {
        final long expectedFull = oracle(directories, col, null);
        assertEquals(Math.max(1, expectedFull), store.projectedColumnFillBytes(col), "column " + col + " leaves " + leaves);
        assertEquals(store.projectedColumnFillBytes(col), store.projectedColumnFillBytes(col), "memoized sum is stable");
        assertEquals(oracle(directories, col, keep), store.projectedMaskedFillBytes(col, keep),
            "masked column " + col + " leaves " + leaves);
        assertEquals(store.projectedColumnFillBytes(col), store.projectedMaskedFillBytes(col, null));
      }
    }
  }

  /** The per-leaf formula of the store's pricing walks, folded serially over the surviving leaves. */
  private static long oracle(final List<RowGroupDirectory> directories, final int col, final long[] keep) {
    final int bodySegId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
    final int dictSegId = ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(col);
    long bytes = 0;
    for (int i = 0; i < directories.size(); i++) {
      if (keep != null && (keep[i >>> 6] & (1L << (i & 63))) == 0) {
        continue;
      }
      final byte[] descriptor = directories.get(i).descriptor();
      final int bodyEntry = RowGroupDescriptor.entryIndexOf(descriptor, bodySegId);
      if (bodyEntry >= 0) {
        bytes += RowGroupDescriptor.entryByteLen(descriptor, bodyEntry);
      }
      final int dictEntry = RowGroupDescriptor.entryIndexOf(descriptor, dictSegId);
      if (dictEntry >= 0) {
        bytes += RowGroupDescriptor.entryByteLen(descriptor, dictEntry);
      }
      bytes += ProjectionColumnStore.decodedColumnResidentBytes(descriptor, col, KINDS[col]);
    }
    return bytes;
  }

  private static List<RowGroupDirectory> directories(final int count) {
    final List<RowGroupDirectory> out = new ArrayList<>(count);
    for (int leaf = 0; leaf < count; leaf++) {
      out.add(directory(leaf + 1, 1 + leaf % 4));
    }
    return out;
  }

  private static RowGroupDirectory directory(final long rowGroupId, final int rows) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final long[] longs = new long[KINDS.length];
    final boolean[] bools = new boolean[KINDS.length];
    final String[] strings = new String[KINDS.length];
    final boolean[] present = new boolean[KINDS.length];
    final boolean[] unrep = new boolean[KINDS.length];
    final boolean[] nonIntegral = new boolean[KINDS.length];
    final boolean[] nonDoubleSource = new boolean[KINDS.length];
    for (int row = 0; row < rows; row++) {
      longs[0] = rowGroupId * 7 + row;
      strings[1] = "v-" + rowGroupId + "-" + row;
      bools[2] = (row & 1) == 0;
      for (int c = 0; c < KINDS.length; c++) {
        present[c] = true;
      }
      page.appendRow(rowGroupId * 1000 + row, longs, bools, strings, present, unrep, nonIntegral, nonDoubleSource);
    }
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    final int segments = encoded.columnSegmentIds().length;
    final int[] ids = new int[segments];
    final long[] offsets = new long[segments];
    for (int i = 0; i < segments; i++) {
      ids[i] = encoded.columnSegmentIds()[i];
      offsets[i] = 1_000L + i;
    }
    return new RowGroupDirectory(rowGroupId, encoded.descriptor(), ids, offsets, new byte[ids.length][]);
  }
}
