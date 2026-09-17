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
 * The catalog's resident weight of a directory list is reduced on the common pool above a size
 * threshold. The sum must equal an independent serial fold of the same per-leaf formula for both a
 * small list (the serial route) and a large one (the parallel route), so the handle weight a
 * cache admits with cannot depend on which route computed it.
 */
final class ProjectionResidentWeightTest {

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN};

  @Test
  void theParallelReduceEqualsTheSerialFold() {
    final List<RowGroupDirectory> small = directories(300);
    final List<RowGroupDirectory> large = directories(5_000);
    assertEquals(oracle(small), ProjectionIndexCatalog.residentWeightOf(small));
    assertEquals(oracle(large), ProjectionIndexCatalog.residentWeightOf(large));
    assertEquals(0L, ProjectionIndexCatalog.residentWeightOf(List.of()));
  }

  /** The per-leaf formula of {@code ProjectionIndexCatalog.residentWeightOf(byte[])}, folded serially. */
  private static long oracle(final List<RowGroupDirectory> directories) {
    long bytes = 0;
    for (final RowGroupDirectory directory : directories) {
      final byte[] descriptor = directory.descriptor();
      final int segments = RowGroupDescriptor.columnSegmentCount(descriptor);
      for (int entry = 0; entry < segments; entry++) {
        bytes += RowGroupDescriptor.entryByteLen(descriptor, entry);
      }
      final int columns = RowGroupDescriptor.columnCount(descriptor);
      for (int column = 0; column < columns; column++) {
        bytes += ProjectionColumnStore.decodedColumnResidentBytes(descriptor, column,
            RowGroupDescriptor.kind(descriptor, column));
      }
    }
    return bytes;
  }

  private static List<RowGroupDirectory> directories(final int count) {
    final List<RowGroupDirectory> out = new ArrayList<>(count);
    for (int leaf = 0; leaf < count; leaf++) {
      // Three distinct row counts, so the per-leaf weights differ and a wrong split or a dropped
      // range would change the sum.
      out.add(directory(leaf + 1, 1 + leaf % 3));
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
      longs[0] = rowGroupId + row;
      strings[1] = "value-" + rowGroupId + "-" + row;
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
