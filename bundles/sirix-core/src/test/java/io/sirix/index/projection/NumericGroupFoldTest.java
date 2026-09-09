package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class NumericGroupFoldTest {
  private record Fixture(ProjectionColumnStore store, ColumnSlice[] keys, ColumnSlice[][] operands,
      Map<Long, long[]> expected) {
  }

  @Test
  void numericFoldMatchesIndependentAggregatesWithSparseOperandsAndMissingKeys() {
    for (final int columns : new int[] {0, 1, 3}) {
      for (final boolean dense : new boolean[] {false, true}) {
        final Fixture fixture = fixture(columns, false);
        final NumericGroupAggTable table = new NumericGroupAggTable(columns, 16, false, 1L);
        if (dense) {
          table.useDenseIndex();
        }
        final long[] missing = ProjectionIndexByteScan.newGroupAggAcc(columns, Long.MAX_VALUE);
        scan(fixture, table, missing);
        assertEquals(fixture.expected().size(), table.sizeIncludingZero() + 1);
        for (final Map.Entry<Long, long[]> entry : fixture.expected().entrySet()) {
          final Long key = entry.getKey();
          final long[] actual;
          if (key == null) {
            actual = missing;
          } else if (key == 0L) {
            actual = table.zeroSlot();
          } else {
            final int handle = table.acquire(key, Long.MAX_VALUE);
            final int base = table.offsetAtAccBase(handle);
            actual = Arrays.copyOfRange(table.storageAtAccBase(handle), base, base + table.slotWidth());
          }
          assertArrayEquals(entry.getValue(), actual, "group " + key + ", columns " + columns);
        }
      }
    }
  }

  @Test
  void exactSumOverflowStillDeclines() {
    final Fixture fixture = fixture(1, true);
    assertThrows(ArithmeticException.class, () -> scan(fixture, new NumericGroupAggTable(1, 16),
        ProjectionIndexByteScan.newGroupAggAcc(1, Long.MAX_VALUE)));
  }

  private static void scan(final Fixture fixture, final NumericGroupAggTable table, final long[] missing) {
    ProjectionColumnGroupScan.aggregateByGroupNumericFlat(fixture.store(), new ColumnPredicate[0], new ColumnSlice[0][],
        null, null, fixture.keys(), fixture.operands(), null, 0, fixture.store().rowGroupCount(), table, missing, -1,
        null, null, null, false, null, null);
  }

  private static Fixture fixture(final int columns, final boolean overflow) {
    final Map<Long, byte[]> segments = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>();
    final Map<Long, long[]> expected = new HashMap<>();
    final byte[] kinds = new byte[columns + 1];
    Arrays.fill(kinds, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG);
    long address = 1;
    for (int leaf = 0; leaf < 4; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
      for (int row = 0; row < 511; row++) {
        final long[] values = new long[columns + 1];
        final boolean[] present = new boolean[columns + 1];
        final long key = row % 61 - 30;
        values[0] = key;
        present[0] = row % 17 != 0;
        final long ordinal = (long) leaf << 20 | row;
        final long[] acc = expected.computeIfAbsent(present[0]
            ? key
            : null, ignored -> ProjectionIndexByteScan.newGroupAggAcc(columns, ordinal));
        acc[0]++;
        for (int column = 0; column < columns; column++) {
          final long value = overflow || column > 0
              ? Long.MAX_VALUE - row
              : row - 255;
          values[column + 1] = value;
          present[column + 1] = (row + column) % 7 != 0;
          if (present[column + 1]) {
            final int at = 2 + 4 * column;
            acc[at]++;
            if (column == 0 && !overflow) {
              acc[at + 1] += value;
            }
            acc[at + 2] = Math.min(acc[at + 2], value);
            acc[at + 3] = Math.max(acc[at + 3], value);
          }
        }
        page.appendRow(leaf * 1000L + row + 1, values, new boolean[columns + 1], new String[columns + 1], present,
            new boolean[columns + 1], new boolean[columns + 1]);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int[] ids = encoded.columnSegmentIds();
      final long[] offsets = new long[ids.length];
      for (int i = 0; i < ids.length; i++) {
        offsets[i] = address++;
        segments.put(offsets[i], encoded.segments()[i]);
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets, new byte[ids.length][]));
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    final ColumnSegmentFetcher fetcher = offsets -> {
      final byte[][] bytes = new byte[offsets.length][];
      for (int i = 0; i < offsets.length; i++) {
        bytes[i] = segments.get(offsets[i]);
      }
      return bytes;
    };
    final ColumnSlice[][] operands = new ColumnSlice[columns][];
    for (int column = 0; column < columns; column++) {
      operands[column] = store.column(column + 1, fetcher);
    }
    return new Fixture(store, store.column(0, fetcher), operands, expected);
  }
}
