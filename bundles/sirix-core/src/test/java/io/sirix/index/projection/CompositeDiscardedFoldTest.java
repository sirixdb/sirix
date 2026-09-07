package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.HashCommon;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CompositeDiscardedFoldTest {
  private static final byte NUMERIC = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  private static final byte[] KEY_KINDS = {NUMERIC, NUMERIC};
  private static final ColumnPredicate[] NO_PREDICATES = new ColumnPredicate[0];

  private record Fixture(ProjectionColumnStore store, ColumnSlice[][] keys, ColumnSlice[][] operands) {}

  private record Identity(long missing, long first, long second) {}

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void discardedGroupsNeverFoldIntoTheSharedScratch(final boolean transformedLoop) {
    final long first = keyInPartitionZero(1);
    final long second = keyInPartitionZero(first + 1);
    final Fixture fixture = fixture(new long[][] {{first, 7, Long.MAX_VALUE}, {second, 7, Long.MAX_VALUE}});
    final NumericGroupAggTable excluded = table();
    excluded.setPassRange(63, 1, 2);
    // Each group has a valid sum. Folding both into the non-owning pass's shared scratch overflows.
    assertDoesNotThrow(() -> scan(fixture, excluded, transformedLoop));
    assertEquals(0, excluded.size());
    final int discard = NumericGroupAggTable.DISCARD_HANDLE;
    final int base = excluded.offsetAtAccBase(discard);
    assertArrayEquals(new long[excluded.slotWidth()],
        Arrays.copyOfRange(excluded.storageAtAccBase(discard), base, base + excluded.slotWidth()));

    final NumericGroupAggTable owner = table();
    owner.setPassRange(63, 0, 1);
    assertDoesNotThrow(() -> scan(fixture, owner, transformedLoop));
    assertEquals(2, owner.size(), "the owning pass must still fold both distinct groups");
    for (final long[] aggregate : snapshot(owner).values()) {
      assertEquals(Long.MAX_VALUE, aggregate[3]);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void owningGroupOverflowStillFails(final boolean transformedLoop) {
    final long key = keyInPartitionZero(1);
    final Fixture fixture = fixture(new long[][] {{key, 7, Long.MAX_VALUE}, {key, 7, 1}});
    final NumericGroupAggTable owner = table();
    owner.setPassRange(63, 0, 1);
    assertThrows(ArithmeticException.class, () -> scan(fixture, owner, transformedLoop));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void partitionedFoldsPreserveMultiplicityMissingKeysAndFirstSeen(final boolean transformedLoop) {
    final long[][] rows = {{0, 7, 4}, {-3, 7, -8}, {0, 7, 5}, {Long.MIN_VALUE, 7, 11}, {-3, 7, 9},
        {Long.MIN_VALUE, 7, 12}, {keyInPartitionZero(1), 3, 17}};
    final Fixture fixture = fixture(rows);
    final NumericGroupAggTable all = table();
    scan(fixture, all, transformedLoop);
    final NumericGroupAggTable merged = table();
    for (int partition = 0; partition < 2; partition++) {
      final NumericGroupAggTable part = table();
      part.setPassRange(63, partition, partition + 1);
      scan(fixture, part, transformedLoop);
      NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {part}, 0, 64, merged);
    }
    final Map<Identity, long[]> expected = snapshot(all);
    final Map<Identity, long[]> actual = snapshot(merged);
    assertEquals(4, expected.size());
    assertEquals(expected.keySet(), actual.keySet());
    for (final Identity identity : expected.keySet()) {
      assertArrayEquals(expected.get(identity), actual.get(identity), identity.toString());
    }
    assertArrayEquals(new long[] {2, 0, 2, 9, 4, 5, 0}, actual.get(new Identity(0, 0, 7)));
    assertArrayEquals(new long[] {2, 3, 2, 23, 11, 12, 3}, actual.get(new Identity(1, 0, 7)));
  }

  @Test
  void keyTransformErrorsAreCheckedBeforePassOwnership() {
    final Fixture fixture = fixture(new long[][] {{Long.MAX_VALUE, 7, 1}});
    final NumericGroupAggTable out = table();
    out.setPassRange(63, 0, 1);
    final long[] decline = {0};
    scan(fixture, out, new long[] {1, 0}, decline);
    assertEquals(1, decline[0], "an overflowing key transform must still decline");
    assertEquals(0, out.size());
  }

  private static NumericGroupAggTable table() {
    return new NumericGroupAggTable(1, 16, true, 1L, 3);
  }

  private static void scan(final Fixture fixture, final NumericGroupAggTable out, final boolean transformedLoop) {
    scan(fixture, out, transformedLoop ? new long[2] : null, new long[1]);
  }

  private static void scan(final Fixture fixture, final NumericGroupAggTable out, final long[] offsets,
      final long[] decline) {
    ProjectionColumnGroupScan.aggregateByGroupCompositeFlat(fixture.store(), NO_PREDICATES, new ColumnSlice[0][],
        null, null, fixture.keys(), KEY_KINDS, fixture.operands(), 0, 1, out, -1, null, null, offsets, null, decline,
        null, null, null, null, null);
  }

  private static long keyInPartitionZero(final long from) {
    for (long key = from; ; key++) {
      final long hash = (ProjectionIndexByteScan.FNV_SEED * ProjectionIndexByteScan.FNV_PRIME ^ HashCommon.mix(key))
          * ProjectionIndexByteScan.FNV_PRIME ^ HashCommon.mix(7L);
      if (hash != 0 && HashCommon.mix(hash) >>> 63 == 0) {
        return key;
      }
    }
  }

  private static Map<Identity, long[]> snapshot(final NumericGroupAggTable table) {
    final Map<Identity, long[]> result = new HashMap<>();
    for (int c = 0; c < table.storageChunkCount(); c++) {
      final long[] chunk = table.storageChunkOrNull(c);
      if (chunk == null) {
        continue;
      }
      for (int offset = 0; offset < chunk.length; offset += table.stride()) {
        if (chunk[offset] != 0) {
          result.put(new Identity(chunk[offset + 8], chunk[offset + 9], chunk[offset + 10]),
              Arrays.copyOfRange(chunk, offset + 1, offset + 8));
        }
      }
    }
    return result;
  }

  private static Fixture fixture(final long[][] rows) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {NUMERIC, NUMERIC, NUMERIC});
    for (int row = 0; row < rows.length; row++) {
      page.appendRow(row + 1, rows[row], new boolean[3], new String[3],
          new boolean[] {rows[row][0] != Long.MIN_VALUE, true, true}, new boolean[3], new boolean[3]);
    }
    final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    final long[] offsets = new long[encoded.columnSegmentIds().length];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = i + 1;
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(List.of(new RowGroupDirectory(1,
        encoded.descriptor(), encoded.columnSegmentIds(), offsets, new byte[offsets.length][])));
    final ColumnSlice[][] columns = new ColumnSlice[3][];
    for (int col = 0; col < columns.length; col++) {
      columns[col] = store.column(col, wanted -> {
        final byte[][] segments = new byte[wanted.length][];
        for (int i = 0; i < wanted.length; i++) {
          segments[i] = encoded.segments()[Math.toIntExact(wanted[i] - 1)];
        }
        return segments;
      });
    }
    return new Fixture(store, new ColumnSlice[][] {columns[0], columns[1]}, new ColumnSlice[][] {columns[2]});
  }
}
