package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.HashCommon;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;

final class ConstantBucketGroupCountTest {
  private static final byte DICT = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  private static final byte LONG = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  private static final ColumnPredicate[] SELECTED = {ColumnPredicate.numeric(2, ProjectionIndexScan.Op.EQ, 1L)};

  private record Row(String text, Long number, boolean selected) {
  }

  private record Key(String text, Long bucket) {
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void optionalBucketScratchDoesNotDeclineAnOrdinaryFillThatFits(final boolean retainedBody) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {LONG});
    for (int row = 0; row < 3; row++) {
      assertTrue(page.appendRow(row + 1L, new long[] {100L + row}, new boolean[1], new String[1], new boolean[] {true},
          new boolean[1], new boolean[1]));
    }
    final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    final long[] offsets = new long[encoded.segments().length];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = i + 1L;
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(List.of(new RowGroupDirectory(1L,
        encoded.descriptor(), encoded.columnSegmentIds(), offsets, new byte[offsets.length][])));
    final ColumnSegmentFetcher fetcher = keys -> {
      final byte[][] values = new byte[keys.length][];
      for (int i = 0; i < keys.length; i++) {
        values[i] = encoded.segments()[Math.toIntExact(keys[i] - 1)];
      }
      return values;
    };
    if (retainedBody) {
      store.columnBytes(0, fetcher);
    }
    final long previous = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(store.projectedColumnFillBytes(0));
    try {
      final ColumnSlice[] slices = store.numericBucketKeyColumn(0, fetcher, 0L, 100L, 0L);
      assertTrue(store.columnFilled(0), "the exact ordinary fill should remain available at its own budget");
      assertSame(slices, store.column(0, fetcher));
      assertArrayEquals(new long[] {100L, 101L, 102L}, slices[0].numericValues());
    } finally {
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previous);
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0,0", "1,7,0", "0,-7,24", "1,0,24"})
  void bucketViewsRemainExactAcrossCacheCapacityAndModulusCycles(final int dictionaryKey, final long shift,
      final long modulus) {
    final List<List<Row>> blocks = new ArrayList<>();
    final Random random = new Random(99127);
    for (int block = 0; block < 270; block++) {
      final List<Row> rows = new ArrayList<>();
      for (int row = 0; row < 1 + block % 5 * 67; row++) {
        rows.add(new Row(row % 11 == 0
            ? null
            : row % 7 == 0
                ? ""
                : "text-β-" + row % 9,
            row % 13 == 0
                ? null
                : block * 100L + 10 + random.nextInt(60),
            row % 4 != 0));
      }
      blocks.add(rows);
    }
    blocks.add(
        List.of(new Row("boundary", 99L, true), new Row("boundary", 100L, true), new Row("boundary", 2500L, true)));
    assertScan(blocks, dictionaryKey, true, true, shift, 100L, modulus, false, true);
  }

  @ParameterizedTest
  @CsvSource({"0,false,false", "0,false,true", "0,true,false", "0,true,true", "1,false,false", "1,false,true",
      "1,true,false", "1,true,true"})
  void blockCountsMatchRowOracleAcrossMasksDictionariesAndTableGrowth(final int dictionaryKey, final boolean substitute,
      final boolean dense) {
    final Random random = new Random(0xB1C0L);
    final List<List<Row>> blocks = new ArrayList<>();
    for (int block = 0; block < 5; block++) {
      final List<Row> rows = new ArrayList<>();
      // Missing string precedes the stored empty string; fn:string must preserve its ordinal.
      rows.add(new Row(null, 200L, true));
      rows.add(new Row("", 201L, true));
      for (int row = 2; row < 67 + block * 64; row++) {
        final int id = random.nextInt(40);
        rows.add(new Row(id == 0
            ? null
            : id == 1
                ? ""
                : "β-" + id,
            200L + random.nextInt(100), random.nextInt(5) != 0));
      }
      blocks.add(rows);
    }
    assertScan(blocks, dictionaryKey, substitute, dense, 0L, 100L, 24L, false);
  }

  @Test
  void equalResiduesAcrossACycleDoNotProveAConstantBucket() {
    assertScan(List.of(List.of(new Row("a", 1L, true), new Row("a", 2L, true), new Row("a", 25L, true))), 0, false,
        true, 0L, 1L, 24L, false);
  }

  @Test
  void emptyDictionariesAndEntirelyMissingNumericBlocksPreserveMissingGroups() {
    final List<List<Row>> blocks =
        List.of(List.of(new Row(null, 200L, true), new Row(null, 299L, true), new Row(null, 201L, false)),
            List.of(new Row(null, null, true), new Row("", null, true), new Row("a", null, false)),
            List.of(new Row("", 200L, true), new Row(null, 201L, true)));
    for (final boolean substitute : new boolean[] {false, true}) {
      assertScan(blocks, 0, substitute, false, 0L, 100L, 24L, false);
      assertScan(blocks, 1, substitute, true, 0L, 100L, 24L, false);
    }
  }

  @Test
  void signedDivisionShiftAndMissingNumericKeysMatchRowSemantics() {
    final List<List<Row>> blocks =
        List.of(List.of(new Row("a", -2L, true), new Row("a", 2L, true), new Row("b", 0L, true)),
            List.of(new Row("a", -7L, true), new Row("b", -6L, true)),
            List.of(new Row("a", null, true), new Row(null, 1L, true), new Row("b", 2L, true)),
            List.of(new Row("a", 2L, false), new Row("b", 100L, false)),
            List.of(new Row("a", 2L, true), new Row("a", 3L, true)));
    for (final long shift : new long[] {0L, 5L, -7L}) {
      assertScan(blocks, 1, true, true, shift, 3L, 2L, false);
    }
  }

  @Test
  void overflowOutsideSelectionFallsBackAndSelectedOverflowDeclines() {
    assertScan(List.of(List.of(new Row("a", 0L, true), new Row("b", 1L, false))), 0, false, false, Long.MAX_VALUE, 1L,
        0L, false);
    assertScan(List.of(List.of(new Row("a", 0L, true), new Row("b", 1L, true))), 0, false, false, Long.MAX_VALUE, 1L,
        0L, true);
    assertScan(List.of(List.of(new Row("a", 0L, true), new Row("b", -1L, true))), 0, false, true, Long.MIN_VALUE, 0L,
        0L, true);
  }

  private static void assertScan(final List<List<Row>> blocks, final int dictionaryKey, final boolean substitute,
      final boolean dense, final long shift, final long divisor, final long modulus, final boolean expectDecline) {
    assertScan(blocks, dictionaryKey, substitute, dense, shift, divisor, modulus, expectDecline, false);
    if (divisor > 0) {
      assertScan(blocks, dictionaryKey, substitute, dense, shift, divisor, modulus, expectDecline, true);
    }
  }

  private static void assertScan(final List<List<Row>> blocks, final int dictionaryKey, final boolean substitute,
      final boolean dense, final long shift, final long divisor, final long modulus, final boolean expectDecline,
      final boolean bucketView) {
    final List<RowGroupDirectory> directories = new ArrayList<>();
    final List<byte[]> payloads = new ArrayList<>();
    for (int block = 0; block < blocks.size(); block++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {DICT, LONG, LONG});
      final List<Row> rows = blocks.get(block);
      for (int row = 0; row < rows.size(); row++) {
        final Row value = rows.get(row);
        assertTrue(page.appendRow(1L + ((long) block << 20) + row, new long[] {0L, value.number() == null
            ? 0L
            : value.number(),
            value.selected()
                ? 1L
                : 0L},
            new boolean[3], new String[] {value.text() == null
                ? ""
                : value.text(), null, null},
            new boolean[] {value.text() != null, value.number() != null, true}, new boolean[3], new boolean[3]));
      }
      final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final long[] offsets = new long[encoded.segments().length];
      for (int segment = 0; segment < offsets.length; segment++) {
        payloads.add(encoded.segments()[segment]);
        offsets[segment] = payloads.size();
      }
      directories.add(new RowGroupDirectory(block + 1L, encoded.descriptor(), encoded.columnSegmentIds(), offsets,
          new byte[offsets.length][]));
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    final ColumnSlice[][] columns = new ColumnSlice[3][];
    final ColumnSegmentFetcher fetcher = offsets -> {
      final byte[][] segments = new byte[offsets.length][];
      for (int i = 0; i < offsets.length; i++) {
        segments[i] = payloads.get(Math.toIntExact(offsets[i] - 1));
      }
      return segments;
    };
    for (int column = 0; column < columns.length; column++) {
      columns[column] = bucketView && column == 1
          ? store.numericBucketKeyColumn(column, fetcher, shift, divisor, modulus)
          : store.column(column, fetcher);
    }
    if (bucketView) {
      assertFalse(store.columnFilled(1), "representatives must not become the ordinary column cache");
      final ColumnSlice[] originals = store.column(1, fetcher);
      for (int block = 0; block < blocks.size(); block++) {
        for (int row = 0; row < blocks.get(block).size(); row++) {
          final Long number = blocks.get(block).get(row).number();
          if (number != null) {
            assertEquals(number.longValue(), originals[block].numericValues()[row],
                "ordinary reads must still see exact source values");
          }
        }
      }
    }
    final int numericKey = 1 - dictionaryKey;
    final ColumnSlice[][] keys = new ColumnSlice[2][];
    keys[dictionaryKey] = columns[0];
    keys[numericKey] = columns[1];
    final byte[] kinds = new byte[2];
    kinds[dictionaryKey] = DICT;
    kinds[numericKey] = LONG;
    final int[] lanes = CompositeGroupIdentity.laneOffsets(kinds, null);
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 1, true, 0L, lanes[2]);
    if (dense) {
      table.useDenseIndex();
    }
    final long[] shifts = new long[2];
    shifts[numericKey] = shift;
    final long[] divMod = new long[4];
    divMod[2 * numericKey] = divisor;
    divMod[2 * numericKey + 1] = modulus;
    final byte[][] literals = new byte[2][];
    literals[dictionaryKey] = new byte[0];
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(2);
    final long[] decline = new long[1];
    ProjectionColumnGroupScan.aggregateByGroupCompositeFlat(store, SELECTED, new ColumnSlice[][] {columns[2]}, null,
        null, keys, kinds, new ColumnSlice[0][], 0, blocks.size(), table, -1, null, null, shifts, null, decline,
        substitute
            ? new int[] {-1, -1, -1, -1}
            : null,
        null, null, substitute
            ? literals
            : null,
        divMod, null, registry, null);
    assertTrue(registry.identityProven());
    assertEquals(expectDecline
        ? 1L
        : 0L, decline[0]);
    if (expectDecline) {
      return;
    }
    final Map<Key, long[]> expected = new LinkedHashMap<>();
    for (int block = 0; block < blocks.size(); block++) {
      final List<Row> rows = blocks.get(block);
      for (int row = 0; row < rows.size(); row++) {
        final Row value = rows.get(row);
        if (!value.selected()) {
          continue;
        }
        Long bucket = value.number();
        if (bucket != null) {
          bucket = Math.addExact(bucket, shift);
          if (divisor > 0) {
            bucket /= divisor;
          }
          if (modulus > 0) {
            bucket %= modulus;
          }
        }
        final Key key = new Key(substitute && value.text() == null
            ? ""
            : value.text(), bucket);
        final long first = (long) block << 20 | row;
        final long[] accumulator = expected.computeIfAbsent(key, ignored -> new long[] {0L, first});
        accumulator[0]++;
      }
    }
    assertEquals(expected.size(), table.size());
    for (final Map.Entry<Key, long[]> entry : expected.entrySet()) {
      final Key key = entry.getKey();
      final long[] identity = new long[lanes[2]];
      final long[] hashes = new long[2];
      if (key.text() == null) {
        identity[0] |= 1L << dictionaryKey;
        hashes[dictionaryKey] = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
      } else {
        final byte[] bytes = key.text().getBytes(StandardCharsets.UTF_8);
        hashes[dictionaryKey] = ProjectionIndexByteScan.fnv1a64(bytes, 0, bytes.length);
        identity[lanes[dictionaryKey]] = hashes[dictionaryKey];
        identity[lanes[dictionaryKey] + 1] = GlobalValueDictionary.secondaryValueHash(bytes, 0, bytes.length);
      }
      if (key.bucket() == null) {
        identity[0] |= 1L << numericKey;
        hashes[numericKey] = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
      } else {
        identity[lanes[numericKey]] = key.bucket();
        hashes[numericKey] = HashCommon.mix(key.bucket());
      }
      long hash = ProjectionIndexByteScan.FNV_SEED;
      for (final long component : hashes) {
        hash = hash * ProjectionIndexByteScan.FNV_PRIME ^ component;
      }
      final int handle = table.acquireExact(hash, Long.MAX_VALUE, identity, 0);
      final long[] storage = table.storageAtAccBase(handle);
      final int base = table.offsetAtAccBase(handle);
      assertEquals(entry.getValue()[0], storage[base], "count for " + key);
      assertEquals(entry.getValue()[1], storage[base + 1], "first selected row for " + key);
      assertEquals(entry.getValue()[1], table.auxAtAccBase(handle), "winner reference for " + key);
    }
    assertEquals(expected.size(), table.size(), "all expected groups already existed");
  }
}
