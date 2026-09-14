package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.HashCommon;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NeutralGroupTransformTest {
  private static final byte DICT = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  private static final byte LONG = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  private static final ColumnPredicate[] SELECTED = {ColumnPredicate.numeric(1, ProjectionIndexScan.Op.GT, 0L)};

  private record Fixture(ProjectionColumnStore store, ColumnSlice[][] keys, ColumnSlice[][] predicates,
      List<String[]> rows) {
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void selectedCountsPreserveMissingSubstitutionAndFirstSeenAcrossDictionaries(final boolean substitute,
      final boolean dense) {
    final Fixture fixture = fixture(pattern(131, null, "", "β", "a"), pattern(137, "a", "β", "", null));
    for (final boolean ordinaryLoop : new boolean[] {false, true}) {
      final NumericGroupAggTable table = table(dense);
      final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1);
      scan(fixture, table, registry, substitute, ordinaryLoop);
      assertTrue(registry.identityProven());
      assertExpected(fixture, table, substitute);
    }
  }

  @Test
  void partitionOwnershipStillDiscardsWithoutFoldingAndMergeRestoresAllCounts() {
    final Fixture fixture = fixture(pattern(193, null, "", "β", "a", "c"));
    final NumericGroupAggTable merged = table(true);
    for (int partition = 0; partition < 2; partition++) {
      final NumericGroupAggTable part = table(true);
      part.setPassRange(63, partition, partition + 1);
      scan(fixture, part, new ProjectionStringIdentityRegistry(1), true, false);
      final int discard = NumericGroupAggTable.DISCARD_HANDLE;
      final int offset = part.offsetAtAccBase(discard);
      assertArrayEquals(new long[part.slotWidth()],
          Arrays.copyOfRange(part.storageAtAccBase(discard), offset, offset + part.slotWidth()));
      NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {part}, 0, 64, merged);
    }
    assertExpected(fixture, merged, true);
  }

  @Test
  void byteIdentityCollisionStillDeclinesForBothLazyAndEagerProof() {
    final ProjectionStringIdentityRegistry.Fingerprint collision = new ProjectionStringIdentityRegistry.Fingerprint() {
      @Override
      public long primary(final byte[] bytes, final int offset, final int length, final long fnv) {
        return 7L;
      }

      @Override
      public long secondary(final byte[] bytes, final int offset, final int length) {
        return 11L;
      }
    };
    final Fixture fixture = fixture(pattern(128, "aa", "bb"));
    for (final boolean eager : new boolean[] {false, true}) {
      final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, collision, 1 << 20);
      registry.setProveEveryEntry(eager);
      scan(fixture, table(true), registry, false, false);
      assertFalse(registry.identityProven(), "neutral annotations cannot weaken exact string identity");
    }
  }

  @Test
  void differentCardinalitiesAndAnEmptySelectedLeafPreserveAllGroups() {
    final String[] many = new String[320];
    for (int i = 0; i < many.length; i++) {
      many[i] = "key-" + i;
    }
    final Fixture fixture = fixture(pattern(129, "a", "b"), new String[] {"unused"}, many, pattern(130, "b", "a"));
    final NumericGroupAggTable table = table(true);
    scan(fixture, table, new ProjectionStringIdentityRegistry(1), false, false);
    assertExpected(fixture, table, false);
  }

  private static NumericGroupAggTable table(final boolean dense) {
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, 0L, 3);
    return dense
        ? table.useDenseIndex()
        : table;
  }

  @Test
  void divisionWithoutOtherAnnotationsStillChangesGroupingIdentity() {
    final Fixture fixture = fixture(pattern(14, "a"));
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, 0L, 2);
    final long[] decline = new long[1];
    scanNumeric(fixture, table, null, new long[] {2L, 0L}, decline);
    assertEquals(0L, decline[0]);
    assertEquals(1, table.size(), "the raw zero and one keys both divide to zero");
    assertNumericCount(table, 0L, 14L);
  }

  @Test
  void nonzeroOffsetsChangeKeysAndOverflowStillDeclines() {
    final Fixture fixture = fixture(pattern(14, "a"));
    final NumericGroupAggTable shifted = new NumericGroupAggTable(0, 16, true, 0L, 2);
    final long[] decline = new long[1];
    scanNumeric(fixture, shifted, new long[] {-3L}, null, decline);
    assertEquals(0L, decline[0]);
    assertEquals(2, shifted.size());
    assertNumericCount(shifted, -3L, 2L);
    assertNumericCount(shifted, -2L, 12L);

    final NumericGroupAggTable overflow = new NumericGroupAggTable(0, 16, true, 0L, 2);
    scanNumeric(fixture, overflow, new long[] {Long.MAX_VALUE}, null, decline);
    assertEquals(1L, decline[0], "a later overflowing key must invalidate the partial answer");
  }

  private static void scanNumeric(final Fixture fixture, final NumericGroupAggTable table, final long[] offsets,
      final long[] divMod, final long[] decline) {
    ProjectionColumnGroupScan.aggregateByGroupCompositeFlat(fixture.store(), new ColumnPredicate[0],
        new ColumnSlice[0][], null, null, fixture.predicates(), new byte[] {LONG}, new ColumnSlice[0][], 0,
        fixture.rows().size(), table, -1, null, null, offsets, null, decline, null, null, null, null, divMod, null,
        null, null);
  }

  private static void assertNumericCount(final NumericGroupAggTable table, final long key, final long count) {
    final long hash = ProjectionIndexByteScan.FNV_SEED * ProjectionIndexByteScan.FNV_PRIME ^ HashCommon.mix(key);
    final int size = table.size();
    final int handle = table.acquireExact(hash, Long.MAX_VALUE, new long[] {0L, key}, 0);
    assertEquals(size, table.size(), "the transformed identity must already exist");
    assertEquals(count, table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)]);
  }

  private static void scan(final Fixture fixture, final NumericGroupAggTable table,
      final ProjectionStringIdentityRegistry registry, final boolean substitute, final boolean ordinaryLoop) {
    ProjectionColumnGroupScan.aggregateByGroupCompositeFlat(fixture.store(), SELECTED, fixture.predicates(), null, null,
        fixture.keys(), new byte[] {DICT}, new ColumnSlice[0][], 0, fixture.rows().size(), table, -1, null, null,
        ordinaryLoop
            ? new long[] {1L}
            : new long[1],
        new int[2], new long[1], substitute
            ? new int[] {-1, -1}
            : null,
        null, null, substitute
            ? new byte[][] {new byte[0]}
            : null,
        null, null, registry, null);
  }

  private static void assertExpected(final Fixture fixture, final NumericGroupAggTable table,
      final boolean substitute) {
    final Map<String, long[]> expected = new LinkedHashMap<>();
    for (int leaf = 0; leaf < fixture.rows().size(); leaf++) {
      final String[] rows = fixture.rows().get(leaf);
      for (int row = 0; row < rows.length; row++) {
        if (row % 7 == 0) {
          continue;
        }
        final String key = substitute && rows[row] == null
            ? ""
            : rows[row];
        final long ordinal = (long) leaf << 20 | row;
        final long[] value = expected.computeIfAbsent(key, ignored -> new long[] {0L, ordinal});
        value[0]++;
      }
    }
    assertEquals(expected.size(), table.size());
    for (final Map.Entry<String, long[]> entry : expected.entrySet()) {
      final String key = entry.getKey();
      final byte[] bytes = key == null
          ? null
          : key.getBytes(StandardCharsets.UTF_8);
      final long component = key == null
          ? ProjectionIndexByteScan.MISSING_COMPONENT_HASH
          : ProjectionIndexByteScan.fnv1a64(bytes, 0, bytes.length);
      final long[] identity = key == null
          ? new long[] {1L, 0L, 0L}
          : new long[] {0L, component, GlobalValueDictionary.secondaryValueHash(bytes, 0, bytes.length)};
      long hash = ProjectionIndexByteScan.FNV_SEED;
      hash = hash * ProjectionIndexByteScan.FNV_PRIME ^ component;
      final int handle = table.acquireExact(hash, Long.MAX_VALUE, identity, 0);
      final long[] storage = table.storageAtAccBase(handle);
      final int offset = table.offsetAtAccBase(handle);
      assertEquals(entry.getValue()[0], storage[offset], "count for " + key);
      assertEquals(entry.getValue()[1], storage[offset + 1], "first selected ordinal for " + key);
      assertEquals(entry.getValue()[1], table.auxAtAccBase(handle), "winner ordinal for " + key);
    }
    assertEquals(expected.size(), table.size(), "all expected identities must already exist");
  }

  private static String[] pattern(final int size, final String... values) {
    final String[] rows = new String[size];
    for (int row = 0; row < size; row++) {
      rows[row] = values[row % values.length];
    }
    return rows;
  }

  private static Fixture fixture(final String[]... rowGroups) {
    final List<RowGroupDirectory> directories = new ArrayList<>();
    final List<byte[]> payloads = new ArrayList<>();
    for (int leaf = 0; leaf < rowGroups.length; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {DICT, LONG});
      for (int row = 0; row < rowGroups[leaf].length; row++) {
        final String key = rowGroups[leaf][row];
        assertTrue(page.appendRow(1L + ((long) leaf << 20) + row, new long[] {0L, row % 7 == 0
            ? 0L
            : 1L}, new boolean[2], new String[] {
                key == null
                    ? ""
                    : key,
                null},
            new boolean[] {key != null, true}, new boolean[2], new boolean[2]));
      }
      final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final long[] offsets = new long[encoded.segments().length];
      for (int i = 0; i < offsets.length; i++) {
        payloads.add(encoded.segments()[i]);
        offsets[i] = payloads.size();
      }
      directories.add(new RowGroupDirectory(leaf + 1L, encoded.descriptor(), encoded.columnSegmentIds(), offsets,
          new byte[offsets.length][]));
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    final ColumnSlice[][] columns = new ColumnSlice[2][];
    for (int column = 0; column < 2; column++) {
      columns[column] = store.column(column, offsets -> {
        final byte[][] segments = new byte[offsets.length][];
        for (int i = 0; i < offsets.length; i++) {
          segments[i] = payloads.get(Math.toIntExact(offsets[i] - 1));
        }
        return segments;
      });
    }
    return new Fixture(store, new ColumnSlice[][] {columns[0]}, new ColumnSlice[][] {columns[1]}, List.of(rowGroups));
  }
}
