package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DictionaryCountBatchTest {
  private static final String OPTION = "sirix.projection.dictionaryCountBatches";
  private static final String PACKED = "sirix.projection.packedDictionaryCounts";
  private static final byte DICT = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  private static final byte LONG = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;

  private record Fixture(ProjectionColumnStore store, ColumnSlice[] groups, ColumnSlice[] filters,
      Map<String, long[]> expected) {
  }

  @ParameterizedTest
  @CsvSource({"1,false,false", "1,true,true", "2,false,true", "4,true,false", "16,false,false", "16,true,true",
      "257,false,true", "1024,true,false"})
  void exactCountsAndWinnerReferencesSurviveMasksMissingValuesGrowthAndLeafOrder(final int alphabet,
      final boolean denseTable, final boolean tree) {
    for (final boolean enabled : new boolean[] {false, true}) {
      for (final boolean packed : new boolean[] {false, true}) {
        withOptions(enabled, packed, () -> {
          final Fixture fixture = fixture(alphabet);
          // A pruned leaf need not retain its group column.
          fixture.groups()[2] = null;
          final NumericGroupAggTable out = new NumericGroupAggTable(0, 1, true);
          if (denseTable) {
            out.useDenseIndex();
          }
          final long[] missing = new long[2];
          // Reuse the same table across adjacent batches; scratch must reset per leaf and call.
          scan(fixture, tree, out, missing, 0, 3, null, null, null);
          scan(fixture, tree, out, missing, 3, fixture.groups().length, null, null, null);
          assertExpected(fixture, out, missing, true);
          if (enabled && packed) {
            for (final ColumnSlice group : fixture.groups()) {
              if (group != null && group.packedStringIds() != null) {
                assertFalse(group.packedStringIds().isMaterialized(), "COUNT must retain the packed ID stream");
              }
            }
          }
        });
      }
    }
  }

  @Test
  void independentlyAccumulatedLeafRangesMergeWithTheEarliestWinner() {
    for (final boolean enabled : new boolean[] {false, true}) {
      withOptions(enabled, true, () -> {
        final Fixture fixture = fixture(257);
        final NumericGroupAggTable merged = new NumericGroupAggTable(0, 1, true).useDenseIndex();
        final long[] missing = {0L, Long.MAX_VALUE};
        // Merge later leaves first to exercise source-ordinal selection during the merge.
        for (int part = 1; part >= 0; part--) {
          final NumericGroupAggTable table = new NumericGroupAggTable(0, 1, true).useDenseIndex();
          final long[] partMissing = {0L, Long.MAX_VALUE};
          scan(fixture, true, table, partMissing, part * 3, (part + 1) * 3, null, null, null);
          NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {table}, 0, 64, merged);
          missing[0] += partMissing[0];
          if (partMissing[0] != 0L) {
            missing[1] = Math.min(missing[1], partMissing[1]);
          }
        }
        assertExpected(fixture, merged, missing, false);
      });
    }
  }

  @Test
  void terminalBudgetAndRegexMissingKeyDeclinesRemainVisible() {
    for (final boolean enabled : new boolean[] {false, true}) {
      withOptions(enabled, true, () -> {
        final Fixture fixture = fixture(4);
        final NumericGroupAggTable stopped = new NumericGroupAggTable(0, 1, true);
        final long[] missing = new long[2];
        scan(fixture, false, stopped, missing, 0, fixture.groups().length, new long[] {0, 1}, null, null);
        assertEquals(0, stopped.size());
        assertEquals(0, missing[0]);
        final long[] decline = new long[1];
        scan(fixture, false, new NumericGroupAggTable(0, 1, true), new long[2], 0, 1, null, Pattern.compile("entry"),
            decline);
        assertEquals(1, decline[0], "regex over a missing key retains its existing decline");
      });
    }
  }

  private static void scan(final Fixture f, final boolean withTree, final NumericGroupAggTable out,
      final long[] missing, final int from, final int to, final long[] budget, final Pattern regex,
      final long[] decline) {
    final ColumnPredicate[] predicates = {ColumnPredicate.numeric(1, Op.EQ, 1)};
    final PredicateTree tree = withTree
        ? PredicateTree.of(new ColumnPredicate[] {predicates[0], ColumnPredicate.numeric(1, Op.EQ, 0)},
            new byte[] {0, 1, PredicateTree.OP_NOT, PredicateTree.OP_AND})
        : null;
    ProjectionColumnGroupScan.aggregateByGroupStringFlat(f.store(), predicates, new ColumnSlice[][] {f.filters()}, tree,
        withTree
            ? new ColumnSlice[][] {f.filters(), f.filters()}
            : null,
        f.groups(), new ColumnSlice[0][], null, from, to, out, missing, -1, null, null, budget, false, regex,
        regex == null
            ? null
            : "changed",
        decline, null, null, null, null);
  }

  private static Fixture fixture(final int alphabet) {
    final int[] sizes = {65, 129, 1, 1024, 257, 63};
    final List<RowGroupDirectory> directories = new ArrayList<>();
    final List<byte[]> segments = new ArrayList<>();
    final Map<String, long[]> expected = new LinkedHashMap<>();
    for (int leaf = 0; leaf < sizes.length; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {DICT, LONG});
      for (int row = 0; row < sizes[leaf]; row++) {
        final int id = (sizes[leaf] - row + leaf * 11) % alphabet;
        final String text = leaf == 0 || row % 13 == 0
            ? null
            : id == 0
                ? ""
                : "entry-β-" + id;
        final boolean selected = leaf != 2 && row % 4 != 0;
        assertTrue(page.appendRow(1L + ((long) leaf << 20) + row, new long[] {0, selected
            ? 1
            : 0}, new boolean[2], new String[] {
                text == null
                    ? ""
                    : text,
                null},
            new boolean[] {text != null, true}, new boolean[2], new boolean[2]));
        if (selected) {
          final long ordinal = (long) leaf << 20 | row;
          expected.computeIfAbsent(text, ignored -> new long[] {0, ordinal})[0]++;
        }
      }
      final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final long[] offsets = new long[encoded.segments().length];
      for (int i = 0; i < offsets.length; i++) {
        segments.add(encoded.segments()[i]);
        offsets[i] = segments.size();
      }
      directories.add(new RowGroupDirectory(leaf + 1L, encoded.descriptor(), encoded.columnSegmentIds(), offsets,
          new byte[offsets.length][]));
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    final ColumnSegmentFetcher fetcher = offsets -> {
      final byte[][] values = new byte[offsets.length][];
      for (int i = 0; i < offsets.length; i++) {
        values[i] = segments.get(Math.toIntExact(offsets[i] - 1));
      }
      return values;
    };
    return new Fixture(store, store.column(0, fetcher), store.column(1, fetcher), expected);
  }

  private static void assertExpected(final Fixture fixture, final NumericGroupAggTable out, final long[] missing,
      final boolean firstSourceLeaf) {
    assertEquals(fixture.expected().size() - 1, out.size());
    for (final Map.Entry<String, long[]> entry : fixture.expected().entrySet()) {
      final String text = entry.getKey();
      final long[] expected = entry.getValue();
      if (text == null) {
        assertEquals(expected[0], missing[0]);
        assertEquals(expected[1], missing[1]);
        continue;
      }
      final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
      final long hash = ProjectionIndexByteScan.fnv1a64(bytes, 0, bytes.length);
      assertTrue(hash != 0L, "this oracle uses ordinary dictionary values; zero has a separate table lane");
      final int handle = out.acquire(hash, Long.MAX_VALUE);
      final long[] values = out.storageAtAccBase(handle);
      final int base = out.offsetAtAccBase(handle);
      assertEquals(expected[0], values[base], text);
      assertEquals(expected[1], values[base + 1], "first selected row for " + text);
      final long source = out.auxAtAccBase(handle);
      assertEquals(text, fixture.groups()[(int) (source >>> 20)].dictString((int) (source & ((1 << 20) - 1))));
      // Merges retain any valid representative; firstSeen above is the independent ordering key.
      if (firstSourceLeaf) {
        assertEquals(expected[1] >>> 20, source >>> 20, "first source leaf for " + text);
      }
    }
    assertEquals(fixture.expected().size() - 1, out.size(), "looking up every expected group must not add groups");
  }

  private static void withOptions(final boolean enabled, final boolean packed, final Runnable action) {
    final String old = System.getProperty(OPTION);
    final String oldPacked = System.getProperty(PACKED);
    try {
      System.setProperty(OPTION, Boolean.toString(enabled));
      System.setProperty(PACKED, Boolean.toString(packed));
      action.run();
    } finally {
      restore(OPTION, old);
      restore(PACKED, oldPacked);
    }
  }

  private static void restore(final String name, final String previous) {
    if (previous == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, previous);
    }
  }
}
