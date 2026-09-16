package io.sirix.index.projection;

import io.sirix.index.projection.GlobalValueDictionary.ReadView;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

final class LongLaneCompositeCountTest {
  private static final byte LONG = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  private static final String OPTION = "sirix.projection.longLaneCompositeCounts";

  @ParameterizedTest
  @CsvSource({"1,false,false", "1,true,true", "2,false,true", "2,true,false", "3,false,false", "3,true,true",
      "8,false,true", "8,true,false", "64,false,false", "64,true,true"})
  void countsMatchRowOracleAcrossMasksMissingValuesGrowthAndPartitions(final int width, final boolean dense,
      final boolean tree) {
    final int[] columns = new int[width];
    final byte[] kinds = new byte[width + 1];
    Arrays.fill(kinds, LONG);
    for (int k = 0; k < width; k++) {
      columns[k] = k;
      kinds[k] = switch (k % 3) {
        case 1 -> ProjectionIndexRowGroupPage.COLUMN_KIND_DATE;
        case 2 -> ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP;
        default -> LONG;
      };
    }
    final List<byte[]> payloads = new ArrayList<>();
    final Map<List<Long>, long[]> expected = new LinkedHashMap<>();
    for (int leaf = 0; leaf < 4; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
      final int rows = leaf == 2
          ? 1
          : 65 + leaf * 67;
      for (int row = 0; row < rows; row++) {
        final long[] values = new long[kinds.length];
        final boolean[] present = new boolean[kinds.length];
        final List<Long> key = new ArrayList<>(width);
        for (int k = 0; k < width; k++) {
          values[k] = row % 19 == 0
              ? Long.MIN_VALUE
              : row % 23 == 0
                  ? Long.MAX_VALUE
                  : (row * (k + 3L)) % 47 - 23;
          present[k] = (row + k) % 13 != 0;
          key.add(present[k]
              ? values[k]
              : null);
        }
        values[width] = leaf == 2
            ? 0
            : row % 3;
        present[width] = true;
        assertTrue(page.appendRow((long) leaf * 1000 + row + 1, values, new boolean[kinds.length],
            new String[kinds.length], present, new boolean[kinds.length], new boolean[kinds.length]));
        if (values[width] > 0) {
          final long ordinal = (long) (7 + leaf) << 20 | row;
          final long[] aggregate = expected.computeIfAbsent(key, ignored -> new long[] {0L, ordinal});
          aggregate[0]++;
        }
      }
      payloads.add(page.serialize());
    }
    final ColumnPredicate[] predicates = {ColumnPredicate.numeric(width, ProjectionIndexScan.Op.GT, 0)};
    final PredicateTree program = tree
        ? PredicateTree.of(
            new ColumnPredicate[] {ColumnPredicate.numeric(width, ProjectionIndexScan.Op.EQ, 1),
                ColumnPredicate.numeric(width, ProjectionIndexScan.Op.EQ, 2),
                ColumnPredicate.numeric(width, ProjectionIndexScan.Op.EQ, 0)},
            new byte[] {0, 1, PredicateTree.OP_OR, 2, PredicateTree.OP_NOT, PredicateTree.OP_AND})
        : null;
    for (final boolean enabled : new boolean[] {false, true}) {
      withOption(enabled, () -> {
        final NumericGroupAggTable all = table(width, dense);
        scan(payloads, columns, predicates, program, all, null, null, null, null);
        assertExpected(all, expected);
        final NumericGroupAggTable merged = table(width, dense);
        for (int partition = 0; partition < 2; partition++) {
          final NumericGroupAggTable part = table(width, dense);
          part.setPassRange(63, partition, partition + 1);
          scan(payloads, columns, predicates, program, part, null, null, null, null);
          final int discard = NumericGroupAggTable.DISCARD_HANDLE;
          final int base = part.offsetAtAccBase(discard);
          assertArrayEquals(new long[part.slotWidth()],
              Arrays.copyOfRange(part.storageAtAccBase(discard), base, base + part.slotWidth()));
          NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {part}, 0, 64, merged);
        }
        assertExpected(merged, expected);
      });
    }
  }

  @Test
  void collidingProbeHashesRemainDistinctInTheCountKernel() {
    final long firstHash = fold(fold(ProjectionIndexByteScan.FNV_SEED, 11), 29);
    final long second =
        HashCommon.invMix(fold(ProjectionIndexByteScan.FNV_SEED, 42) * ProjectionIndexByteScan.FNV_PRIME ^ firstHash);
    assertEquals(firstHash, fold(fold(ProjectionIndexByteScan.FNV_SEED, 42), second));
    final byte[] page = page(LONG, new long[][] {{11, 29}, {42, second}, {11, 29}});
    for (final boolean enabled : new boolean[] {false, true}) {
      withOption(enabled, () -> {
        final NumericGroupAggTable out = table(2, true);
        scan(List.of(page), new int[] {0, 1}, new ColumnPredicate[0], null, out, null, null, null, null);
        final Map<List<Long>, long[]> expected = new LinkedHashMap<>();
        expected.put(List.of(11L, 29L), new long[] {2, 7L << 20});
        expected.put(List.of(42L, second), new long[] {1, (7L << 20) | 1});
        assertExpected(out, expected);
      });
    }
  }

  @Test
  void kindChangesWidthErrorsAndStopFlagsRemainVisible() {
    final byte[] numeric = page(LONG, new long[][] {{1}});
    final byte[] temporal = page(ProjectionIndexRowGroupPage.COLUMN_KIND_DATE, new long[][] {{1}});
    for (final boolean enabled : new boolean[] {false, true}) {
      withOption(enabled, () -> {
        assertThrows(IllegalStateException.class, () -> scan(List.of(numeric, temporal), new int[] {0},
            new ColumnPredicate[0], null, table(1, false), null, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> scan(List.of(numeric), new int[] {0}, new ColumnPredicate[0],
            null, table(2, false), null, null, null, null));
        final NumericGroupAggTable stopped = table(1, false);
        scan(List.of(numeric), new int[] {0}, new ColumnPredicate[0], null, stopped, new long[] {0, 1}, null, null,
            null);
        scan(List.of(numeric), new int[] {0}, new ColumnPredicate[0], null, stopped, null, new long[] {1}, null, null);
        assertEquals(0, stopped.size());
      });
    }
  }

  @Test
  void transformsStillUseTheirCheckedSemantics() {
    withOption(true, () -> {
      final NumericGroupAggTable divided = table(1, false);
      scan(List.of(page(LONG, new long[][] {{0}, {1}})), new int[] {0}, new ColumnPredicate[0], null, divided, null,
          new long[1], null, new long[] {2, 0});
      assertExpected(divided, Map.of(List.of(0L), new long[] {2, 7L << 20}));
      final long[] decline = new long[1];
      scan(List.of(page(LONG, new long[][] {{Long.MAX_VALUE}})), new int[] {0}, new ColumnPredicate[0], null,
          table(1, false), null, decline, new long[] {1}, null);
      assertEquals(1, decline[0]);
    });
  }

  private record Tuple(long missing, long numeric, long conditional, long global, long date) {
  }

  private record GlobalFixture(List<byte[]> payloads, int[] conditions, long[] literals, byte[][] elseBytes,
      long[] elseIds, ReadView[] views, Map<Tuple, long[]> expected) {
  }

  @ParameterizedTest
  @CsvSource({"false,0", "true,0", "false,1", "true,1", "false,-2", "true,-2"})
  void conditionalGlobalIdsPreserveExactCountsMissingKindsAndFirstSeen(final boolean dense, final long elseId) {
    final GlobalFixture fixture = globalFixture(elseId, dense);
    for (final boolean enabled : new boolean[] {false, true}) {
      withOption(enabled, () -> {
        final NumericGroupAggTable all = table(4, dense);
        scanGlobal(fixture, all);
        assertGlobalExpected(fixture, all);
        final NumericGroupAggTable merged = table(4, dense);
        for (int partition = 0; partition < 2; partition++) {
          final NumericGroupAggTable part = table(4, dense);
          part.setPassRange(63, partition, partition + 1);
          scanGlobal(fixture, part);
          final int discard = NumericGroupAggTable.DISCARD_HANDLE;
          final int base = part.offsetAtAccBase(discard);
          assertArrayEquals(new long[part.slotWidth()],
              Arrays.copyOfRange(part.storageAtAccBase(discard), base, base + part.slotWidth()));
          NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {part}, 0, 64, merged);
        }
        assertGlobalExpected(fixture, merged);
      });
    }
  }

  @Test
  void globalViewLiteralAndConditionKindValidationRemainMandatory() {
    for (final boolean enabled : new boolean[] {false, true}) {
      withOption(enabled, () -> {
        final GlobalFixture noView = globalFixture(1, false);
        noView.views()[1] = null;
        assertThrows(IllegalStateException.class, () -> scanGlobal(noView, table(4, false)));
        final GlobalFixture noLiteral = globalFixture(1, false);
        noLiteral.elseIds()[1] = Long.MIN_VALUE;
        assertThrows(IllegalStateException.class, () -> scanGlobal(noLiteral, table(4, false)));
        final GlobalFixture wrongCondition = globalFixture(1, false);
        // A date has the same raw lane width, but is not a numeric condition operand.
        wrongCondition.payloads().get(0)[24 + 4] = ProjectionIndexRowGroupPage.COLUMN_KIND_DATE;
        assertThrows(IllegalStateException.class, () -> scanGlobal(wrongCondition, table(4, false)));
      });
    }
  }

  private static GlobalFixture globalFixture(final long elseId, final boolean substituteMissing) {
    final byte global = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
    final byte[] kinds = {LONG, global, global, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE, LONG, LONG, LONG};
    final int[] conditions = {-1, -1, 4, 5, -1, -1, -1, -1};
    final long[] literals = {0, 0, 2, 3, 0, 0, 0, 0};
    final byte[][] elseBytes = new byte[4][];
    final long[] elseIds = {Long.MIN_VALUE, elseId, 1, Long.MIN_VALUE};
    if (elseId != 0) {
      elseBytes[1] = (elseId == 1
          ? ""
          : "unstored").getBytes(StandardCharsets.UTF_8);
    }
    if (substituteMissing) {
      elseBytes[2] = new byte[0];
    }
    // The kernel checks readability but groups on already-resolved exact ids, without fetching text.
    final ReadView view = mock(ReadView.class);
    final ReadView[] views = {null, view, view, null};
    final List<byte[]> payloads = new ArrayList<>();
    final Map<Tuple, long[]> expected = new LinkedHashMap<>();
    final GlobalValueDictionaryWriter conditionalDictionary = new GlobalValueDictionaryWriter();
    final GlobalValueDictionaryWriter plainDictionary = new GlobalValueDictionaryWriter();
    for (int id = 1; id <= 7; id++) {
      assertEquals(id, conditionalDictionary.intern(dictionaryValue(id)));
      assertEquals(id, plainDictionary.intern(dictionaryValue(id)));
    }
    for (int leaf = 0; leaf < 4; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
      page.setGlobalDictionaries(
          new GlobalValueDictionaryWriter[] {null, conditionalDictionary, plainDictionary, null, null, null, null});
      final int rows = leaf == 1
          ? 1
          : 65 + leaf * 37;
      for (int row = 0; row < rows; row++) {
        final long[] values = {row % 9 - 4, 1 + row % 7, 1 + row % 5, row % 3 - 1, row % 4, row % 5, row % 3};
        final boolean[] present =
            {row % 13 != 0, row % 11 != 0, row % 7 != 0, row % 17 != 0, row % 19 != 0, row % 23 != 0, true};
        if (leaf == 1)
          values[6] = 0;
        final String[] strings = {null, dictionaryValue(values[1]), dictionaryValue(values[2]), null, null, null, null};
        assertTrue(page.appendRow(leaf * 1000L + row + 1, values, new boolean[7], strings, present, new boolean[7],
            new boolean[7]));
        if (values[6] == 0)
          continue;
        final boolean useStored = present[4] && present[5] && values[4] == 2 && values[5] == 3;
        final long[] identity = new long[5];
        final long[] hashes = new long[4];
        for (int k = 0; k < 4; k++) {
          if (k == 1 && !useStored) {
            if (elseId == 0) {
              identity[0] |= 1L << k;
              identity[k + 1] = 1L;
              hashes[k] = 0L;
            } else {
              identity[k + 1] = elseId;
              hashes[k] = HashCommon.mix(elseId);
            }
          } else if (!present[k]) {
            if (k == 2 && substituteMissing) {
              identity[k + 1] = 1L;
              hashes[k] = HashCommon.mix(1L);
            } else {
              identity[0] |= 1L << k;
              identity[k + 1] = 0L;
              hashes[k] = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
            }
          } else {
            identity[k + 1] = values[k];
            hashes[k] = HashCommon.mix(values[k]);
          }
        }
        long hash = ProjectionIndexByteScan.FNV_SEED;
        for (final long component : hashes)
          hash = hash * ProjectionIndexByteScan.FNV_PRIME ^ component;
        final Tuple key = new Tuple(identity[0], identity[1], identity[2], identity[3], identity[4]);
        long[] aggregate = expected.get(key);
        if (aggregate == null) {
          aggregate = new long[] {hash, 0L, (long) (7 + leaf) << 20 | row};
          expected.put(key, aggregate);
        }
        aggregate[1]++;
      }
      payloads.add(page.serialize());
    }
    return new GlobalFixture(payloads, conditions, literals, elseBytes, elseIds, views, expected);
  }

  private static void scanGlobal(final GlobalFixture fixture, final NumericGroupAggTable out) {
    final ColumnPredicate[] predicates = {ColumnPredicate.numeric(6, ProjectionIndexScan.Op.GT, 0L)};
    ProjectionIndexByteScan.conjunctiveAggregateByGroupCompositeFlat(fixture.payloads(), predicates,
        new int[] {0, 1, 2, 3}, new int[0], out, 7, -1, null, null, new long[4], new int[8], new long[1], null,
        fixture.conditions(), fixture.literals(), fixture.elseBytes(), new long[8], fixture.views(), null,
        fixture.elseIds());
  }

  private static String dictionaryValue(final long id) {
    return id == 1
        ? ""
        : "value-" + id;
  }

  private static void assertGlobalExpected(final GlobalFixture fixture, final NumericGroupAggTable out) {
    assertEquals(fixture.expected().size(), out.size());
    for (final Map.Entry<Tuple, long[]> entry : fixture.expected().entrySet()) {
      final Tuple key = entry.getKey();
      final long[] expected = entry.getValue();
      final long[] identity = {key.missing(), key.numeric(), key.conditional(), key.global(), key.date()};
      final int handle = out.acquireExact(expected[0], Long.MAX_VALUE, identity, 0);
      assertEquals(fixture.expected().size(), out.size(), "canonical group and its probe hash must already exist");
      final int base = out.offsetAtAccBase(handle);
      assertArrayEquals(new long[] {expected[1], expected[2]},
          Arrays.copyOfRange(out.storageAtAccBase(handle), base, base + 2));
      assertEquals(expected[2], out.auxAtAccBase(handle));
    }
  }

  private static byte[] page(final byte kind, final long[][] rows) {
    final int width = rows[0].length;
    final byte[] kinds = new byte[width];
    Arrays.fill(kinds, kind);
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final boolean[] present = new boolean[width];
    Arrays.fill(present, true);
    for (int row = 0; row < rows.length; row++) {
      assertTrue(page.appendRow(row + 1L, rows[row], new boolean[width], new String[width], present, new boolean[width],
          new boolean[width]));
    }
    return page.serialize();
  }

  private static NumericGroupAggTable table(final int width, final boolean dense) {
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, 0L, width + 1);
    return dense
        ? table.useDenseIndex()
        : table;
  }

  private static void scan(final List<byte[]> payloads, final int[] columns, final ColumnPredicate[] predicates,
      final PredicateTree tree, final NumericGroupAggTable out, final long[] budget, final long[] decline,
      final long[] offsets, final long[] divMod) {
    ProjectionIndexByteScan.conjunctiveAggregateByGroupCompositeFlat(payloads, predicates, columns, new int[0], out, 7,
        -1, null, budget, offsets, null, decline, tree, null, null, null, divMod, null, null, null);
  }

  private static void assertExpected(final NumericGroupAggTable out, final Map<List<Long>, long[]> expected) {
    assertEquals(expected.size(), out.size());
    for (final Map.Entry<List<Long>, long[]> entry : expected.entrySet()) {
      final long[] identity = new long[entry.getKey().size() + 1];
      long hash = ProjectionIndexByteScan.FNV_SEED;
      for (int k = 0; k < entry.getKey().size(); k++) {
        final Long value = entry.getKey().get(k);
        if (value == null) {
          identity[0] |= 1L << k;
          identity[k + 1] = 0L;
          hash = hash * ProjectionIndexByteScan.FNV_PRIME ^ ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
        } else {
          identity[k + 1] = value;
          hash = fold(hash, value);
        }
      }
      final int handle = out.acquireExact(hash, Long.MAX_VALUE, identity, 0);
      assertEquals(expected.size(), out.size(), "oracle identity must already exist");
      final int base = out.offsetAtAccBase(handle);
      assertArrayEquals(entry.getValue(), Arrays.copyOfRange(out.storageAtAccBase(handle), base, base + 2));
      assertEquals(entry.getValue()[1], out.auxAtAccBase(handle));
    }
  }

  private static long fold(final long hash, final long value) {
    return hash * ProjectionIndexByteScan.FNV_PRIME ^ HashCommon.mix(value);
  }

  private static void withOption(final boolean enabled, final Runnable check) {
    final String previous = System.setProperty(OPTION, Boolean.toString(enabled));
    try {
      check.run();
    } finally {
      if (previous == null) {
        System.clearProperty(OPTION);
      } else {
        System.setProperty(OPTION, previous);
      }
    }
  }
}
