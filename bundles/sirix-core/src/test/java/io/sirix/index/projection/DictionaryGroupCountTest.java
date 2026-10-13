package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.GlobalValueDictionary.ReadView;
import org.junit.jupiter.api.Test;

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
import static org.mockito.Mockito.mock;

final class DictionaryGroupCountTest {
  private static final byte DICT = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  private static final byte LONG = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  private static final byte DATE = ProjectionIndexRowGroupPage.COLUMN_KIND_DATE;
  private static final byte GLOBAL = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
  private static final ColumnPredicate[] SELECTED = {ColumnPredicate.numeric(2, ProjectionIndexScan.Op.GT, 0)};

  private record Key(String string, Long number, Long date, Long global) {
  }

  private record Fixture(List<byte[]> leaves, Map<Key, long[]> composite, Map<String, long[]> strings) {
  }

  private static Fixture fixture() {
    final List<byte[]> leaves = new ArrayList<>();
    final Map<Key, long[]> composite = new LinkedHashMap<>();
    final Map<String, long[]> strings = new LinkedHashMap<>();
    final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
    for (int id = 1; id <= 5; id++) {
      assertEquals(id, dictionary.intern("global-" + id));
    }
    final int[] sizes = {129, 1, 65, 511};
    for (int leaf = 0; leaf < sizes.length; leaf++) {
      final ProjectionIndexRowGroupPage page =
          new ProjectionIndexRowGroupPage(new byte[] {DICT, LONG, LONG, DATE, GLOBAL});
      page.setGlobalDictionaries(new GlobalValueDictionaryWriter[] {null, null, null, null, dictionary});
      for (int row = 0; row < sizes[leaf]; row++) {
        final String string = row % 17 == 0
            ? ""
            : "β-" + (row + leaf * 29) % 131;
        final long number = row % 19 == 0
            ? Long.MIN_VALUE
            : row % 11 - 5;
        final long date = row % 7 - 3;
        final long global = row % 5 + 1;
        final boolean[] present = {row % 13 != 0, row % 23 != 0, true, row % 29 != 0, row % 31 != 0};
        final long selected = row % 3;
        assertTrue(
            page.appendRow(leaf * 1000L + row + 1, new long[] {0, number, selected, date, global}, new boolean[5],
                new String[] {string, null, null, null, "global-" + global}, present, new boolean[5], new boolean[5]));
        if (selected != 0) {
          final String stringKey = present[0]
              ? string
              : null;
          final Key key = new Key(stringKey, present[1]
              ? number
              : null,
              present[3]
                  ? date
                  : null,
              present[4]
                  ? global
                  : null);
          final long ordinal = (long) (leaf + 9) << 20 | row;
          composite.computeIfAbsent(key, ignored -> new long[] {0, ordinal})[0]++;
          strings.computeIfAbsent(stringKey, ignored -> new long[] {0, ordinal})[0]++;
        }
      }
      leaves.add(page.serialize());
    }
    return new Fixture(leaves, composite, strings);
  }

  @Test
  void dictionaryCountsPreserveMissingKeysFirstOrdinalsAndScratchOwnership() {
    final Fixture fixture = fixture();
    for (final boolean dense : new boolean[] {false, true}) {
      // Alternate count-only and general folds on the same thread: they reuse dictionary scratch.
      for (final int aggregates : new int[] {0, 1, 0}) {
        final NumericGroupAggTable table = new NumericGroupAggTable(aggregates, 16, true);
        if (dense) {
          table.useDenseIndex();
        }
        final long[] missing = new long[2 + 4 * aggregates];
        ProjectionIndexByteScan.conjunctiveAggregateByGroupStringFlat(fixture.leaves(), SELECTED, 0, aggregates == 0
            ? new int[0]
            : new int[] {2}, table, missing, 9);
        assertEquals(fixture.strings().size() - 1, table.sizeIncludingZero());
        assertEquals(fixture.strings().get(null)[0], missing[0]);
        assertEquals(fixture.strings().get(null)[1], missing[1]);
        for (final Map.Entry<String, long[]> entry : fixture.strings().entrySet()) {
          if (entry.getKey() == null) {
            continue;
          }
          final byte[] bytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
          final int handle = table.handleOfProbeKey(ProjectionIndexByteScan.utf8Hash(bytes));
          final long[] storage = table.storageAtAccBase(handle);
          final int offset = table.offsetAtAccBase(handle);
          assertEquals(entry.getValue()[0], storage[offset]);
          assertEquals(entry.getValue()[1], storage[offset + 1]);
        }
      }
    }
  }

  @Test
  void compositeCountsMatchTheRowOracleThroughGrowthMissingKindsAndPasses() {
    final Fixture fixture = fixture();
    for (final boolean dense : new boolean[] {false, true}) {
      for (final boolean general : new boolean[] {false, true}) {
        final NumericGroupAggTable all = table(dense);
        scan(fixture.leaves(), all, new ProjectionStringIdentityRegistry(4), general);
        assertComposite(fixture.composite(), all);
        final NumericGroupAggTable merged = table(dense);
        for (int partition = 0; partition < 2; partition++) {
          final NumericGroupAggTable part = table(dense);
          part.setPassRange(63, partition, partition + 1);
          scan(fixture.leaves(), part, new ProjectionStringIdentityRegistry(4), general);
          final int offset = part.offsetAtAccBase(NumericGroupAggTable.DISCARD_HANDLE);
          assertArrayEquals(new long[part.slotWidth()], Arrays.copyOfRange(
              part.storageAtAccBase(NumericGroupAggTable.DISCARD_HANDLE), offset, offset + part.slotWidth()));
          NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {part}, 0, 64, merged);
        }
        assertComposite(fixture.composite(), merged);
      }
    }
  }

  @Test
  void compositeIdentityProofStillDeclinesForCollisionsAndBudgetExhaustionBeforeDiscarding() {
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
    final Fixture fixture = fixture();
    for (final boolean eager : new boolean[] {false, true}) {
      for (final boolean exhausted : new boolean[] {false, true}) {
        final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(4, exhausted
            ? ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT
            : collision,
            exhausted
                ? 1
                : 1 << 20);
        registry.setProveEveryEntry(eager);
        final NumericGroupAggTable out = table(true);
        out.setPassRange(63, 2, 3); // Both real partitions are discarded; proof must still run.
        scan(fixture.leaves(), out, registry, false);
        assertFalse(registry.identityProven());
        assertEquals(exhausted, registry.unproven());
        assertEquals(!exhausted, registry.collisionDetected());
        assertEquals(0, out.size());
      }
    }
  }

  private static NumericGroupAggTable table(final boolean dense) {
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, 0L, 6);
    return dense
        ? table.useDenseIndex()
        : table;
  }

  private static void scan(final List<byte[]> leaves, final NumericGroupAggTable out,
      final ProjectionStringIdentityRegistry registry, final boolean general) {
    ProjectionIndexByteScan.conjunctiveAggregateByGroupCompositeFlat(leaves, SELECTED, new int[] {0, 1, 3, 4},
        new int[0], out, 9, -1, null, null, general
            ? new long[4]
            : null,
        null, new long[1], null, null, null, null, null, new ReadView[] {null, null, null, mock(ReadView.class)},
        registry, null);
  }

  private static void assertComposite(final Map<Key, long[]> expected, final NumericGroupAggTable out) {
    assertEquals(expected.size(), out.size());
    for (final Map.Entry<Key, long[]> entry : expected.entrySet()) {
      final Key key = entry.getKey();
      final long[] identity = new long[6];
      long hash = ProjectionIndexByteScan.FNV_SEED;
      if (key.string() == null) {
        identity[0] |= 1L;
        hash = fold(hash, ProjectionIndexByteScan.MISSING_COMPONENT_HASH);
      } else {
        final byte[] bytes = key.string().getBytes(StandardCharsets.UTF_8);
        identity[1] = ProjectionIndexByteScan.utf8Hash(bytes);
        identity[2] = GlobalValueDictionary.secondaryValueHash(bytes, 0, bytes.length);
        hash = fold(hash, identity[1]);
      }
      final Long[] values = {key.number(), key.date(), key.global()};
      for (int k = 0; k < values.length; k++) {
        if (values[k] == null) {
          identity[0] |= 1L << (k + 1);
          hash = fold(hash, ProjectionIndexByteScan.MISSING_COMPONENT_HASH);
        } else {
          identity[k + 3] = values[k];
          hash = fold(hash, mix(values[k]));
        }
      }
      final int size = out.size();
      final int handle = out.acquireExact(hash, Long.MAX_VALUE, identity, 0);
      assertEquals(size, out.size(), "the independently computed identity must already exist");
      final long[] storage = out.storageAtAccBase(handle);
      final int offset = out.offsetAtAccBase(handle);
      assertEquals(entry.getValue()[0], storage[offset]);
      assertEquals(entry.getValue()[1], storage[offset + 1]);
      assertEquals(entry.getValue()[1], out.auxAtAccBase(handle));
    }
  }

  private static long mix(final long value) {
    // Independent spelling of fastutil's reversible long mix.
    long h = value * 0x9E3779B97F4A7C15L;
    h ^= h >>> 32;
    return h ^ h >>> 16;
  }

  private static long fold(final long hash, final long component) {
    return hash * ProjectionIndexByteScan.FNV_PRIME ^ component;
  }
}
