package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NumericGroupLookupTest {

  @Test
  void cachedGroupsSurviveGrowthInBothTableLayouts() {
    for (final boolean dense : new boolean[] {false, true}) {
      final NumericGroupAggTable table = new NumericGroupAggTable(1, 16);
      if (dense) {
        table.useDenseIndex();
      }
      final NumericGroupLookup lookup = new NumericGroupLookup();
      lookup.beginLeaf(-100, 100, 1024, table);
      for (int round = 0; round < 3; round++) {
        for (int key = -100; key <= 100; key++) {
          if (key == 0) {
            continue;
          }
          increment(table, lookup.acquire(table, key, round * 1000L + key + 100));
        }
      }
      assertTrue(table.rehashes() > 0);
      assertEquals(200, table.size());
      for (int key = -100; key <= 100; key++) {
        if (key != 0) {
          assertGroup(table, key, 3, key + 100);
        }
      }
    }
  }

  @Test
  void outOfRangeInsertionsInvalidateCachedHandlesAndNewLeavesRebind() {
    final NumericGroupLookup lookup = new NumericGroupLookup();
    for (int leaf = 0; leaf < 2; leaf++) {
      final NumericGroupAggTable table = new NumericGroupAggTable(0, 16);
      lookup.beginLeaf(Long.MIN_VALUE, Long.MIN_VALUE + 8, 100, table);
      increment(table, lookup.acquire(table, Long.MIN_VALUE, 4));
      for (long key = 1; key <= 1000; key++) {
        increment(table, lookup.acquire(table, key, key));
      }
      increment(table, lookup.acquire(table, Long.MIN_VALUE, 1001));
      assertGroup(table, Long.MIN_VALUE, 2, 4);
      assertEquals(1001, table.size());
    }
  }

  @Test
  void wideEmptyAndOverflowingRangesKeepExactLongKeys() {
    final long[] keys = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1, 1, Long.MAX_VALUE - 1, Long.MAX_VALUE};
    final long[][] bounds = {{Long.MIN_VALUE, Long.MAX_VALUE}, {Long.MAX_VALUE, Long.MIN_VALUE}, {-100_000, 100_000},
        {Long.MAX_VALUE - 8, Long.MAX_VALUE}};
    final NumericGroupLookup lookup = new NumericGroupLookup();
    for (final long[] bound : bounds) {
      final NumericGroupAggTable table = new NumericGroupAggTable(0, 16);
      lookup.beginLeaf(bound[0], bound[1], 100, table);
      for (int round = 0; round < 2; round++) {
        for (int i = 0; i < keys.length; i++) {
          increment(table, lookup.acquire(table, keys[i], i));
        }
      }
      assertEquals(keys.length, table.size());
      for (int i = 0; i < keys.length; i++) {
        assertGroup(table, keys[i], 2, i);
      }
    }
  }

  @Test
  void passFilteringCannotAcquireAnExcludedGroupThroughTheCache() {
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16);
    table.setPassRange(62, 1, 2);
    final NumericGroupLookup lookup = new NumericGroupLookup();
    lookup.beginLeaf(1, 100, 1024, table);
    for (int round = 0; round < 2; round++) {
      for (long key = 1; key <= 100; key++) {
        final int expected = table.acquire(key, key);
        assertEquals(expected, lookup.acquire(table, key, key));
        increment(table, expected);
      }
    }
    assertTrue(table.size() > 0 && table.size() < 100);
  }

  private static void increment(final NumericGroupAggTable table, final int handle) {
    table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)]++;
  }

  private static void assertGroup(final NumericGroupAggTable table, final long key, final long count,
      final long firstSeen) {
    final int handle = table.acquire(key, Long.MAX_VALUE);
    final long[] storage = table.storageAtAccBase(handle);
    final int offset = table.offsetAtAccBase(handle);
    assertEquals(key, table.keyAtAccBase(handle));
    assertEquals(count, storage[offset]);
    assertEquals(firstSeen, storage[offset + 1]);
  }
}
