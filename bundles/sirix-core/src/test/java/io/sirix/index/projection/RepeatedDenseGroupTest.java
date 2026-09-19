package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RepeatedDenseGroupTest {

  @Test
  void repeatedProbeStillSeparatesEveryIdentityLaneAndPreservesFirstSeen() {
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, 0L, 3).useDenseIndex();
    final long[] identity = {99L, 0L, 17L, 23L, 99L};
    final int first = table.acquireExact(7L, 13L, identity, 1);
    increment(table, first);
    assertEquals(first, table.acquireExact(7L, 14L, identity, 1));
    increment(table, first);
    for (int lane = 1; lane <= 3; lane++) {
      final long original = identity[lane];
      identity[lane] = original + 1L;
      final int distinct = table.acquireExact(7L, 20L + lane, identity, 1);
      assertNotEquals(first, distinct);
      assertEquals(distinct, table.acquireExact(7L, 30L + lane, identity, 1));
      increment(table, distinct);
      identity[lane] = original;
      assertEquals(first, table.acquireExact(7L, 40L + lane, identity, 1));
    }
    assertEquals(4, table.size());
    assertTrue(table.hasProbeKeyCollision());
    final long[] storage = table.storageAtAccBase(first);
    final int offset = table.offsetAtAccBase(first);
    assertEquals(2L, storage[offset]);
    assertEquals(13L, storage[offset + 1]);
  }

  @Test
  void zeroProbeAndItsSubstituteRemainDistinctOnRepeatedAcquisitions() {
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, false, 0L, 1).useDenseIndex();
    final long[] zeroIdentity = {0L};
    final long[] otherIdentity = {1L};
    final int zero = table.acquireExact(0L, 5L, zeroIdentity, 0);
    assertEquals(zero, table.acquireExact(0L, 6L, zeroIdentity, 0));
    final int other = table.acquireExact(0x9E3779B97F4A7C15L, 7L, otherIdentity, 0);
    assertNotEquals(zero, other);
    assertEquals(other, table.acquireExact(0x9E3779B97F4A7C15L, 8L, otherIdentity, 0));
    assertEquals(zero, table.acquireExact(0L, 9L, zeroIdentity, 0));
    assertEquals(2, table.size());
    assertTrue(table.hasProbeKeyCollision());
  }

  @Test
  void repeatedHandlesSurviveIndexAndStorageGrowthWithoutAliasingRecycledChunks() {
    final NumericGroupAggTable table = new NumericGroupAggTable(1, 16, true).useDenseIndex();
    final int groups = 40_000;
    for (int group = 1; group <= groups; group++) {
      final int handle = table.acquire(group, group);
      increment(table, handle);
      assertEquals(handle, table.acquire(group, Long.MAX_VALUE));
      increment(table, handle);
      table.setAuxAtAccBase(handle, ~((long) group));
    }
    assertTrue(table.rehashes() > 0);
    assertTrue(table.storageChunkCount() > 1);
    for (int group = groups; group >= 1; group--) {
      final int handle = table.acquire(group, 0L);
      assertEquals(handle, table.acquire(group, 0L));
      final long[] storage = table.storageAtAccBase(handle);
      final int offset = table.offsetAtAccBase(handle);
      assertEquals(2L, storage[offset]);
      assertEquals(group, storage[offset + 1]);
      assertEquals(~((long) group), table.auxAtAccBase(handle));
    }
    table.release();
    assertThrows(RuntimeException.class, () -> table.acquire(1L, 0L));
  }

  private static void increment(final NumericGroupAggTable table, final int handle) {
    table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)]++;
  }
}
