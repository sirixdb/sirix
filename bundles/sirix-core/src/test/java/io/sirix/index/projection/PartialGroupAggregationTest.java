package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PartialGroupAggregationTest {

  @Test
  void partialRecordsMergeExactlyAcrossCollisionsGrowthAndChangingCardinality() {
    for (final boolean compact : new boolean[] {false, true}) {
      final NumericGroupAggTable expected = table(compact, 16);
      final NumericGroupAggTable partial = table(compact, 16).useDenseIndex().allowPartialGroups();
      final SplittableRandom random = new SplittableRandom(713);
      for (int row = 0; row < 120_000; row++) {
        final int group = row < 20_000 ? row : row < 80_000 ? random.nextInt(20_000) : row % 2;
        fold(expected, group, row, compact);
        fold(partial, group, row, compact);
      }
      assertTrue(partial.usesPartialGroups());
      assertTrue(partial.size() > expected.size(), "displaced groups remain as additional partial records");
      assertTrue(partial.rehashes() > 0);
      final int[][] index = partial.buildPartitionIndex(16, 60);
      final NumericGroupAggTable indexed = table(compact, 16).useDenseIndex();
      for (int partition = 0; partition < 16; partition++) {
        NumericGroupAggTable.mergePartitionIndexed(new NumericGroupAggTable[] {partial}, new int[][][] {index},
            partition, indexed);
      }
      final NumericGroupAggTable stripes = table(compact, 16);
      for (int chunk = 0; chunk < partial.storageChunkCount(); chunk++) {
        final long[] data = partial.storageChunkOrNull(chunk);
        if (data == null) {
          continue;
        }
        for (int offset = 0; offset < data.length && data[offset] != 0L; offset += partial.stride()) {
          stripes.mergeStripes(data, offset, offset + partial.stride());
        }
      }
      assertEquals(20_000, indexed.size());
      assertEquals(20_000, stripes.size());
      for (int group = 0; group < 20_000; group++) {
        assertGroup(expected, indexed, group);
        assertGroup(expected, stripes, group);
      }
      assertTrue(indexed.hasProbeKeyCollision(), "the exact merge still detects same-hash identity collisions");
    }
  }

  @Test
  void directCachePreservesRepeatedNumericKeysZeroAndGrowth() {
    final NumericGroupAggTable partial = new NumericGroupAggTable(1, 16, true).useDenseIndex().allowPartialGroups();
    final NumericGroupAggTable expected = new NumericGroupAggTable(1, 16, true);
    final NumericGroupAggTable[] inputs = {partial, expected};
    for (int row = 0; row < 90_000; row++) {
      final long key = row % 5_000 + 1L;
      for (final NumericGroupAggTable table : inputs) {
        final int handle = table.acquire(key, row);
        table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)]++;
        table.setAuxAtAccBase(handle, key);
      }
    }
    assertTrue(partial.usesPartialGroups());
    assertTrue(partial.size() > expected.size());
    partial.acquireZero(13)[0] = 7;
    partial.setZeroAux(17);
    final NumericGroupAggTable merged = new NumericGroupAggTable(1, 16, true);
    NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {partial}, 0, 64, merged);
    assertEquals(5_001, merged.sizeIncludingZero());
    assertEquals(7, merged.zeroSlot()[0]);
    assertEquals(13, merged.zeroSlot()[1]);
    assertEquals(17, merged.zeroAux());
    for (long key = 1; key <= 5_000; key++) {
      final int handle = merged.acquire(key, 0);
      final long[] data = merged.storageAtAccBase(handle);
      final int offset = merged.offsetAtAccBase(handle);
      assertEquals(18, data[offset]);
      assertEquals(key - 1, data[offset + 1]);
      assertEquals(key, merged.auxAtAccBase(handle));
    }
    assertThrows(IllegalStateException.class, () -> partial.keyAtBucket(0));
    assertThrows(IllegalStateException.class, () -> partial.accBaseOfBucket(0));
    assertThrows(IllegalStateException.class, () -> partial.handleOfProbeKey(1));
    assertThrows(IllegalStateException.class, partial::hasProbeKeyCollision);
  }

  @Test
  void lowCardinalityKeepsExactProbingAndInvalidUsesAreRejected() {
    final NumericGroupAggTable partial = table(false, 16).useDenseIndex().allowPartialGroups();
    for (int row = 0; row < 30_000; row++) {
      fold(partial, row % 8, row, false);
    }
    assertFalse(partial.usesPartialGroups());
    assertEquals(8, partial.size());
    assertThrows(IllegalStateException.class, partial::allowPartialGroups);
    assertThrows(IllegalStateException.class, () -> table(false, 16).allowPartialGroups());
    final NumericGroupAggTable empty = table(false, 16).useDenseIndex().allowPartialGroups();
    assertThrows(IllegalStateException.class,
        () -> NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {partial}, 0, 64, empty));
    assertThrows(IllegalStateException.class, () -> empty.mergeStripes(new long[0], 0, 0));
    empty.release();
    assertThrows(IllegalStateException.class, empty::allowPartialGroups);
  }

  @Test
  void spillsAndFinalWorkerRecordsPreservePassOwnershipAndContributions() {
    final NumericGroupAggTable expected = table(true, 16);
    final GroupTableSpill spill = new GroupTableSpill(16, 60, hint -> table(true, hint),
        30_000, 4, 12, 100_000, true);
    final NumericGroupAggTable first = spill.freshLocal();
    final int[] expectedPartitions = new int[30_000];
    Arrays.fill(expectedPartitions, -1);
    for (int group = 0; group < 30_000; group++) {
      final int handle = fold(first, group, group, true);
      if (handle != NumericGroupAggTable.DISCARD_HANDLE) {
        expectedPartitions[group] = NumericGroupAggTable.partitionOf(first.keyAtAccBase(handle), 60);
        fold(expected, group, group, true);
      }
    }
    assertTrue(first.usesPartialGroups());
    spill.flush(first);
    assertTrue(first.released());
    final NumericGroupAggTable last = spill.freshLocal();
    for (int group = 0; group < 30_000; group++) {
      if (fold(last, group, 30_000L + group, true) != NumericGroupAggTable.DISCARD_HANDLE) {
        fold(expected, group, 30_000L + group, true);
      }
    }
    final int[][] index = last.buildPartitionIndex(16, 60);
    int groups = 0;
    for (int partition = 4; partition < 12; partition++) {
      final NumericGroupAggTable merged = spill.takeOrCreate(partition, () -> table(true, 16));
      NumericGroupAggTable.mergePartitionIndexed(new NumericGroupAggTable[] {last}, new int[][][] {index},
          partition, merged);
      assertFalse(merged.usesPartialGroups());
      groups += merged.size();
      for (int group = 0; group < 30_000; group++) {
        if (expectedPartitions[group] == partition) {
          assertGroup(expected, merged, group);
        }
      }
      merged.release();
    }
    assertEquals(expected.size(), groups);
    assertFalse(spill.aborted());
    last.release();
    spill.releaseTables();
  }

  @Test
  void exactMergeChecksOverflowAcrossDisplacedPartialRecords() {
    final NumericGroupAggTable partial = table(true, 16).useDenseIndex().allowPartialGroups();
    for (int group = 0; group < 10_000; group++) {
      fold(partial, group, group, true);
    }
    assertTrue(partial.usesPartialGroups());
    final long[] id = {0, 100_000};
    int handle = partial.acquireExact(17, 0, id, 0);
    long[] data = partial.storageAtAccBase(handle);
    int offset = partial.offsetAtAccBase(handle);
    data[offset] = 1;
    data[offset + 2] = 1;
    data[offset + 3] = Long.MAX_VALUE;
    partial.acquireExact(17, 0, new long[] {0, 100_001}, 0);
    handle = partial.acquireExact(17, 1, id, 0);
    data = partial.storageAtAccBase(handle);
    offset = partial.offsetAtAccBase(handle);
    data[offset] = 1;
    data[offset + 2] = 1;
    data[offset + 3] = 1;
    final NumericGroupAggTable merged = table(true, 16);
    assertThrows(ArithmeticException.class,
        () -> NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {partial}, 0, 64, merged));
  }

  private static NumericGroupAggTable table(final boolean compact, final int hint) {
    return compact ? NumericGroupAggTable.sumsOnly(1, hint, true, 1L, 2)
        : new NumericGroupAggTable(1, hint, true, 1L, 2);
  }

  private static long hash(final int group) {
    return group % 1_021; // Includes the remapped zero probe and unequal exact identities sharing a hash.
  }

  private static long[] identity(final int group) {
    return new long[] {group % 17 == 0 ? 1 : 0, group};
  }

  private static int fold(final NumericGroupAggTable table, final int group, final long ordinal,
      final boolean compact) {
    final int handle = table.acquireExact(hash(group), ordinal, identity(group), 0);
    if (handle == NumericGroupAggTable.DISCARD_HANDLE) {
      return handle;
    }
    final long[] data = table.storageAtAccBase(handle);
    final int base = table.offsetAtAccBase(handle);
    if (data[base]++ == 0) {
      table.setAuxAtAccBase(handle, group);
    }
    if (ordinal % 3 != 0) {
      final long value = ordinal % 101 - 50;
      data[base + 2]++;
      data[base + 3] = Math.addExact(data[base + 3], value);
      if (!compact) {
        data[base + 4] = Math.min(data[base + 4], value);
        data[base + 5] = Math.max(data[base + 5], value);
      }
    }
    return handle;
  }

  private static void assertGroup(final NumericGroupAggTable expected, final NumericGroupAggTable actual,
      final int group) {
    final int e = expected.acquireExact(hash(group), 0, identity(group), 0);
    final int a = actual.acquireExact(hash(group), 0, identity(group), 0);
    final int eb = expected.offsetAtAccBase(e);
    final int ab = actual.offsetAtAccBase(a);
    assertArrayEquals(Arrays.copyOfRange(expected.storageAtAccBase(e), eb, eb + expected.slotWidth()),
        Arrays.copyOfRange(actual.storageAtAccBase(a), ab, ab + actual.slotWidth()), "group " + group);
    assertEquals(expected.auxAtAccBase(e), actual.auxAtAccBase(a));
  }
}
