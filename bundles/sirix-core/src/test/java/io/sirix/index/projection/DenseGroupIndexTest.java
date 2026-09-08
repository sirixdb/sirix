package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DenseGroupIndexTest {

  @Test
  void denseRecordsMatchSparseBucketsAcrossGrowthCollisionsAndRepeatedGroups() {
    final NumericGroupAggTable sparse = new NumericGroupAggTable(2, 16, true, 3L, 2);
    final NumericGroupAggTable dense = new NumericGroupAggTable(2, 16, true, 3L, 2).useDenseIndex();
    final SplittableRandom random = new SplittableRandom(193);
    final long[] identity = new long[2];
    for (int row = 0; row < 180_000; row++) {
      final int group = row < 60_000
          ? row
          : random.nextInt(60_000);
      identity[0] = group;
      identity[1] = ~group;
      final long hash = group % 8_191; // Real hash collisions, including the zero probe hash.
      fold(sparse, hash, identity, row);
      fold(dense, hash, identity, row);
    }
    assertEquals(sparse.size(), dense.size());
    assertTrue(dense.rehashes() > 0);
    assertTrue(dense.hasProbeKeyCollision());
    for (int group = 0; group < 60_000; group++) {
      identity[0] = group;
      identity[1] = ~group;
      final int expected = sparse.acquireExact(group % 8_191, 0L, identity, 0);
      final int actual = dense.acquireExact(group % 8_191, 0L, identity, 0);
      assertEquals(group, actual, "dense handles retain insertion order across index growth");
      assertArrayEquals(accumulator(sparse, expected), accumulator(dense, actual));
      assertEquals(sparse.auxAtAccBase(expected), dense.auxAtAccBase(actual));
    }
    int buckets = 0;
    for (int bucket = 0; bucket < dense.capacity(); bucket++) {
      final long key = dense.keyAtBucket(bucket);
      if (key != 0L) {
        assertEquals(key, dense.keyAtAccBase(dense.accBaseOfBucket(bucket)));
        buckets++;
      }
    }
    assertEquals(dense.size(), buckets);
  }

  @Test
  void indexChunksRecycleAndProbeOnlyLookupKeepsFullKeys() {
    final LongChunkPool payload = new LongChunkPool(NumericGroupAggTable.fullChunkLanes(8), 128);
    final LongChunkPool index = new LongChunkPool(NumericGroupAggTable.MAX_STORAGE_CHUNK_LANES, 128);
    final NumericGroupAggTable table =
        new NumericGroupAggTable(1, 16, true).useDenseIndex().attachProbePool(index).attachChunkPool(payload);
    for (int key = 1; key <= 100_000; key++) {
      final int handle = table.acquire(key, key);
      table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)]++;
    }
    for (int key = 1; key <= 100_000; key++) {
      assertEquals(key, table.keyAtAccBase(table.handleOfProbeKey(key)));
    }
    table.acquireZero(0L)[0] = 3L;
    assertEquals(100_001, table.sizeIncludingZero());
    assertThrows(IllegalStateException.class, table::useDenseIndex);
    table.release();
    assertTrue(table.released());
    final long[] recycled = index.take();
    for (final long lane : recycled) {
      assertEquals(0L, lane);
    }
  }

  @Test
  void countOnlyDenseStripesPreserveZeroAndPassOwnership() {
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16).useDenseIndex();
    table.setPassRange(60, 0, 8);
    long accepted = 0L;
    for (int row = 0; row < 160_000; row++) {
      final long key = row % 40_000 + 1;
      final int handle = table.acquire(key, row);
      if (NumericGroupAggTable.partitionOf(key, 60) < 8) {
        table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)]++;
        accepted++;
      } else {
        assertEquals(NumericGroupAggTable.DISCARD_HANDLE, handle);
      }
    }
    long rows = 0L;
    for (int bucket = 0; bucket < table.capacity(); bucket++) {
      if (table.keyAtBucket(bucket) != 0L) {
        final int handle = table.accBaseOfBucket(bucket);
        final long count = table.storageAtAccBase(handle)[table.offsetAtAccBase(handle)];
        assertEquals(4L, count);
        rows += count;
      }
    }
    assertEquals(accepted, rows);
    table.acquireZero(0L)[0] = 2L;
    assertEquals(2L, table.zeroSlot()[0]);
  }

  private static void fold(final NumericGroupAggTable table, final long hash, final long[] identity,
      final int ordinal) {
    final int handle = table.acquireExact(hash, ordinal, identity, 0);
    final long[] block = table.storageAtAccBase(handle);
    final int base = table.offsetAtAccBase(handle);
    if (block[base] == 0L) {
      table.setAuxAtAccBase(handle, ordinal);
    }
    block[base]++;
    for (int column = 0; column < 2; column++) {
      if ((ordinal + column) % 3 != 0) {
        final long value = column == 0
            ? ordinal
            : -ordinal;
        final int at = base + 2 + 4 * column;
        block[at]++;
        block[at + 1] = Math.addExact(block[at + 1], value);
        block[at + 2] = Math.min(block[at + 2], value);
        block[at + 3] = Math.max(block[at + 3], value);
      }
    }
  }

  private static long[] accumulator(final NumericGroupAggTable table, final int handle) {
    final long[] result = new long[table.slotWidth()];
    System.arraycopy(table.storageAtAccBase(handle), table.offsetAtAccBase(handle), result, 0, result.length);
    return result;
  }
}
