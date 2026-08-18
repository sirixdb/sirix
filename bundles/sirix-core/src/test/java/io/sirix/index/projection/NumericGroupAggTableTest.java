/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stripe-layout contract of {@link NumericGroupAggTable}: key, accumulator and aux lane of one
 * group live in ONE stripe, and growth must carry all three together. The growth path only opens
 * above ~98K distinct keys per worker, which no query-level test reaches — so it is exercised
 * directly here, where a base that outlives its array shows up as a wrong count rather than as a
 * silently discarded fold.
 */
final class NumericGroupAggTableTest {

  @Test
  @DisplayName("A stripe is [key][acc...][aux]: the key is one lane below the acc base, aux one width above")
  void stripeAddressing() {
    final NumericGroupAggTable t = new NumericGroupAggTable(2, 64, true);
    assertEquals(10, t.slotWidth(), "slotWidth = 2 + 4 * aggColumns");
    assertEquals(12, t.stride(), "stride = 1 + slotWidth + aux lane");

    final int base = t.acquire(0x1234_5678_9ABCL, 42L);
    assertEquals(0x1234_5678_9ABCL, t.keyAtAccBase(base));
    assertEquals(0L, t.table()[base], "fresh count");
    assertEquals(42L, t.table()[base + 1], "fresh first-seen ordinal");
    assertEquals(0L, t.auxAtAccBase(base), "a never-stamped aux lane reads 0");

    t.setAuxAtAccBase(base, 0x7FL);
    assertEquals(0x7FL, t.auxAtAccBase(base));
    assertEquals(0x1234_5678_9ABCL, t.keyAtAccBase(base), "the aux write must not spill into a key lane");

    // The bucket accessors address the same stripe as the acc-base ones.
    final int bucket = (base - 1) / t.stride();
    assertEquals(base, t.accBaseOfBucket(bucket));
    assertEquals(t.keyAtAccBase(base), t.keyAtBucket(bucket));
  }

  @Test
  @DisplayName("A count-only table is one cache line per group")
  void countOnlyStripeIsOneLine() {
    assertEquals(3, new NumericGroupAggTable(0, 16, false).stride());
    assertEquals(4, new NumericGroupAggTable(0, 16, true).stride(), "3 lanes plus aux = 32 bytes");
  }

  @Test
  @DisplayName("Fresh blocks carry fold-ready min/max seeds")
  void freshBlockSeeds() {
    final NumericGroupAggTable t = new NumericGroupAggTable(2, 16);
    final int base = t.acquire(7L, 3L);
    for (int a = 0; a < 2; a++) {
      final int aggBase = base + 2 + 4 * a;
      assertEquals(0L, t.table()[aggBase], "present count");
      assertEquals(0L, t.table()[aggBase + 1], "sum");
      assertEquals(Long.MAX_VALUE, t.table()[aggBase + 2], "min seed");
      assertEquals(Long.MIN_VALUE, t.table()[aggBase + 3], "max seed");
    }
  }

  @Test
  @DisplayName("Growth carries key, accumulator, first-seen and aux of every group")
  void growthPreservesEveryLane() {
    final int keys = 200_000; // several rehashes past the 16-bucket start
    final NumericGroupAggTable t = new NumericGroupAggTable(1, 16, true);
    final SplittableRandom rnd = new SplittableRandom(0xC0FFEEL);
    final long[] key = new long[keys];
    final LongOpenHashSet seen = new LongOpenHashSet(keys);
    for (int i = 0; i < keys; i++) {
      long k;
      do {
        k = rnd.nextLong();
      } while (k == 0L || !seen.add(k));
      key[i] = k;
      final int base = t.acquire(k, i);
      t.table()[base]++; // count
      t.table()[base + 2]++; // present count
      t.table()[base + 3] = i; // sum
      t.setAuxAtAccBase(base, ~(long) i);
    }
    assertEquals(keys, t.size());

    // Fold a second row into every group through a FRESH probe — a base that outlived its array
    // would land in the discarded one (or out of its bounds).
    for (int i = 0; i < keys; i++) {
      final int base = t.acquire(key[i], Long.MAX_VALUE);
      t.table()[base]++;
    }
    for (int i = 0; i < keys; i++) {
      final int base = t.acquire(key[i], Long.MAX_VALUE);
      assertEquals(key[i], t.keyAtAccBase(base), "key lane of group " + i);
      assertEquals(2L, t.table()[base], "count of group " + i);
      assertEquals(i, t.table()[base + 1], "first-seen ordinal of group " + i);
      assertEquals(i, t.table()[base + 3], "sum of group " + i);
      assertEquals(~(long) i, t.auxAtAccBase(base), "aux of group " + i);
    }
    assertEquals(keys, t.size(), "re-probing must not insert");
  }

  @Test
  @DisplayName("Key 0 takes the side slot, never a bucket")
  void zeroKeySideSlot() {
    final NumericGroupAggTable t = new NumericGroupAggTable(1, 16, true);
    assertFalse(t.hasZeroKey());
    final long[] zero = t.acquireZero(9L);
    assertTrue(t.hasZeroKey());
    assertEquals(9L, zero[1]);
    zero[0]++;
    t.setZeroAux(5L);
    assertEquals(9L, t.acquireZero(1_000L)[1], "the first sighting owns the ordinal");
    assertEquals(0, t.size(), "the zero group is not a bucket");
    assertEquals(1, t.sizeIncludingZero());
    assertEquals(5L, t.zeroAux());
  }

  @Test
  @DisplayName("The partition index addresses accumulator bases, and the indexed merge equals the scanning one")
  void indexedMergeMatchesScanningMerge() {
    final int partitions = 8;
    final int shift = 64 - Integer.numberOfTrailingZeros(partitions);
    final int sources = 4;
    final NumericGroupAggTable[] src = new NumericGroupAggTable[sources];
    final int[][][] index = new int[sources][][];
    final Long2LongOpenHashMap expectedCount = new Long2LongOpenHashMap();
    final Long2LongOpenHashMap expectedFirstSeen = new Long2LongOpenHashMap();
    long ordinal = 0;
    for (int s = 0; s < sources; s++) {
      final NumericGroupAggTable t = new NumericGroupAggTable(1, 64, true);
      for (int i = 0; i < 5_000; i++) {
        final long key = i % 1_000; // every source shares most keys with every other
        if (key == 0L) {
          final long[] zero = t.acquireZero(ordinal);
          zero[0]++;
        } else {
          final int base = t.acquire(key, ordinal);
          if (t.table()[base] == 0L) {
            t.setAuxAtAccBase(base, ordinal);
          }
          t.table()[base]++;
        }
        expectedCount.addTo(key, 1L);
        if (!expectedFirstSeen.containsKey(key)) {
          expectedFirstSeen.put(key, ordinal);
        }
        ordinal++;
      }
      src[s] = t;
      index[s] = t.buildPartitionIndex(partitions, shift);
      for (final int[] part : index[s]) {
        for (final int accBase : part) {
          assertNotEquals(0L, t.keyAtAccBase(accBase), "an index entry must address a LIVE stripe");
        }
      }
    }

    final Long2LongOpenHashMap indexedCount = new Long2LongOpenHashMap();
    final Long2LongOpenHashMap scannedCount = new Long2LongOpenHashMap();
    for (int p = 0; p < partitions; p++) {
      final NumericGroupAggTable indexed = new NumericGroupAggTable(1, 64, true);
      NumericGroupAggTable.mergePartitionIndexed(src, index, p, indexed);
      final NumericGroupAggTable scanned = new NumericGroupAggTable(1, 64, true);
      NumericGroupAggTable.mergePartition(src, p, shift, scanned);
      assertEquals(scanned.sizeIncludingZero(), indexed.sizeIncludingZero(), "partition " + p + " group count");
      drainCounts(indexed, indexedCount);
      drainCounts(scanned, scannedCount);
      for (int off = 0, st = indexed.stride(); off < indexed.table().length; off += st) {
        final long key = indexed.table()[off];
        if (key != 0L) {
          assertEquals(p, NumericGroupAggTable.partitionOf(key, shift), "a key merged into a foreign partition");
          assertEquals(expectedFirstSeen.get(key), indexed.table()[off + 2], "first-seen of key " + key);
          assertEquals(expectedFirstSeen.get(key), indexed.auxAtAccBase(off + 1), "aux of key " + key);
        }
      }
    }
    assertEquals(expectedCount, indexedCount);
    assertEquals(expectedCount, scannedCount);
  }

  @Test
  @DisplayName("Merging incompatible tables fails loudly instead of writing into a neighbour's key lane")
  void incompatibleMergeRejected() {
    final NumericGroupAggTable withAux = new NumericGroupAggTable(1, 16, true);
    withAux.acquire(3L, 0L);
    final NumericGroupAggTable noAux = new NumericGroupAggTable(1, 16, false);
    final int[][][] index = new int[][][] {withAux.buildPartitionIndex(1, 64)};
    assertThrows(IllegalStateException.class,
        () -> NumericGroupAggTable.mergePartitionIndexed(new NumericGroupAggTable[] {withAux}, index, 0, noAux));

    final NumericGroupAggTable widerBlock = new NumericGroupAggTable(2, 16, true);
    assertThrows(IllegalStateException.class,
        () -> NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {widerBlock}, 0, 64, withAux));
  }

  @Test
  @DisplayName("Negative aggregate counts are rejected")
  void rejectsNegativeAggColumns() {
    assertThrows(IllegalArgumentException.class, () -> new NumericGroupAggTable(-1, 16));
  }

  @Test
  @DisplayName("An overflowing SUM lane raises only when the query reads it")
  void unreadSumLanesDoNotRaiseOnMerge() {
    // Two partial sums that individually fit and jointly do not — the shape the JSONBench Q4
    // partition merge hit at 100M rows, where every worker's slice of one busy group was fine.
    final long half = Long.MAX_VALUE / 2 + 2L;

    // Mask 0: no sum or avg reads lane 0, so the merge must fold the lanes it IS asked for
    // (count, min, max) and leave the overflow unraised.
    final NumericGroupAggTable lenientSrc = new NumericGroupAggTable(1, 16, false, 0L);
    final NumericGroupAggTable lenientDst = new NumericGroupAggTable(1, 16, false, 0L);
    final int lenientBase = lenientSrc.acquire(7L, 0L);
    lenientSrc.table()[lenientBase + 2] = 1L;
    lenientSrc.table()[lenientBase + 3] = half;
    lenientSrc.table()[lenientBase + 4] = 11L;
    lenientSrc.table()[lenientBase + 5] = 99L;
    final int dstBase = lenientDst.acquire(7L, 0L);
    lenientDst.table()[dstBase + 2] = 1L;
    lenientDst.table()[dstBase + 3] = half;
    lenientDst.table()[dstBase + 4] = 5L;
    lenientDst.table()[dstBase + 5] = 42L;
    NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {lenientSrc}, 0, 64, lenientDst);
    final int merged = lenientDst.acquire(7L, 0L);
    assertEquals(2L, lenientDst.table()[merged + 2], "present count still folds");
    assertEquals(5L, lenientDst.table()[merged + 4], "min still folds");
    assertEquals(99L, lenientDst.table()[merged + 5], "max still folds");

    // Mask 1: lane 0 IS read, so the same merge must raise rather than emit a wrapped total.
    final NumericGroupAggTable exactSrc = new NumericGroupAggTable(1, 16, false, 1L);
    final NumericGroupAggTable exactDst = new NumericGroupAggTable(1, 16, false, 1L);
    final int exactSrcBase = exactSrc.acquire(7L, 0L);
    exactSrc.table()[exactSrcBase + 3] = half;
    final int exactDstBase = exactDst.acquire(7L, 0L);
    exactDst.table()[exactDstBase + 3] = half;
    assertThrows(ArithmeticException.class,
        () -> NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {exactSrc}, 0, 64, exactDst));
  }

  @Test
  @DisplayName("Tables that disagree about which sum lanes are exact refuse to merge")
  void mismatchedSumMasksRejected() {
    final NumericGroupAggTable lenient = new NumericGroupAggTable(1, 16, false, 0L);
    lenient.acquire(3L, 0L);
    final NumericGroupAggTable exact = new NumericGroupAggTable(1, 16, false, 1L);
    assertThrows(IllegalStateException.class,
        () -> NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {lenient}, 0, 64, exact));
  }

  @Test
  @DisplayName("Lanes past 63 stay exact — the mask is a long, and the conservative direction is exact")
  void lanesBeyondTheMaskWidthStayExact() {
    assertTrue(NumericGroupAggTable.sumsExact(0L, 64), "lane 64 has no bit and must default to exact");
    assertTrue(NumericGroupAggTable.sumsExact(0L, 200), "a far lane must default to exact");
    assertFalse(NumericGroupAggTable.sumsExact(0L, 63), "lane 63 is inside the mask");
    assertTrue(NumericGroupAggTable.sumsExact(1L << 63, 63), "lane 63's bit is the sign bit and must read set");
  }

  private static void drainCounts(final NumericGroupAggTable t, final Long2LongOpenHashMap into) {
    final long[] tbl = t.table();
    for (int off = 0, st = t.stride(); off < tbl.length; off += st) {
      if (tbl[off] != 0L) {
        into.addTo(tbl[off], tbl[off + 1]);
      }
    }
    if (t.hasZeroKey()) {
      into.addTo(0L, t.zeroSlot()[0]);
    }
  }
}
