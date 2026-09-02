/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
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

    final int handle = t.acquire(0x1234_5678_9ABCL, 42L);
    assertEquals(0x1234_5678_9ABCL, t.keyAtAccBase(handle));
    assertEquals(0L, lane(t, handle, 0), "fresh count");
    assertEquals(42L, lane(t, handle, 1), "fresh first-seen ordinal");
    assertEquals(0L, t.auxAtAccBase(handle), "a never-stamped aux lane reads 0");

    t.setAuxAtAccBase(handle, 0x7FL);
    assertEquals(0x7FL, t.auxAtAccBase(handle));
    assertEquals(0x1234_5678_9ABCL, t.keyAtAccBase(handle), "the aux write must not spill into a key lane");

    // The bucket accessors address the same stripe as the acc-base ones.
    final int bucket = handle;
    assertEquals(handle, t.accBaseOfBucket(bucket));
    assertEquals(t.keyAtAccBase(handle), t.keyAtBucket(bucket));
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
    final int handle = t.acquire(7L, 3L);
    for (int a = 0; a < 2; a++) {
      final int aggBase = 2 + 4 * a;
      assertEquals(0L, lane(t, handle, aggBase), "present count");
      assertEquals(0L, lane(t, handle, aggBase + 1), "sum");
      assertEquals(Long.MAX_VALUE, lane(t, handle, aggBase + 2), "min seed");
      assertEquals(Long.MIN_VALUE, lane(t, handle, aggBase + 3), "max seed");
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
      final int handle = t.acquire(k, i);
      incrementLane(t, handle, 0); // count
      incrementLane(t, handle, 2); // present count
      setLane(t, handle, 3, i); // sum
      t.setAuxAtAccBase(handle, ~(long) i);
    }
    assertEquals(keys, t.size());
    assertTrue(t.storageChunkCount() > 1, "the high-cardinality table must exercise physical chunking");
    for (int chunk = 0; chunk < t.storageChunkCount(); chunk++) {
      final long[] storage = t.storageChunkOrNull(chunk);
      if (storage != null) {
        assertTrue(storage.length <= NumericGroupAggTable.MAX_STORAGE_CHUNK_LANES,
            "backing array " + chunk + " crossed the non-humongous lane ceiling");
      }
    }

    // Fold a second row into every group through a FRESH probe — a base that outlived its array
    // would land in the discarded one (or out of its bounds).
    for (int i = 0; i < keys; i++) {
      final int handle = t.acquire(key[i], Long.MAX_VALUE);
      incrementLane(t, handle, 0);
    }
    for (int i = 0; i < keys; i++) {
      final int handle = t.acquire(key[i], Long.MAX_VALUE);
      assertEquals(key[i], t.keyAtAccBase(handle), "key lane of group " + i);
      assertEquals(2L, lane(t, handle, 0), "count of group " + i);
      assertEquals(i, lane(t, handle, 1), "first-seen ordinal of group " + i);
      assertEquals(i, lane(t, handle, 3), "sum of group " + i);
      assertEquals(~(long) i, t.auxAtAccBase(handle), "aux of group " + i);
    }
    assertEquals(keys, t.size(), "re-probing must not insert");
  }

  @Test
  @DisplayName("Linear probes cross chunk and table boundaries without changing bucket order")
  void probesCrossPhysicalBoundaries() {
    final NumericGroupAggTable chunkCrossing = new NumericGroupAggTable(1, 20_000, true);
    assertTrue(chunkCrossing.storageChunkCount() > 1);
    final int bucketsInFirstChunk = Integer.highestOneBit(
        NumericGroupAggTable.MAX_STORAGE_CHUNK_LANES / chunkCrossing.stride());
    final int boundaryBucket = bucketsInFirstChunk - 1;
    final long[] colliding = keysForBucket(chunkCrossing.capacity(), boundaryBucket, 3);
    final int first = chunkCrossing.acquire(colliding[0], 0L);
    final int second = chunkCrossing.acquire(colliding[1], 1L);
    final int third = chunkCrossing.acquire(colliding[2], 2L);
    assertEquals(boundaryBucket, first);
    assertEquals(boundaryBucket + 1, second);
    assertEquals(boundaryBucket + 2, third);
    assertTrue(chunkCrossing.storageAtAccBase(first) != chunkCrossing.storageAtAccBase(second),
        "the probe must cross into the next physical chunk");
    assertEquals(colliding[0], chunkCrossing.keyAtAccBase(first));
    assertEquals(colliding[1], chunkCrossing.keyAtAccBase(second));
    assertEquals(colliding[2], chunkCrossing.keyAtAccBase(third));

    final NumericGroupAggTable wrapping = new NumericGroupAggTable(1, 20_000, true);
    final int lastBucket = wrapping.capacity() - 1;
    final long[] wrappingKeys = keysForBucket(wrapping.capacity(), lastBucket, 2);
    assertEquals(lastBucket, wrapping.acquire(wrappingKeys[0], 0L));
    assertEquals(0, wrapping.acquire(wrappingKeys[1], 1L));
    assertEquals(wrappingKeys[0], wrapping.keyAtBucket(lastBucket));
    assertEquals(wrappingKeys[1], wrapping.keyAtBucket(0));
  }

  @Test
  @DisplayName("A high-cardinality sizing hint allocates only the chunks touched by live groups")
  void logicalCapacityDoesNotEagerlyAllocateEveryChunk() {
    final NumericGroupAggTable table = new NumericGroupAggTable(1, 65_536, true);
    assertTrue(table.storageChunkCount() > 8, "the hint must span enough chunks to prove laziness");
    assertEquals(0, allocatedChunks(table), "construction must allocate no bucket storage");

    final long[] keys = keysForBucket(table.capacity(), 17, 3);
    for (int i = 0; i < keys.length; i++) {
      final int handle = table.acquire(keys[i], i);
      incrementLane(table, handle, 0);
    }
    assertEquals(1, allocatedChunks(table), "one probe cluster must stay in one physical chunk");
    for (final long key : keys) {
      final int handle = table.acquire(key, Long.MAX_VALUE);
      assertEquals(1L, lane(table, handle, 0));
    }
  }

  @Test
  @DisplayName("A cached handle into an empty post-rehash chunk fails validation and self-heals")
  void staleHandleIntoLazyChunkFailsClosedAfterRehash() {
    final NumericGroupAggTable table = new NumericGroupAggTable(1, 20_000, true);
    final int oldCapacity = table.capacity();
    final int staleBucket = oldCapacity >>> 1;
    long staleKey = 1L;
    while (true) {
      final int mixed = (int) HashCommon.mix(staleKey);
      if ((mixed & oldCapacity - 1) == staleBucket && (mixed & oldCapacity) != 0) {
        break;
      }
      staleKey++;
    }
    final int staleHandle = table.acquire(staleKey, 0L);
    assertEquals(staleBucket, staleHandle);

    // Fill to the growth boundary with keys whose doubled-capacity home buckets are all in the
    // upper half. Their old-capacity homes occupy only the first quarter, so rehash cannot allocate
    // the lower-half chunk addressed by staleHandle.
    final int triggerSize = oldCapacity - (oldCapacity >>> 2) + 1;
    int inserted = 1;
    for (long key = staleKey + 1; inserted < triggerSize; key++) {
      final int mixed = (int) HashCommon.mix(key);
      if ((mixed & oldCapacity) == 0 || (mixed & oldCapacity - 1) >= (oldCapacity >>> 2)) {
        continue;
      }
      table.acquire(key, inserted++);
    }
    assertEquals(oldCapacity << 1, table.capacity(), "the final insertion must trigger one rehash");
    assertEquals(0L, table.keyAtAccBase(staleHandle), "an unallocated chunk is an empty-bucket witness");

    final int healed = table.acquire(staleKey, Long.MAX_VALUE);
    assertNotEquals(staleHandle, healed);
    assertEquals(staleKey, table.keyAtAccBase(healed));
    assertEquals(0L, lane(table, healed, 0), "validation must find the original accumulator, not insert a duplicate");
    assertEquals(triggerSize, table.size());
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
          final int handle = t.acquire(key, ordinal);
          if (lane(t, handle, 0) == 0L) {
            t.setAuxAtAccBase(handle, ordinal);
          }
          incrementLane(t, handle, 0);
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
      int bucket = 0;
      for (int chunk = 0; chunk < indexed.storageChunkCount(); chunk++) {
        final long[] storage = indexed.storageChunkOrNull(chunk);
        if (storage == null) {
          bucket += indexed.capacity() / indexed.storageChunkCount();
          continue;
        }
        for (int off = 0, st = indexed.stride(); off < storage.length; off += st, bucket++) {
          final long key = storage[off];
          if (key != 0L) {
            final int handle = indexed.accBaseOfBucket(bucket);
            assertEquals(p, NumericGroupAggTable.partitionOf(key, shift), "a key merged into a foreign partition");
            assertEquals(expectedFirstSeen.get(key), storage[off + 2], "first-seen of key " + key);
            assertEquals(expectedFirstSeen.get(key), indexed.auxAtAccBase(handle), "aux of key " + key);
          }
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
    setLane(lenientSrc, lenientBase, 2, 1L);
    setLane(lenientSrc, lenientBase, 3, half);
    setLane(lenientSrc, lenientBase, 4, 11L);
    setLane(lenientSrc, lenientBase, 5, 99L);
    final int dstBase = lenientDst.acquire(7L, 0L);
    setLane(lenientDst, dstBase, 2, 1L);
    setLane(lenientDst, dstBase, 3, half);
    setLane(lenientDst, dstBase, 4, 5L);
    setLane(lenientDst, dstBase, 5, 42L);
    NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {lenientSrc}, 0, 64, lenientDst);
    final int merged = lenientDst.acquire(7L, 0L);
    assertEquals(2L, lane(lenientDst, merged, 2), "present count still folds");
    assertEquals(5L, lane(lenientDst, merged, 4), "min still folds");
    assertEquals(99L, lane(lenientDst, merged, 5), "max still folds");

    // Mask 1: lane 0 IS read, so the same merge must raise rather than emit a wrapped total.
    final NumericGroupAggTable exactSrc = new NumericGroupAggTable(1, 16, false, 1L);
    final NumericGroupAggTable exactDst = new NumericGroupAggTable(1, 16, false, 1L);
    final int exactSrcBase = exactSrc.acquire(7L, 0L);
    setLane(exactSrc, exactSrcBase, 3, half);
    final int exactDstBase = exactDst.acquire(7L, 0L);
    setLane(exactDst, exactDstBase, 3, half);
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
    for (int chunk = 0; chunk < t.storageChunkCount(); chunk++) {
      final long[] storage = t.storageChunkOrNull(chunk);
      if (storage == null) {
        continue;
      }
      for (int off = 0, st = t.stride(); off < storage.length; off += st) {
        if (storage[off] != 0L) {
          into.addTo(storage[off], storage[off + 1]);
        }
      }
    }
    if (t.hasZeroKey()) {
      into.addTo(0L, t.zeroSlot()[0]);
    }
  }

  private static long lane(final NumericGroupAggTable table, final int handle, final int lane) {
    final long[] storage = table.storageAtAccBase(handle);
    return storage[table.offsetAtAccBase(handle) + lane];
  }

  private static void setLane(final NumericGroupAggTable table, final int handle, final int lane, final long value) {
    final long[] storage = table.storageAtAccBase(handle);
    storage[table.offsetAtAccBase(handle) + lane] = value;
  }

  private static void incrementLane(final NumericGroupAggTable table, final int handle, final int lane) {
    final long[] storage = table.storageAtAccBase(handle);
    storage[table.offsetAtAccBase(handle) + lane]++;
  }

  private static long[] keysForBucket(final int capacity, final int bucket, final int count) {
    final long[] keys = new long[count];
    final int mask = capacity - 1;
    int found = 0;
    for (long key = 1L; found < count; key++) {
      if (((int) HashCommon.mix(key) & mask) == bucket) {
        keys[found++] = key;
      }
    }
    return keys;
  }

  private static int allocatedChunks(final NumericGroupAggTable table) {
    int count = 0;
    for (int chunk = 0; chunk < table.storageChunkCount(); chunk++) {
      if (table.storageChunkOrNull(chunk) != null) {
        count++;
      }
    }
    return count;
  }

  @Test
  @DisplayName("A pooled table agrees with a private one through growth, recycles what it outgrows and releases everything")
  void pooledTableRecyclesThroughGrowthAndRelease() {
    final int stride = new NumericGroupAggTable(1, 16, true).stride();
    final int lanes = NumericGroupAggTable.fullChunkLanes(stride);
    assertEquals(0, lanes % stride, "a full chunk holds whole stripes");
    assertTrue(lanes <= NumericGroupAggTable.MAX_STORAGE_CHUNK_LANES, "a full chunk stays non-humongous");
    final LongChunkPool pool = new LongChunkPool(lanes, 1 << 12);
    // Sized above the chunk ceiling, so every chunk has the pool's length from the first insertion.
    final NumericGroupAggTable pooled = new NumericGroupAggTable(1, 1 << 12, true).attachChunkPool(pool);
    final NumericGroupAggTable plain = new NumericGroupAggTable(1, 1 << 12, true);
    assertSame(pool, pooled.chunkPool());
    assertEquals(lanes, pooled.chunkLanes(), "the sizing hint puts the table at full-size chunks");
    final int capacityBefore = pooled.capacity();

    final SplittableRandom rnd = new SplittableRandom(0x5EEDL);
    final LongOpenHashSet seen = new LongOpenHashSet();
    final long[] keys = new long[20_000];
    for (int i = 0; i < keys.length; i++) {
      long k;
      do {
        k = rnd.nextLong();
      } while (k == 0L || !seen.add(k));
      keys[i] = k;
    }
    for (int i = 0; i < keys.length; i++) {
      final int hp = pooled.acquire(keys[i], i);
      final int hq = plain.acquire(keys[i], i);
      pooled.storageAtAccBase(hp)[pooled.offsetAtAccBase(hp)] += i;
      plain.storageAtAccBase(hq)[plain.offsetAtAccBase(hq)] += i;
      pooled.setAuxAtAccBase(hp, i);
      plain.setAuxAtAccBase(hq, i);
    }
    assertTrue(pooled.capacity() > capacityBefore, "20K keys must grow a 2^12-hinted table");
    assertEquals(plain.capacity(), pooled.capacity(), "pooling changes the physical arrays only");
    assertEquals(plain.size(), pooled.size());
    assertTrue(pool.hits() + pool.misses() > 0, "every chunk of the pooled table went through the pool");
    assertTrue(LongChunkPool.totalGives() > 0L && pool.pooled() + pool.hits() > 0L,
        "the rehash recycled the chunks it outgrew");
    for (int i = 0; i < keys.length; i++) {
      final int hp = pooled.acquire(keys[i], -1L); // present: a pure lookup
      final int hq = plain.acquire(keys[i], -1L);
      assertEquals(keys[i], pooled.keyAtAccBase(hp));
      assertEquals(lane(plain, hq, 0), lane(pooled, hp, 0), "count lane of key " + i);
      assertEquals(lane(plain, hq, 1), lane(pooled, hp, 1), "first-seen lane of key " + i);
      assertEquals(plain.auxAtAccBase(hq), pooled.auxAtAccBase(hp), "aux lane of key " + i);
    }

    int allocated = 0;
    for (int c = 0; c < pooled.storageChunkCount(); c++) {
      if (pooled.storageChunkOrNull(c) != null) {
        allocated++;
      }
    }
    assertTrue(allocated > 1, "the grown table spans several chunks");
    final int pooledBefore = pool.pooled();
    pooled.release();
    assertTrue(pooled.released());
    assertEquals(0, pooled.storageChunkCount(), "a released table has no spine to probe");
    assertEquals(pooledBefore + allocated, pool.pooled(), "release hands back every allocated chunk");
    assertEquals(plain.size(), pooled.size(), "the counters survive the release");

    // The next table of the same layout takes those chunks back, and they are empty.
    final long hitsBefore = pool.hits();
    final NumericGroupAggTable next = new NumericGroupAggTable(1, 1 << 12, true).attachChunkPool(pool);
    final int h = next.acquire(keys[0], 0L);
    assertTrue(pool.hits() > hitsBefore, "the fresh table's first chunk came from the pool");
    final long[] chunk = next.storageAtAccBase(h);
    int nonZero = 0;
    for (final long lane : chunk) {
      if (lane != 0L) {
        nonZero++;
      }
    }
    // Only the one stripe just written is non-zero: the recycled chunk came back empty.
    final int off = next.offsetAtAccBase(h) - 1;
    int written = 0;
    for (int lane = off; lane < off + next.stride(); lane++) {
      if (chunk[lane] != 0L) {
        written++;
      }
    }
    assertEquals(written, nonZero, "every non-zero lane of the recycled chunk belongs to the new stripe");
  }

  @Test
  @DisplayName("A pool can be attached only before the first insertion")
  void poolAttachesOnlyToAnEmptyTable() {
    final LongChunkPool pool = new LongChunkPool(NumericGroupAggTable.fullChunkLanes(8), 16);
    final NumericGroupAggTable t = new NumericGroupAggTable(1, 16, true);
    t.acquire(5L, 0L);
    assertThrows(IllegalStateException.class, () -> t.attachChunkPool(pool));
    final NumericGroupAggTable zero = new NumericGroupAggTable(1, 16, true);
    zero.acquire(0L, 0L);
    assertThrows(IllegalStateException.class, () -> zero.attachChunkPool(pool), "the zero group counts too");
  }

  @Test
  @DisplayName("A table without a pool releases to the collector and reports it")
  void releaseWithoutPool() {
    final NumericGroupAggTable t = new NumericGroupAggTable(1, 16, true);
    t.acquire(5L, 0L);
    assertFalse(t.released());
    t.release();
    assertTrue(t.released());
    assertEquals(1, t.size());
    assertEquals(0, t.storageChunkCount());
  }
}
