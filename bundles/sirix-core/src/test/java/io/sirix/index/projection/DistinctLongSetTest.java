/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The 64-bit distinct set behind a parallel {@code COUNT(DISTINCT numericColumn)}:
 * {@link DistinctLongSet}, {@link SharedDistinctLongSet} and the slice kernels
 * {@link ProjectionColumnScan#distinctLongs} / {@link ProjectionColumnScan#distinctBitset} — every
 * count held against a {@link LongOpenHashSet} of the same values.
 */
final class DistinctLongSetTest {

  @Test
  @DisplayName("add/contains agree with a reference set through several growths, zero and the sign extremes included")
  void addAndContainsAgreeWithTheReference() {
    final Random random = new Random(7);
    final DistinctLongSet set = new DistinctLongSet(0, null);
    final LongOpenHashSet truth = new LongOpenHashSet();
    final long[] special = {0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE};
    for (final long v : special) {
      assertEquals(truth.add(v), set.add(v), "first add of " + v);
      assertFalse(set.add(v), "second add of " + v);
    }
    for (int i = 0; i < 200_000; i++) {
      // Low-entropy low bits: multiples of 1024 defeat a set that indexes by the raw value.
      final long v = ((long) random.nextInt(60_000)) << 10;
      assertEquals(truth.add(v), set.add(v));
    }
    assertEquals(truth.size(), set.size());
    for (final long v : truth) {
      assertTrue(set.contains(v), "contains " + v);
    }
    for (int i = 0; i < 10_000; i++) {
      final long v = random.nextLong() | 1L; // odd: never one of the multiples of 1024 above
      assertEquals(truth.contains(v), set.contains(v));
    }
    assertTrue(set.capacity() >= truth.size() * 4 / 3, "load stays at or below three quarters");
    assertEquals((long) set.capacity() * Long.BYTES, set.chargedBytes());
  }

  @Test
  @DisplayName("capacityFor holds the keys at three-quarter load and never below the minimum")
  void capacityForBoundaries() {
    assertEquals(256, DistinctLongSet.capacityFor(0));
    assertEquals(256, DistinctLongSet.capacityFor(192));
    assertEquals(512, DistinctLongSet.capacityFor(193));
    assertEquals(1 << 20, DistinctLongSet.capacityFor(786_432));
    assertEquals(1 << 21, DistinctLongSet.capacityFor(786_433));
  }

  @Test
  @DisplayName("a growth the budget refuses throws and returns the charge")
  void budgetRefusalIsLoud() {
    final AtomicLong budget = new AtomicLong(256L * Long.BYTES + 64L);
    final DistinctLongSet set = new DistinctLongSet(0, budget);
    assertEquals(64L, budget.get());
    assertThrows(DistinctHash128Set.ByteBudgetExceededException.class, () -> {
      for (long v = 1; v <= 1_000; v++) {
        set.add(v);
      }
    });
    assertEquals(64L, budget.get(), "the refused growth is not charged");
    assertThrows(DistinctHash128Set.ByteBudgetExceededException.class,
        () -> new DistinctLongSet(1 << 20, new AtomicLong(1L)));
  }

  @Test
  @DisplayName("the partition and the slot index come from independent bits of the mix")
  void partitionAndSlotAreIndependent() {
    int[] partitionHits = new int[64];
    for (long v = 0; v < 64_000; v++) {
      partitionHits[DistinctLongSet.partitionOf(DistinctLongSet.mix(v << 10), 6)]++;
    }
    for (int p = 0; p < 64; p++) {
      assertTrue(partitionHits[p] > 700 && partitionHits[p] < 1_300, "partition " + p + " hits " + partitionHits[p]);
    }
    assertEquals(0, DistinctLongSet.partitionOf(-1L, 0));
    assertEquals(63, DistinctLongSet.partitionOf(-1L, 6));
  }

  @Test
  @DisplayName("workers filling one shared set concurrently count exactly what one set counts")
  void sharedSetCountsExactly() throws Exception {
    final int workers = 8;
    final int perWorker = 300_000;
    final long[][] values = new long[workers][perWorker];
    final LongOpenHashSet truth = new LongOpenHashSet();
    final Random random = new Random(11);
    for (int w = 0; w < workers; w++) {
      for (int i = 0; i < perWorker; i++) {
        // Heavy overlap between workers: 1M distinct candidates for 2.4M puts, zero among them.
        final long v = random.nextInt(1_000_000) * 4_096L;
        values[w][i] = v;
        truth.add(v);
      }
    }
    final AtomicLong budget = new AtomicLong(1L << 30);
    final SharedDistinctLongSet shared =
        new SharedDistinctLongSet(64, 1 << 10, SharedDistinctLongSet.DEFAULT_BUFFER_KEYS, budget);
    final Thread[] threads = new Thread[workers];
    for (int w = 0; w < workers; w++) {
      final long[] mine = values[w];
      threads[w] = new Thread(() -> {
        final SharedDistinctLongSet.Worker worker = shared.worker();
        for (final long v : mine) {
          worker.put(v);
        }
        worker.flush();
      });
      threads[w].start();
    }
    for (final Thread t : threads) {
      t.join();
    }
    assertEquals(truth.size(), shared.size());
    assertEquals(64, shared.partitions());
    assertTrue(shared.chargedBytes() >= (long) truth.size() * Long.BYTES, "the sets hold at least the answer");
    assertEquals((1L << 30) - budget.get() - shared.chargedBytes(),
        (long) workers * 64 * SharedDistinctLongSet.DEFAULT_BUFFER_KEYS * Long.BYTES,
        "buffers are the only other charge");
  }

  @Test
  @DisplayName("a worker's buffers are charged and refused like the sets")
  void workerBuffersAreBudgeted() {
    final AtomicLong budget = new AtomicLong(64L * 256L * Long.BYTES + 16L);
    final SharedDistinctLongSet shared = new SharedDistinctLongSet(64, 0, 512, budget);
    assertThrows(DistinctHash128Set.ByteBudgetExceededException.class, shared::worker);
    assertEquals(16L, budget.get());
    assertThrows(IllegalArgumentException.class, () -> new SharedDistinctLongSet(48, 0, 512, null));
    assertThrows(IllegalArgumentException.class, () -> new SharedDistinctLongSet(64, 0, 0, null));
  }

  /**
   * Slices of {@code rows} values from {@code values[offset..]}, every {@code gap}-th row missing.
   */
  private static ColumnSlice[] slices(final long[] values, final int rowsPerSlice, final int gap) {
    final int count = (values.length + rowsPerSlice - 1) / rowsPerSlice;
    final ColumnSlice[] slices = new ColumnSlice[count + 1];
    int at = 0;
    for (int s = 0; s < count; s++) {
      final int rows = Math.min(rowsPerSlice, values.length - at);
      final long[] v = new long[rows];
      final long[] presence = new long[(rows + 63) >>> 6];
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int r = 0; r < rows; r++) {
        v[r] = values[at + r];
        if ((at + r) % gap != 0) {
          presence[r >>> 6] |= 1L << (r & 63);
          min = Math.min(min, v[r]);
          max = Math.max(max, v[r]);
        }
      }
      slices[s] = new ColumnSlice(rows, (byte) 0, min, max, presence, v, null, null, null, null);
      at += rows;
    }
    slices[count] = null; // a missing leaf, skipped like an empty one
    return slices;
  }

  private static LongOpenHashSet presentTruth(final long[] values, final int gap) {
    final LongOpenHashSet truth = new LongOpenHashSet();
    for (int i = 0; i < values.length; i++) {
      if (i % gap != 0) {
        truth.add(values[i]);
      }
    }
    return truth;
  }

  @Test
  @DisplayName("distinctLongs feeds only PRESENT values, over any slice range, and reports a slice without lanes")
  void distinctLongsKernel() {
    final Random random = new Random(3);
    final long[] values = new long[50_000];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextInt(9_000) - 4_500L;
    }
    final ColumnSlice[] slices = slices(values, 1_024, 7);
    final DistinctLongSet whole = new DistinctLongSet(0, null);
    assertTrue(ProjectionColumnScan.distinctLongs(slices, 0, slices.length, whole));
    assertEquals(presentTruth(values, 7).size(), whole.size());
    // Two disjoint ranges into one set: the parallel split's contract.
    final DistinctLongSet split = new DistinctLongSet(0, null);
    assertTrue(ProjectionColumnScan.distinctLongs(slices, 0, 17, split));
    assertTrue(ProjectionColumnScan.distinctLongs(slices, 17, slices.length, split));
    assertEquals(whole.size(), split.size());
    // The MISSING rows carry a value the present ones never do; it must not be counted.
    final long[] marked = values.clone();
    for (int i = 0; i < marked.length; i += 7) {
      marked[i] = 1_000_000L + i;
    }
    final DistinctLongSet markedSet = new DistinctLongSet(0, null);
    assertTrue(ProjectionColumnScan.distinctLongs(slices(marked, 1_024, 7), 0, 50, markedSet));
    assertEquals(whole.size(), markedSet.size());
    final ColumnSlice[] lameLanes = slices.clone();
    lameLanes[3] = new ColumnSlice(10, (byte) 0, 0L, 0L, new long[1], null, null, null, null, null);
    assertFalse(ProjectionColumnScan.distinctLongs(lameLanes, 0, lameLanes.length, new DistinctLongSet(0, null)));
  }

  @Test
  @DisplayName("distinctBitset ORs into per-worker bitsets whose union count is the answer; a value outside the zone declines")
  void distinctBitsetKernel() {
    final Random random = new Random(5);
    final long[] values = new long[40_000];
    final long min = -20_000L;
    for (int i = 0; i < values.length; i++) {
      values[i] = min + random.nextInt(30_000);
    }
    values[1] = min; // row 1 is present (1 % 5 != 0): the zone's minimum is really in the column
    final ColumnSlice[] slices = slices(values, 1_000, 5);
    final int words = (30_000 >> 6) + 1;
    final long[][] bitsets = new long[3][words];
    assertTrue(ProjectionColumnScan.distinctBitset(slices, 0, 14, min, bitsets[0]));
    assertTrue(ProjectionColumnScan.distinctBitset(slices, 14, 30, min, bitsets[1]));
    assertTrue(ProjectionColumnScan.distinctBitset(slices, 30, slices.length, min, bitsets[2]));
    long count = ProjectionColumnScan.distinctBitsetUnionCount(bitsets, 0, 100)
        + ProjectionColumnScan.distinctBitsetUnionCount(bitsets, 100, words);
    assertEquals(presentTruth(values, 5).size(), count);
    // A bitset too small for the span is a lie about the zone map — declined, never an exception.
    assertFalse(ProjectionColumnScan.distinctBitset(slices, 0, slices.length, min, new long[words - 1]));
    assertFalse(ProjectionColumnScan.distinctBitset(slices, 0, slices.length, min + 1, new long[words]));
  }
}
