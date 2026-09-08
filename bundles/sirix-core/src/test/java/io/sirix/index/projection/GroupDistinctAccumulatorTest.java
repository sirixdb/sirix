package io.sirix.index.projection;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The striped accumulator against a single-threaded reference: exact under heavy cross-worker
 * duplication, exact for the missing-key rows, and bounded past its ceiling.
 */
final class GroupDistinctAccumulatorTest {

  /** Group ids that stress the hashing: zero, negatives, the extremes, and a few dense small ones. */
  private static final long[] GROUPS =
      {0L, 1L, 2L, 3L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x5DEECE66DL, 1L << 40, -(1L << 33), 42L, 43L, 1_000_003L};

  @Test
  void directGroupsUseTheSamePassAndDuplicateSemanticsAsSinks() {
    final GroupDistinctAccumulator acc = new GroupDistinctAccumulator(2, Long.MAX_VALUE);
    acc.setPassRange(60, 3, 8);
    for (final long group : GROUPS) {
      acc.worker(0).addToGroup(group, Long.MIN_VALUE);
      acc.worker(0).addToGroup(group, 0L);
      acc.worker(1).sinkFor(group).add(Long.MIN_VALUE);
      acc.worker(1).sinkFor(group).add(0L);
    }
    acc.worker(0).missing().add(1L);
    acc.finish();
    for (final long group : GROUPS) {
      final int partition = NumericGroupAggTable.partitionOf(group, 60);
      assertEquals(partition >= 3 && partition < 8
          ? 2L
          : 0L, acc.groupSizes().get(group));
    }
    assertEquals(0L, acc.missingSize());
  }

  @Test
  void singletonGroupsPromoteExactlyAndResetBetweenPasses() {
    final GroupDistinctAccumulator acc = new GroupDistinctAccumulator(3, Long.MAX_VALUE);
    for (int pass = 0; pass < 2; pass++) {
      for (int group = 0; group < 30_000; group++) {
        final long first = group + pass;
        acc.worker(0).sinkFor(group).add(first);
        acc.worker(1).sinkFor(group).add(first);
        if (group % 3 == 0) {
          acc.worker(2).sinkFor(group).add(~first);
          acc.worker(2).sinkFor(group).add(~first);
        }
      }
      acc.finish();
      assertEquals(40_000L, acc.entries());
      assertEquals(30_000, acc.groupSizes().size());
      for (int group = 0; group < 30_000; group++) {
        assertEquals(group % 3 == 0
            ? 2L
            : 1L, acc.groupSizes().get(group));
      }
      acc.reset();
    }
  }

  @Test
  void parallelCountPublicationVisitsEveryGroupOnceAndResets() {
    final GroupDistinctAccumulator acc = new GroupDistinctAccumulator(3, Long.MAX_VALUE);
    assertThrows(IllegalStateException.class, () -> acc.forEachGroupSize((group, count) -> {
    }));
    try (final ExecutorService pool = Executors.newFixedThreadPool(4)) {
      for (int pass = 0; pass < 2; pass++) {
        for (int group = -20_000; group < 20_000; group++) {
          for (int value = 0; value < 7; value++) {
            acc.worker(value % 3).addToGroup(group, value + pass);
            acc.worker((value + 1) % 3).addToGroup(group, value + pass);
          }
        }
        for (int worker = 0; worker < 3; worker++) {
          acc.worker(worker).missing().add(Long.MIN_VALUE);
          acc.worker(worker).missing().add(0L);
        }
        acc.finish((tasks, task) -> {
          final CompletableFuture<?>[] futures = new CompletableFuture<?>[tasks];
          for (int i = 0; i < tasks; i++) {
            final int index = i;
            futures[i] = CompletableFuture.runAsync(() -> task.accept(index), pool);
          }
          CompletableFuture.allOf(futures).join();
        });
        acc.finish((tasks, task) -> {
          throw new AssertionError("finish must be idempotent");
        });
        final Long2LongOpenHashMap visited = new Long2LongOpenHashMap();
        acc.forEachGroupSize((group, count) -> {
          assertEquals(0L, visited.put(group, count), "one callback per completed group");
          assertEquals(7L, count);
        });
        assertEquals(40_000, visited.size());
        assertEquals(visited, acc.groupSizes());
        assertEquals(2L, acc.missingSize());
        acc.reset();
        assertThrows(IllegalStateException.class, () -> acc.forEachGroupSize((group, count) -> {
        }));
      }
    }
  }

  @Test
  void failedPublicationDoesNotExposePartialCountsAndCanBeRetried() {
    final GroupDistinctAccumulator acc = new GroupDistinctAccumulator(1, Long.MAX_VALUE);
    acc.worker(0).addToGroup(0L, 5L);
    acc.worker(0).addToGroup(-1L, 7L);
    acc.worker(0).missing().add(9L);
    assertThrows(IllegalStateException.class, () -> acc.finish((tasks, task) -> task.accept(0)));
    assertThrows(IllegalStateException.class, acc::groupSizes);
    assertThrows(IllegalStateException.class, acc::missingSize);
    acc.finish();
    assertEquals(1L, acc.groupSizes().get(0L));
    assertEquals(1L, acc.groupSizes().get(-1L));
    assertEquals(1L, acc.missingSize());
    assertEquals(3L, acc.entries());
  }

  @Test
  @DisplayName("exact under duplication across workers, including the missing-key rows")
  void exactAcrossWorkers() throws Exception {
    final int workers = 8;
    final int rowsPerWorker = 200_000;
    final GroupDistinctAccumulator acc = new GroupDistinctAccumulator(workers, Long.MAX_VALUE);
    // The reference: what a single thread would keep. Built from the SAME row streams.
    final Long2ObjectOpenHashMap<LongOpenHashSet> expected = new Long2ObjectOpenHashMap<>();
    final LongOpenHashSet expectedMissing = new LongOpenHashSet();
    final long[][] rowGroups = new long[workers][rowsPerWorker];
    final long[][] rowValues = new long[workers][rowsPerWorker];
    final boolean[][] rowMissing = new boolean[workers][rowsPerWorker];
    final SplittableRandom rnd = new SplittableRandom(7);
    for (int w = 0; w < workers; w++) {
      for (int r = 0; r < rowsPerWorker; r++) {
        // A value pool small enough that every worker sees most values: the cross-worker
        // duplication the old design paid for once per worker.
        final long value = switch (rnd.nextInt(10)) {
          case 0 -> Long.MIN_VALUE;
          case 1 -> 0L;
          case 2 -> -rnd.nextInt(5_000);
          default -> rnd.nextInt(5_000);
        };
        final boolean missing = rnd.nextInt(20) == 0;
        final long group = GROUPS[rnd.nextInt(GROUPS.length)];
        rowGroups[w][r] = group;
        rowValues[w][r] = value;
        rowMissing[w][r] = missing;
        if (missing) {
          expectedMissing.add(value);
        } else {
          expected.computeIfAbsent(group, k -> new LongOpenHashSet()).add(value);
        }
      }
    }
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      final Future<?>[] futures = new Future<?>[workers];
      for (int w = 0; w < workers; w++) {
        final int idx = w;
        futures[w] = pool.submit(() -> {
          final GroupDistinctAccumulator.Worker worker = acc.worker(idx);
          final GroupDistinctAccumulator.Sink missing = worker.missing();
          for (int r = 0; r < rowsPerWorker; r++) {
            if (rowMissing[idx][r]) {
              missing.add(rowValues[idx][r]);
            } else {
              worker.sinkFor(rowGroups[idx][r]).add(rowValues[idx][r]);
            }
          }
        });
      }
      for (final Future<?> f : futures) {
        f.get(2, TimeUnit.MINUTES);
      }
    } finally {
      pool.shutdownNow();
    }
    assertThrows(IllegalStateException.class, acc::groupSizes, "sizes are published by finish() only");
    acc.finish();
    acc.finish(); // idempotent
    assertFalse(acc.exceeded());
    final Long2LongOpenHashMap sizes = acc.groupSizes();
    assertEquals(expected.size(), sizes.size(), "one size per group that was seen");
    long totalExpected = 0L;
    for (final Long2ObjectMap.Entry<LongOpenHashSet> e : expected.long2ObjectEntrySet()) {
      assertEquals(e.getValue().size(), sizes.get(e.getLongKey()), "group " + e.getLongKey());
      totalExpected += e.getValue().size();
    }
    assertEquals(expectedMissing.size(), acc.missingSize(), "the missing-key rows are one exact set");
    assertEquals(totalExpected + expectedMissing.size(), acc.entries(),
        "entries() counts every distinct pair exactly once");
    assertEquals(0L, sizes.get(9_999_999L), "an unseen group counts zero");
  }

  @Test
  @DisplayName("past the ceiling the state stops growing and the caller is told to decline")
  void ceilingBoundsGrowth() {
    final int workers = 4;
    final long ceiling = 1_000L;
    final GroupDistinctAccumulator acc = new GroupDistinctAccumulator(workers, ceiling);
    for (int w = 0; w < workers; w++) {
      final GroupDistinctAccumulator.Worker worker = acc.worker(w);
      for (int r = 0; r < 200_000; r++) {
        worker.sinkFor(r % 7).add((long) w * 1_000_000L + r); // every pair distinct
      }
    }
    acc.finish();
    assertTrue(acc.exceeded(), "800k distinct pairs against a ceiling of 1000");
    // Growth slack: at most one unflushed batch per stripe per worker beyond the ceiling.
    final long slack = (long) workers * GroupDistinctAccumulator.stripes() * GroupDistinctAccumulator.BATCH;
    assertTrue(acc.entries() <= ceiling + slack,
        "entries " + acc.entries() + " exceed the ceiling by more than the batch slack " + slack);
  }

  @Test
  @DisplayName("the same pair always lands on the same stripe, and the value stripes partition a group")
  void stripingIsDeterministicAndPartitioning() {
    final SplittableRandom rnd = new SplittableRandom(11);
    for (int i = 0; i < 100_000; i++) {
      final long g = rnd.nextLong();
      final long v = rnd.nextLong();
      final int s = GroupDistinctAccumulator.stripeOf(g, v);
      assertEquals(s, GroupDistinctAccumulator.stripeOf(g, v));
      assertTrue(s >= 0 && s < GroupDistinctAccumulator.stripes());
      // Same group: the stripe differs only in the two value bits.
      assertEquals(s >>> 2, GroupDistinctAccumulator.stripeOf(g, v ^ 0x1234L) >>> 2);
      final int m = GroupDistinctAccumulator.missingStripeOf(v);
      assertTrue(m >= 0 && m < GroupDistinctAccumulator.stripes());
    }
  }

  @Test
  @DisplayName("the default ceiling honours the property and the heap-derived floor")
  void defaultCeiling() {
    final long previous = GroupDistinctAccumulator.setMaxValuesForTesting(-1L);
    try {
      final long derived = GroupDistinctAccumulator.defaultMaxValues();
      assertTrue(derived >= 1L << 24 && derived <= 1L << 28, "derived ceiling " + derived);
      GroupDistinctAccumulator.setMaxValuesForTesting(123L);
      assertEquals(123L, GroupDistinctAccumulator.defaultMaxValues());
      assertEquals(123L, new GroupDistinctAccumulator(1).maxValues());
    } finally {
      GroupDistinctAccumulator.setMaxValuesForTesting(previous);
    }
  }
}
