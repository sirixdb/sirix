/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.ref.Reference;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * What one row costs a grouped scan in its group table: finding the group's stripe and folding the
 * row into it. {@code ProjectionFoldKernelBenchmark} measures the fold kernels over ONE
 * accumulator; here every row first probes a {@link NumericGroupAggTable}, so the score includes
 * the table's own memory — the probe's key reads, the stripe's writes and, in the build arms, the
 * chunks a growing table takes from its {@link GroupWorkspace} and hands back.
 *
 * <p>
 * Three arms, each over the same {@value #ROWS} rows of {@code groups} distinct keys in random
 * order:
 * <ul>
 * <li>{@code foldIntoBuiltTable}: every row hits a group that exists. The steady state of a pass.
 * <li>{@code buildWithinScan}: a fresh table per invocation in a workspace that stays open, so the
 * chunks of the last table serve the next. A later pass of a multi-pass scan.
 * <li>{@code buildPerScan}: a fresh workspace per invocation, closed after it. A whole grouped
 * scan: its regions are borrowed from the allocator and returned.
 * </ul>
 *
 * <p>
 * Scores are per row. Run for example with:
 * {@code ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="GroupTableBenchmark" -Pjmh.warmupIterations=5
 * -Pjmh.iterations=10 -Pjmh.fork=2}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Thread)
public class GroupTableBenchmark {

  private static final int ROWS = 1 << 21;
  private static final int AGGS = 2;
  private static final long ALL_SUMS_EXACT = -1L;

  /** Distinct groups: a table inside L2, one inside L3 and one far outside the caches. */
  @Param({"1000", "100000", "1000000"})
  public int groups;

  private long[] keys;
  private long[] values;
  private GroupWorkspace workspace;
  private NumericGroupAggTable built;

  @Setup(Level.Trial)
  public void setUp() {
    Allocators.getInstance().init(4L << 30);
    final SplittableRandom random = new SplittableRandom(0x5EEDL);
    final long[] distinct = new long[groups];
    for (int g = 0; g < groups; g++) {
      long key = mix(g + 1L);
      if (key == 0L) {
        key = 1L; // the zero key has its own slot outside the hashed table
      }
      distinct[g] = key;
    }
    keys = new long[ROWS];
    values = new long[ROWS];
    for (int row = 0; row < ROWS; row++) {
      keys[row] = distinct[random.nextInt(groups)];
      values[row] = random.nextInt(1 << 20);
    }
    workspace = new GroupWorkspace();
    built = new NumericGroupAggTable(AGGS, groups, true, ALL_SUMS_EXACT).attachWorkspace(workspace);
    fold(built);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    built.release();
    workspace.close();
  }

  /** SplitMix64's finalizer: group keys as spread as the hashes a composite key folds to. */
  private static long mix(final long value) {
    long z = value * 0x9E3779B97F4A7C15L;
    z = (z ^ z >>> 30) * 0xBF58476D1CE4E5B9L;
    z = (z ^ z >>> 27) * 0x94D049BB133111EBL;
    return z ^ z >>> 31;
  }

  /** The row loop of a numeric group kernel: acquire the stripe, then count, sum and extrema. */
  private long fold(final NumericGroupAggTable table) {
    final long[] rowKeys = keys;
    final long[] rowValues = values;
    for (int row = 0; row < ROWS; row++) {
      // Resolve storage AFTER acquire: growth can move the stripe to another chunk.
      final long block = table.acquireAccumulator(rowKeys[row], row, AGGS, 4);
      final long value = rowValues[row];
      GroupKernelLanes.increment(block, 0);
      for (int a = 0; a < AGGS; a++) {
        final int at = 2 + 4 * a;
        GroupKernelLanes.increment(block, at);
        GroupKernelLanes.add(block, at + 1, value);
        GroupKernelLanes.min(block, at + 2, value);
        GroupKernelLanes.max(block, at + 3, value);
      }
    }
    Reference.reachabilityFence(table);
    return table.size();
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long foldIntoBuiltTable() {
    return fold(built);
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long buildWithinScan() {
    final NumericGroupAggTable table =
        new NumericGroupAggTable(AGGS, 1 << 10, true, ALL_SUMS_EXACT).attachWorkspace(workspace);
    final long size = fold(table);
    table.release();
    return size;
  }

  @Benchmark
  @OperationsPerInvocation(ROWS)
  public long buildPerScan() {
    try (GroupWorkspace scan = new GroupWorkspace()) {
      final NumericGroupAggTable table =
          new NumericGroupAggTable(AGGS, 1 << 10, true, ALL_SUMS_EXACT).attachWorkspace(scan);
      return fold(table);
    }
  }
}
