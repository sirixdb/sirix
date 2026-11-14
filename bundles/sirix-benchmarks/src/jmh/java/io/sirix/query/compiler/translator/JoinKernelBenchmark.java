/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.compiler.translator;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.node.SegmentAccess;
import io.sirix.query.compiler.translator.JoinKernels.BucketedSide;
import io.sirix.query.compiler.translator.JoinKernels.DistinctDomain;
import io.sirix.query.compiler.translator.JoinKernels.ExtremaFold;
import io.sirix.query.compiler.translator.JoinKernels.SideLanes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Arrays versus off-heap slabs for the column-side join kernels of
 * {@link SirixJoinedGroupAggregateExpr}, measured on HotSpot C2 (the stand-in for a GraalVM that
 * hoists the per-read segment checks). Every slab arm reads through the supported foreign-memory
 * handles; the array arms are the heap kernels of the pre-lane main branch, kept verbatim so a slab
 * form that reaches their time is at parity by construction.
 *
 * <p>
 * Three kernels, shaped like the t250k interval join (SH1 Q10) and the row join (Q4/Q9):
 * <ul>
 * <li>{@code merge*}: both sides bucketed by join key, the probe records streamed once against
 * their key's build range ({@code foldMerge} + {@code ExtremaFold.pair}).</li>
 * <li>{@code probe*}: the dense probe walk — admitted rows from mask words, the join key and the
 * rostered fields read per matched row, a chained build table and a flat fold.</li>
 * <li>{@code pack*}: the counting sort that packs one side (count, then scatter).</li>
 * </ul>
 * Verdicts (HotSpot C2, performance cores, 2026-11-06): the merge reads fastest as a long-stepped
 * record loop through the handle (+6% over arrays, probe-major); an ordinal-indexed loop (+12%) and
 * one 256-bit vector load per record (+19%, +25% ordinal-indexed) are slower, so the shipped merge
 * keeps the handle reads and inverts the loop instead (one build record per bucket in locals). In
 * the probe walk the foundation's per-field segment-and-base arrays cost +16-50% over arrays; one
 * segment and per-field bases in locals cost +10% on dense masks and gain 15% on sparse ones, and a
 * per-leaf slice per lane sits between the two; a counted run over a full mask word buys nothing on
 * C2 for either representation.
 *
 * <p>
 * Run on the GraalVM JDK as HotSpot C2:
 * {@code ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="JoinKernelBenchmark"
 * -Pjmh.jvmArgs="-XX:-UseJVMCICompiler" -Pjmh.warmupIterations=5 -Pjmh.iterations=5 -Pjmh.fork=2}
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class JoinKernelBenchmark {

  private static final VarHandle SLAB_LONG = ValueLayout.JAVA_LONG_UNALIGNED.varHandle();

  /** Main's heap record: five longs (from, to, margin, distinct, group). */
  static final int STRIDE = 5;
  /** The shipped slab record (from, to, margin, group, distinct). */
  static final long STRIDE_BYTES = BucketedSide.STRIDE_BYTES;
  static final long FROM = BucketedSide.FROM;
  static final long TO = BucketedSide.TO;
  static final long MARGIN = BucketedSide.MARGIN;
  static final long META = BucketedSide.META;
  static final long DISTINCT = BucketedSide.DISTINCT;

  private static final long YEAR = 365L * 86_400L;

  // ==================== merge fold: packed sides ====================

  /**
   * Two sides as lanes, shaped like the t250k interval join: the build side ({@code slots} keys, one
   * or two records per key, group index per record) and the probe side (~{@code probePerSlot} records
   * per key, a distinct operand per record), half of all pairs overlapping. The same lanes are packed
   * by main's heap counting sort (records of five longs in a {@code long[]}) and by the shipped
   * {@link JoinKernels.BucketedSide} (records in a slab), so the merge arms read identical data.
   */
  @State(Scope.Benchmark)
  @SuppressWarnings("NullAway.Init") // JMH initialises the state in @Setup.
  public static class Packed {
    @Param({"2048"})
    int slots;
    @Param({"40"})
    int probePerSlot;

    static final int ROWS = 1024;
    final int groups = 16;
    final long origin = 1_700_000_000L;
    int distinctWords;
    final List<MemorySegment> slabs = new ArrayList<>();
    // Heap-packed (main's layout: from, to, margin, distinct, group).
    int[] buildStart;
    int[] probeStart;
    long[] buildRows;
    long[] probeRows;
    /** The probe side's heap lanes, the input of the heap pack arm. */
    HeapSide heapProbe;
    // Shipped lanes and packed sides.
    SideLanes buildLanes;
    SideLanes probeLanes;
    BucketedSide build;
    BucketedSide probe;

    @Setup
    public void setUp() {
      final Random random = new Random(42);
      final int buildLeaves = (int) Math.ceil(2.0 * slots / ROWS);
      final int probeLeaves = (int) Math.ceil((double) slots * probePerSlot / ROWS);
      final HeapSide heapBuild = side(random, buildLeaves, true);
      heapProbe = side(random, probeLeaves, false);
      distinctWords = (1 << 16) >>> 6;
      // Heap pack.
      buildStart = new int[slots + 1];
      buildRows = heapPack(heapBuild, buildStart);
      probeStart = new int[slots + 1];
      probeRows = heapPack(heapProbe, probeStart);
      // Shipped pack.
      buildLanes = lanes(heapBuild);
      probeLanes = lanes(heapProbe);
      build = shippedPack(buildLanes);
      probe = shippedPack(probeLanes);
    }

    @TearDown
    public void tearDown() {
      for (final MemorySegment slab : slabs) {
        ALLOCATOR.releaseSlab(slab);
      }
      slabs.clear();
    }

    /** Lanes of one side on the heap; every row admitted (the mask walk is the probe benchmark). */
    private HeapSide side(final Random random, final int leaves, final boolean buildSide) {
      final HeapSide side = new HeapSide(leaves);
      for (int leaf = 0; leaf < leaves; leaf++) {
        Arrays.fill(side.masks[leaf], -1L);
        for (int row = 0; row < ROWS; row++) {
          side.join[leaf][row] = random.nextInt(slots);
          final long from = origin + random.nextInt((int) YEAR);
          side.from[leaf][row] = from;
          side.to[leaf][row] = from + 1 + random.nextInt((int) (YEAR / 2));
          side.margin[leaf][row] = buildSide
              ? 100 + random.nextInt(900)
              : random.nextInt(200);
          side.distinct[leaf][row] = buildSide
              ? 0L
              : random.nextInt(1 << 16);
          side.group[leaf][row] = buildSide
              ? random.nextInt(groups)
              : 0L;
        }
      }
      return side;
    }

    private long[] heapPack(final HeapSide side, final int[] bucketStart) {
      int total = 0;
      for (int leaf = 0; leaf < side.masks.length; leaf++) {
        for (int row = 0; row < ROWS; row++) {
          final long slot = side.join[leaf][row];
          if (slot >= 0 && slot < slots) {
            bucketStart[(int) slot + 1]++;
            total++;
          }
        }
      }
      for (int slot = 0; slot < slots; slot++) {
        bucketStart[slot + 1] += bucketStart[slot];
      }
      final long[] rows = new long[total * STRIDE];
      final int[] fill = Arrays.copyOf(bucketStart, slots);
      for (int leaf = 0; leaf < side.masks.length; leaf++) {
        for (int row = 0; row < ROWS; row++) {
          final long slot = side.join[leaf][row];
          if (slot < 0 || slot >= slots) {
            continue;
          }
          final int at = fill[(int) slot]++ * STRIDE;
          rows[at] = side.from[leaf][row];
          rows[at + 1] = side.to[leaf][row];
          rows[at + 2] = side.margin[leaf][row];
          rows[at + 3] = side.distinct[leaf][row];
          rows[at + 4] = side.group[leaf][row];
        }
      }
      return rows;
    }

    private SideLanes lanes(final HeapSide side) {
      final int leaves = side.masks.length;
      final long laneBytes = (8L * ROWS + 63L) & -64L;
      final MemorySegment slab = allocate(laneBytes * 6L * leaves);
      slabs.add(slab);
      final long[][] masks = new long[leaves][];
      final MemorySegment[] joinSeg = new MemorySegment[leaves];
      final long[] joinBase = new long[leaves];
      final MemorySegment[] fromSeg = new MemorySegment[leaves];
      final long[] fromBase = new long[leaves];
      final MemorySegment[] toSeg = new MemorySegment[leaves];
      final long[] toBase = new long[leaves];
      final MemorySegment[] marginSeg = new MemorySegment[leaves];
      final long[] marginBase = new long[leaves];
      final MemorySegment[] distinctSeg = new MemorySegment[leaves];
      final long[] distinctBase = new long[leaves];
      final MemorySegment[] groupSeg = new MemorySegment[leaves];
      final long[] groupBase = new long[leaves];
      long cursor = 0L;
      for (int leaf = 0; leaf < leaves; leaf++) {
        masks[leaf] = side.masks[leaf].clone();
        cursor = place(slab, cursor, side.join[leaf], joinSeg, joinBase, leaf);
        cursor = place(slab, cursor, side.from[leaf], fromSeg, fromBase, leaf);
        cursor = place(slab, cursor, side.to[leaf], toSeg, toBase, leaf);
        cursor = place(slab, cursor, side.margin[leaf], marginSeg, marginBase, leaf);
        cursor = place(slab, cursor, side.distinct[leaf], distinctSeg, distinctBase, leaf);
        cursor = place(slab, cursor, side.group[leaf], groupSeg, groupBase, leaf);
      }
      return new SideLanes(masks, joinSeg, joinBase, fromSeg, fromBase, toSeg, toBase, marginSeg, marginBase,
          distinctSeg, distinctBase, groupSeg, groupBase);
    }

    private static long place(final MemorySegment slab, final long cursor, final long[] values,
        final MemorySegment[] segments, final long[] bases, final int leaf) {
      MemorySegment.copy(values, 0, slab, ValueLayout.JAVA_LONG_UNALIGNED, cursor, values.length);
      segments[leaf] = slab;
      bases[leaf] = cursor;
      return cursor + ((8L * values.length + 63L) & -64L);
    }

    private BucketedSide shippedPack(final SideLanes lanes) {
      final int[] bucketStart = new int[slots + 1];
      final int total = BucketedSide.countByBucket(lanes, 0L, slots, bucketStart);
      final MemorySegment slab = allocate(BucketedSide.slabBytes(slots, total));
      slabs.add(slab);
      return BucketedSide.pack(slab, lanes, 0L, slots, 0L, bucketStart, total);
    }
  }

  /** One side's lanes on the heap: a join key, two bounds, a margin, a distinct operand, a group. */
  static final class HeapSide {
    final long[][] masks;
    final long[][] join;
    final long[][] from;
    final long[][] to;
    final long[][] margin;
    final long[][] distinct;
    final long[][] group;

    HeapSide(final int leaves) {
      masks = new long[leaves][Packed.ROWS >>> 6];
      join = new long[leaves][Packed.ROWS];
      from = new long[leaves][Packed.ROWS];
      to = new long[leaves][Packed.ROWS];
      margin = new long[leaves][Packed.ROWS];
      distinct = new long[leaves][Packed.ROWS];
      group = new long[leaves][Packed.ROWS];
    }
  }

  /** A private slab allocator for the fixtures: 1 GiB, never the process allocator. */
  private static final FrameSlotAllocator ALLOCATOR = new FrameSlotAllocator(1L << 30);

  static MemorySegment allocate(final long bytes) {
    final MemorySegment slab = ALLOCATOR.allocateSlab(bytes);
    if (slab == null) {
      throw new IllegalStateException("allocator refused " + bytes + " bytes");
    }
    return slab;
  }

  /** The flat fold state of main's heap kernel: per-group extrema and a value-major bitmap. */
  @State(Scope.Thread)
  @SuppressWarnings("NullAway.Init") // JMH initialises the state in @Setup.
  public static class Fold {
    long[] groupMinimum;
    long[] groupMaximum;
    long[] distinctCounts;
    long[] bits;
    int groups;

    @Setup(Level.Trial)
    public void setUp(final Packed packed) {
      groups = packed.groups;
      groupMinimum = new long[groups];
      groupMaximum = new long[groups];
      distinctCounts = new long[groups];
      bits = new long[groups * packed.distinctWords];
    }

    @Setup(Level.Invocation)
    public void reset() {
      Arrays.fill(groupMinimum, Long.MAX_VALUE);
      Arrays.fill(groupMaximum, Long.MIN_VALUE);
      Arrays.fill(distinctCounts, 0L);
      Arrays.fill(bits, 0L);
    }

    long checksum() {
      long sum = 0L;
      for (int g = 0; g < groups; g++) {
        sum = sum * 31 + groupMinimum[g] + groupMaximum[g] + distinctCounts[g];
      }
      return sum;
    }
  }

  @State(Scope.Thread)
  @SuppressWarnings("NullAway.Init")
  public static class ShippedFold {
    ExtremaFold fold;

    @Setup(Level.Trial)
    public void setUp(final Packed packed) {
      fold = new ExtremaFold(packed.groups, new DistinctDomain(0L, packed.distinctWords), packed.build, false, true);
    }

    @Setup(Level.Invocation)
    public void reset() {
      Arrays.fill(fold.groupMinimum, Long.MAX_VALUE);
      Arrays.fill(fold.groupMaximum, Long.MIN_VALUE);
      Arrays.fill(fold.distinctCounts, 0L);
      Arrays.fill(fold.bits, 0L);
    }
  }

  // --- arrays: the heap kernel of main (int offsets into long[] records) ---

  @Benchmark
  public long mergeArrays(final Packed packed, final Fold fold) {
    final int[] buildStart = packed.buildStart;
    final int[] probeStart = packed.probeStart;
    final long[] probeRows = packed.probeRows;
    final int slots = packed.slots;
    for (int slot = 0; slot < slots; slot++) {
      final int buildBegin = buildStart[slot] * STRIDE;
      final int buildEnd = buildStart[slot + 1] * STRIDE;
      final int probeBegin = probeStart[slot] * STRIDE;
      final int probeEnd = probeStart[slot + 1] * STRIDE;
      if (buildBegin == buildEnd || probeBegin == probeEnd) {
        continue;
      }
      for (int p = probeBegin; p < probeEnd; p += STRIDE) {
        pairArrays(fold, packed.buildRows, probeRows[p], probeRows[p + 1], probeRows[p + 2], probeRows[p + 3],
            buildBegin, buildEnd);
      }
    }
    return fold.checksum();
  }

  private static void pairArrays(final Fold fold, final long[] buildRows, final long from, final long to,
      final long margin, final long probeValue, final int buildBegin, final int buildEnd) {
    final int groups = fold.groups;
    for (int b = buildEnd - STRIDE; b >= buildBegin; b -= STRIDE) {
      if (from >= buildRows[b + 1] || buildRows[b] >= to) {
        continue;
      }
      final int group = (int) buildRows[b + 4];
      final long value = Math.subtractExact(buildRows[b + 2], margin);
      if (value < fold.groupMinimum[group]) {
        fold.groupMinimum[group] = value;
      }
      if (value > fold.groupMaximum[group]) {
        fold.groupMaximum[group] = value;
      }
      final long bitIndex = probeValue * groups + group;
      final int bitWord = (int) (bitIndex >>> 6);
      final long bit = 1L << bitIndex;
      final long prior = fold.bits[bitWord];
      if ((prior & bit) == 0L) {
        fold.bits[bitWord] = prior | bit;
        fold.distinctCounts[group]++;
      }
    }
  }

  // --- handle: the foundation's probe-major slab merge (four probe reads, two to four build reads
  // per pair) ---

  @Benchmark
  public long mergeHandle(final Packed packed, final Fold fold) {
    final MemorySegment buildSlab = packed.build.slab;
    final MemorySegment probeSlab = packed.probe.slab;
    final long buildRows = packed.build.rowsBase;
    final long probeRows = packed.probe.rowsBase;
    final int slots = packed.slots;
    int buildStart = SegmentAccess.getInt(buildSlab, 0L);
    int probeStart = SegmentAccess.getInt(probeSlab, 0L);
    for (int slot = 0; slot < slots; slot++) {
      final int buildNext = SegmentAccess.getInt(buildSlab, 4L * (slot + 1));
      final int probeNext = SegmentAccess.getInt(probeSlab, 4L * (slot + 1));
      if (buildStart != buildNext && probeStart != probeNext) {
        final long buildBegin = buildRows + buildStart * STRIDE_BYTES;
        final long buildEnd = buildRows + buildNext * STRIDE_BYTES;
        final long probeEnd = probeRows + probeNext * STRIDE_BYTES;
        for (long p = probeRows + probeStart * STRIDE_BYTES; p < probeEnd; p += STRIDE_BYTES) {
          pairHandle(fold, buildSlab, (long) SLAB_LONG.get(probeSlab, p + FROM),
              (long) SLAB_LONG.get(probeSlab, p + TO), (long) SLAB_LONG.get(probeSlab, p + MARGIN),
              (long) SLAB_LONG.get(probeSlab, p + DISTINCT), buildBegin, buildEnd);
        }
      }
      buildStart = buildNext;
      probeStart = probeNext;
    }
    return fold.checksum();
  }

  private static void pairHandle(final Fold fold, final MemorySegment slab, final long from, final long to,
      final long margin, final long probeValue, final long buildBegin, final long buildEnd) {
    final int groups = fold.groups;
    for (long b = buildEnd - STRIDE_BYTES; b >= buildBegin; b -= STRIDE_BYTES) {
      if (from >= (long) SLAB_LONG.get(slab, b + TO) || (long) SLAB_LONG.get(slab, b + FROM) >= to) {
        continue;
      }
      final int group = (int) (long) SLAB_LONG.get(slab, b + META);
      final long value = Math.subtractExact((long) SLAB_LONG.get(slab, b + MARGIN), margin);
      if (value < fold.groupMinimum[group]) {
        fold.groupMinimum[group] = value;
      }
      if (value > fold.groupMaximum[group]) {
        fold.groupMaximum[group] = value;
      }
      final long bitIndex = probeValue * groups + group;
      final int bitWord = (int) (bitIndex >>> 6);
      final long bit = 1L << bitIndex;
      final long prior = fold.bits[bitWord];
      if ((prior & bit) == 0L) {
        fold.bits[bitWord] = prior | bit;
        fold.distinctCounts[group]++;
      }
    }
  }

  // --- shipped: JoinKernels.foldMerge (build-major, one build record per bucket in locals) ---

  @Benchmark
  public long mergeShipped(final Packed packed, final ShippedFold state) {
    final ExtremaFold fold = state.fold;
    JoinKernels.foldMerge(fold, packed.build, packed.probe, packed.slots);
    return checksum(fold, packed.groups);
  }

  /**
   * JoinKernels.foldChained: the probe lanes streamed against the packed build side (a cold query).
   */
  @Benchmark
  public long chainedShipped(final Packed packed, final ShippedFold state) {
    final ExtremaFold fold = state.fold;
    JoinKernels.foldChained(fold, packed.build, packed.probeLanes, 0L, packed.slots);
    return checksum(fold, packed.groups);
  }

  private static long checksum(final ExtremaFold fold, final int groups) {
    long sum = 0L;
    for (int g = 0; g < groups; g++) {
      sum = sum * 31 + fold.groupMinimum[g] + fold.groupMaximum[g] + fold.distinctCount(g);
    }
    return sum;
  }

  @SuppressWarnings("ReferenceEquality") // Verify trial-owned array identity, not its contents.
  public static void main(final String[] args) {
    final Packed packed = new Packed();
    try {
      mergeFixture(packed);
      final JoinKernelBenchmark benchmark = new JoinKernelBenchmark();
      final Fold arrays = new Fold();
      arrays.setUp(packed);
      final ShippedFold shipped = new ShippedFold();
      shipped.setUp(packed);
      final ExtremaFold reusable = shipped.fold;
      final long[] minima = arrays.groupMinimum;
      final long[] expectedMinimum = new long[packed.groups];
      final long[] expectedMaximum = new long[packed.groups];
      final long[] expectedDistinct = new long[packed.groups];
      Arrays.fill(expectedMinimum, Long.MAX_VALUE);
      Arrays.fill(expectedMaximum, Long.MIN_VALUE);
      expectedMinimum[0] = 7L;
      expectedMaximum[0] = 17L;
      expectedDistinct[0] = 2L;
      expectedMinimum[1] = 22L;
      expectedMaximum[1] = 26L;
      expectedDistinct[1] = 2L;
      for (int repeat = 0; repeat < 3; repeat++) {
        arrays.reset();
        requireEmpty(arrays.groupMinimum, arrays.groupMaximum, arrays.distinctCounts, arrays.bits);
        benchmark.mergeArrays(packed, arrays);
        requireResult(arrays.groupMinimum, arrays.groupMaximum, arrays.distinctCounts, expectedMinimum, expectedMaximum,
            expectedDistinct);
        arrays.reset();
        requireEmpty(arrays.groupMinimum, arrays.groupMaximum, arrays.distinctCounts, arrays.bits);
        benchmark.mergeHandle(packed, arrays);
        requireResult(arrays.groupMinimum, arrays.groupMaximum, arrays.distinctCounts, expectedMinimum, expectedMaximum,
            expectedDistinct);
        shipped.reset();
        requireEmpty(shipped.fold.groupMinimum, shipped.fold.groupMaximum, shipped.fold.distinctCounts,
            shipped.fold.bits);
        benchmark.mergeShipped(packed, shipped);
        requireResult(shipped.fold.groupMinimum, shipped.fold.groupMaximum, shipped.fold.distinctCounts,
            expectedMinimum, expectedMaximum, expectedDistinct);
        shipped.reset();
        requireEmpty(shipped.fold.groupMinimum, shipped.fold.groupMaximum, shipped.fold.distinctCounts,
            shipped.fold.bits);
        benchmark.chainedShipped(packed, shipped);
        requireResult(shipped.fold.groupMinimum, shipped.fold.groupMaximum, shipped.fold.distinctCounts,
            expectedMinimum, expectedMaximum, expectedDistinct);
        if (shipped.fold != reusable || arrays.groupMinimum != minima) {
          throw new IllegalStateException("merge invocation replaced its trial-owned fold state");
        }
      }
    } finally {
      packed.tearDown();
    }
  }

  private static void mergeFixture(final Packed packed) {
    packed.slots = 4;
    packed.distinctWords = 1;
    final HeapSide build = new HeapSide(1);
    final HeapSide probe = new HeapSide(1);
    Arrays.fill(build.join[0], -1L);
    Arrays.fill(probe.join[0], -1L);
    fixtureRow(build, 0, 0L, 0L, 10L, 20L, 0L, 0L);
    fixtureRow(build, 1, 0L, 5L, 15L, 30L, 0L, 1L);
    fixtureRow(build, 2, 1L, 0L, 5L, 9L, 0L, 0L);
    fixtureRow(build, 3, 2L, 50L, 60L, 100L, 0L, 2L);
    fixtureRow(probe, 0, 0L, 0L, 5L, 3L, 7L, 0L);
    fixtureRow(probe, 1, 0L, 8L, 12L, 4L, 7L, 0L);
    fixtureRow(probe, 2, 0L, 10L, 15L, 8L, 9L, 0L);
    fixtureRow(probe, 3, 1L, 4L, 6L, 2L, 11L, 0L);
    fixtureRow(probe, 4, 2L, 60L, 70L, 5L, 13L, 0L);
    fixtureRow(probe, 5, 3L, 0L, 1L, 1L, 15L, 0L);
    packed.heapProbe = probe;
    packed.buildStart = new int[packed.slots + 1];
    packed.probeStart = new int[packed.slots + 1];
    packed.buildRows = packed.heapPack(build, packed.buildStart);
    packed.probeRows = packed.heapPack(probe, packed.probeStart);
    packed.buildLanes = packed.lanes(build);
    packed.probeLanes = packed.lanes(probe);
    packed.build = packed.shippedPack(packed.buildLanes);
    packed.probe = packed.shippedPack(packed.probeLanes);
  }

  private static void fixtureRow(final HeapSide side, final int row, final long key, final long from, final long to,
      final long margin, final long distinct, final long group) {
    side.masks[0][row >>> 6] |= 1L << row;
    side.join[0][row] = key;
    side.from[0][row] = from;
    side.to[0][row] = to;
    side.margin[0][row] = margin;
    side.distinct[0][row] = distinct;
    side.group[0][row] = group;
  }

  private static void requireEmpty(final long[] minimum, final long[] maximum, final long[] distinct,
      final long[] bits) {
    for (int group = 0; group < minimum.length; group++) {
      if (minimum[group] != Long.MAX_VALUE || maximum[group] != Long.MIN_VALUE || distinct[group] != 0L) {
        throw new IllegalStateException("merge invocation did not reset group " + group);
      }
    }
    for (final long word : bits) {
      if (word != 0L) {
        throw new IllegalStateException("merge invocation did not clear its distinct bitmap");
      }
    }
  }

  private static void requireResult(final long[] minimum, final long[] maximum, final long[] distinct,
      final long[] expectedMinimum, final long[] expectedMaximum, final long[] expectedDistinct) {
    if (!Arrays.equals(expectedMinimum, minimum) || !Arrays.equals(expectedMaximum, maximum)
        || !Arrays.equals(expectedDistinct, distinct)) {
      throw new IllegalStateException("merge disagrees with the worked interval-join fixture");
    }
  }

  // ==================== probe walk: masks over column lanes ====================

  /**
   * A dense probe side of {@code leaves} leaves of 1,024 rows with a join key and three rostered
   * fields per row, and a small chained build table over a dense key domain; the mask admits each
   * word in full with probability {@code fullWords}, else a random subset.
   */
  @State(Scope.Benchmark)
  @SuppressWarnings("NullAway.Init") // JMH initialises the state in @Setup.
  public static class Probe {
    static final int ROWS = 1024;
    static final int FIELDS = 4;
    @Param({"256"})
    int leaves;
    @Param({"1.0", "0.5", "0.0"})
    double fullWords;
    @Param({"0.6"})
    double density;

    long[][] masks;
    long[][][] heapLanes; // [field][leaf][row]
    MemorySegment slab;
    long[][] laneBase; // [field][leaf]
    int[] denseHeads;
    int[] nextRow;
    long[] buildValues;
    long keyMinimum = 0L;
    int keySpan;
    int groups = 64;

    @Setup
    public void setUp() {
      final SplittableRandom random = new SplittableRandom(7);
      masks = new long[leaves][ROWS >>> 6];
      heapLanes = new long[FIELDS][leaves][ROWS];
      laneBase = new long[FIELDS][leaves];
      keySpan = 4096;
      final long laneBytes = (8L * ROWS + 63L) & -64L;
      slab = allocate(laneBytes * FIELDS * leaves);
      long cursor = 0L;
      for (int field = 0; field < FIELDS; field++) {
        for (int leaf = 0; leaf < leaves; leaf++) {
          laneBase[field][leaf] = cursor;
          cursor += laneBytes;
        }
      }
      for (int leaf = 0; leaf < leaves; leaf++) {
        for (int w = 0; w < masks[leaf].length; w++) {
          if (random.nextDouble() < fullWords) {
            masks[leaf][w] = -1L;
          } else {
            long word = 0L;
            for (int bit = 0; bit < 64; bit++) {
              if (random.nextDouble() < density) {
                word |= 1L << bit;
              }
            }
            masks[leaf][w] = word;
          }
        }
        for (int row = 0; row < ROWS; row++) {
          heapLanes[0][leaf][row] = random.nextInt(keySpan);
          heapLanes[1][leaf][row] = random.nextInt(1000);
          heapLanes[2][leaf][row] = random.nextInt(groups);
          heapLanes[3][leaf][row] = random.nextInt(100);
        }
        for (int field = 0; field < FIELDS; field++) {
          MemorySegment.copy(heapLanes[field][leaf], 0, slab, ValueLayout.JAVA_LONG_UNALIGNED, laneBase[field][leaf],
              ROWS);
        }
      }
      // Build table: two rows per key on average, chained.
      final int buildRows = keySpan * 2;
      buildValues = new long[buildRows * 2];
      nextRow = new int[buildRows];
      denseHeads = new int[keySpan];
      Arrays.fill(denseHeads, -1);
      for (int row = 0; row < buildRows; row++) {
        final int key = random.nextInt(keySpan);
        buildValues[row * 2] = key;
        buildValues[row * 2 + 1] = random.nextInt(groups);
        nextRow[row] = denseHeads[key];
        denseHeads[key] = row;
      }
    }

    @TearDown
    public void tearDown() {
      ALLOCATOR.releaseSlab(slab);
    }
  }

  /** Per-thread fold scratch: a flat sum and count per group. */
  @State(Scope.Thread)
  @SuppressWarnings("NullAway.Init") // JMH initialises the state in @Setup.
  public static class ProbeFold {
    final long[] sums = new long[64];
    final long[] counts = new long[64];
    final long[] values = new long[Probe.FIELDS];
    final MemorySegment[] laneSeg = new MemorySegment[Probe.FIELDS];
    final long[] laneOff = new long[Probe.FIELDS];

    @Setup(Level.Invocation)
    public void reset() {
      Arrays.fill(sums, 0L);
      Arrays.fill(counts, 0L);
    }

    long checksum() {
      long sum = 0L;
      for (int g = 0; g < sums.length; g++) {
        sum = sum * 31 + sums[g] + counts[g];
      }
      return sum;
    }
  }

  @Benchmark
  public long probeArrays(final Probe probe, final ProbeFold fold) {
    final long[] values = fold.values;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      final long[] join = probe.heapLanes[0][leaf];
      final long[] f1 = probe.heapLanes[1][leaf];
      final long[] f2 = probe.heapLanes[2][leaf];
      final long[] f3 = probe.heapLanes[3][leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = rowBase + bit;
          final long joinKey = join[row];
          int at = joinKey < probe.keyMinimum || joinKey >= probe.keyMinimum + probe.keySpan
              ? -1
              : probe.denseHeads[(int) (joinKey - probe.keyMinimum)];
          if (at < 0) {
            continue;
          }
          values[1] = f1[row];
          values[2] = f2[row];
          values[3] = f3[row];
          for (; at >= 0; at = probe.nextRow[at]) {
            final int group = (int) probe.buildValues[at * 2 + 1];
            fold.sums[group] += values[1] + values[3];
            fold.counts[group]++;
          }
        }
      }
    }
    return fold.checksum();
  }

  @Benchmark
  public long probeHandle(final Probe probe, final ProbeFold fold) {
    final long[] values = fold.values;
    final MemorySegment[] laneSeg = fold.laneSeg;
    final long[] laneOff = fold.laneOff;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      for (int field = 0; field < Probe.FIELDS; field++) {
        laneSeg[field] = probe.slab;
        laneOff[field] = probe.laneBase[field][leaf];
      }
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = rowBase + bit;
          final long joinKey = (long) SLAB_LONG.get(laneSeg[0], laneOff[0] + 8L * row);
          int at = joinKey < probe.keyMinimum || joinKey >= probe.keyMinimum + probe.keySpan
              ? -1
              : probe.denseHeads[(int) (joinKey - probe.keyMinimum)];
          if (at < 0) {
            continue;
          }
          readNumericRow(laneSeg, laneOff, row, values);
          for (; at >= 0; at = probe.nextRow[at]) {
            final int group = (int) probe.buildValues[at * 2 + 1];
            fold.sums[group] += values[1] + values[3];
            fold.counts[group]++;
          }
        }
      }
    }
    return fold.checksum();
  }

  private static void readNumericRow(final MemorySegment[] lanes, final long[] laneOffsets, final int row,
      final long[] values) {
    final long at = 8L * row;
    for (int field = 1; field < laneOffsets.length; field++) {
      values[field] = (long) SLAB_LONG.get(lanes[field], laneOffsets[field] + at);
    }
  }

  /** The slab form with the lanes hoisted into locals (one segment, per-field bases). */
  @Benchmark
  public long probeHoisted(final Probe probe, final ProbeFold fold) {
    final MemorySegment slab = probe.slab;
    final long[] values = fold.values;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      final long joinBase = probe.laneBase[0][leaf];
      final long b1 = probe.laneBase[1][leaf];
      final long b2 = probe.laneBase[2][leaf];
      final long b3 = probe.laneBase[3][leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = rowBase + bit;
          final long at = 8L * row;
          final long joinKey = (long) SLAB_LONG.get(slab, joinBase + at);
          int at0 = joinKey < probe.keyMinimum || joinKey >= probe.keyMinimum + probe.keySpan
              ? -1
              : probe.denseHeads[(int) (joinKey - probe.keyMinimum)];
          if (at0 < 0) {
            continue;
          }
          values[1] = (long) SLAB_LONG.get(slab, b1 + at);
          values[2] = (long) SLAB_LONG.get(slab, b2 + at);
          values[3] = (long) SLAB_LONG.get(slab, b3 + at);
          for (; at0 >= 0; at0 = probe.nextRow[at0]) {
            final int group = (int) probe.buildValues[at0 * 2 + 1];
            fold.sums[group] += values[1] + values[3];
            fold.counts[group]++;
          }
        }
      }
    }
    return fold.checksum();
  }

  /**
   * Locals plus a counted loop over the 64 rows of a full mask word (offsets affine in the index).
   */
  @Benchmark
  public long probeHoistedFullWord(final Probe probe, final ProbeFold fold) {
    final MemorySegment slab = probe.slab;
    final long[] values = fold.values;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      final long joinBase = probe.laneBase[0][leaf];
      final long b1 = probe.laneBase[1][leaf];
      final long b2 = probe.laneBase[2][leaf];
      final long b3 = probe.laneBase[3][leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        if (word == -1L) {
          for (int r = 0; r < 64; r++) {
            probeRowHoisted(probe, fold, slab, joinBase, b1, b2, b3, values, rowBase + r);
          }
        } else {
          while (word != 0L) {
            final int bit = Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            probeRowHoisted(probe, fold, slab, joinBase, b1, b2, b3, values, rowBase + bit);
          }
        }
      }
    }
    return fold.checksum();
  }

  private static void probeRowHoisted(final Probe probe, final ProbeFold fold, final MemorySegment slab,
      final long joinBase, final long b1, final long b2, final long b3, final long[] values, final int row) {
    final long at = 8L * row;
    final long joinKey = (long) SLAB_LONG.get(slab, joinBase + at);
    int at0 = joinKey < probe.keyMinimum || joinKey >= probe.keyMinimum + probe.keySpan
        ? -1
        : probe.denseHeads[(int) (joinKey - probe.keyMinimum)];
    if (at0 < 0) {
      return;
    }
    values[1] = (long) SLAB_LONG.get(slab, b1 + at);
    values[2] = (long) SLAB_LONG.get(slab, b2 + at);
    values[3] = (long) SLAB_LONG.get(slab, b3 + at);
    for (; at0 >= 0; at0 = probe.nextRow[at0]) {
      final int group = (int) probe.buildValues[at0 * 2 + 1];
      fold.sums[group] += values[1] + values[3];
      fold.counts[group]++;
    }
  }

  /**
   * One slice per lane and leaf (sliced once per leaf, never per row): the per-row offset is
   * {@code 8 * row} alone, affine in the row, and the lane array holds segments, which no long store
   * in the loop can alias.
   */
  @Benchmark
  public long probeSliced(final Probe probe, final ProbeFold fold) {
    final long[] values = fold.values;
    final MemorySegment[] lanes = fold.laneSeg;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      for (int field = 0; field < Probe.FIELDS; field++) {
        lanes[field] = probe.slab.asSlice(probe.laneBase[field][leaf], 8L * Probe.ROWS);
      }
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          probeRowSliced(probe, fold, lanes, values, rowBase + bit);
        }
      }
    }
    return fold.checksum();
  }

  /** {@link #probeSliced} with the counted run over a full word. */
  @Benchmark
  public long probeSlicedFullWord(final Probe probe, final ProbeFold fold) {
    final long[] values = fold.values;
    final MemorySegment[] lanes = fold.laneSeg;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      for (int field = 0; field < Probe.FIELDS; field++) {
        lanes[field] = probe.slab.asSlice(probe.laneBase[field][leaf], 8L * Probe.ROWS);
      }
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        if (word == -1L) {
          for (int r = 0; r < 64; r++) {
            probeRowSliced(probe, fold, lanes, values, rowBase + r);
          }
        } else {
          while (word != 0L) {
            final int bit = Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            probeRowSliced(probe, fold, lanes, values, rowBase + bit);
          }
        }
      }
    }
    return fold.checksum();
  }

  private static void probeRowSliced(final Probe probe, final ProbeFold fold, final MemorySegment[] lanes,
      final long[] values, final int row) {
    final long at = 8L * row;
    final long joinKey = (long) SLAB_LONG.get(lanes[0], at);
    int at0 = joinKey < probe.keyMinimum || joinKey >= probe.keyMinimum + probe.keySpan
        ? -1
        : probe.denseHeads[(int) (joinKey - probe.keyMinimum)];
    if (at0 < 0) {
      return;
    }
    for (int field = 1; field < Probe.FIELDS; field++) {
      values[field] = (long) SLAB_LONG.get(lanes[field], at);
    }
    for (; at0 >= 0; at0 = probe.nextRow[at0]) {
      final int group = (int) probe.buildValues[at0 * 2 + 1];
      fold.sums[group] += values[1] + values[3];
      fold.counts[group]++;
    }
  }

  /** The foundation form plus a counted loop over the 64 rows of a full mask word. */
  @Benchmark
  public long probeHandleFullWord(final Probe probe, final ProbeFold fold) {
    final long[] values = fold.values;
    final MemorySegment[] laneSeg = fold.laneSeg;
    final long[] laneOff = fold.laneOff;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      for (int field = 0; field < Probe.FIELDS; field++) {
        laneSeg[field] = probe.slab;
        laneOff[field] = probe.laneBase[field][leaf];
      }
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        if (word == -1L) {
          for (int r = 0; r < 64; r++) {
            probeRow(probe, fold, laneSeg, laneOff, values, rowBase + r);
          }
        } else {
          while (word != 0L) {
            final int bit = Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            probeRow(probe, fold, laneSeg, laneOff, values, rowBase + bit);
          }
        }
      }
    }
    return fold.checksum();
  }

  private static void probeRow(final Probe probe, final ProbeFold fold, final MemorySegment[] laneSeg,
      final long[] laneOff, final long[] values, final int row) {
    final long joinKey = (long) SLAB_LONG.get(laneSeg[0], laneOff[0] + 8L * row);
    int at = joinKey < probe.keyMinimum || joinKey >= probe.keyMinimum + probe.keySpan
        ? -1
        : probe.denseHeads[(int) (joinKey - probe.keyMinimum)];
    if (at < 0) {
      return;
    }
    readNumericRow(laneSeg, laneOff, row, values);
    for (; at >= 0; at = probe.nextRow[at]) {
      final int group = (int) probe.buildValues[at * 2 + 1];
      fold.sums[group] += values[1] + values[3];
      fold.counts[group]++;
    }
  }

  /** Arrays with the same full-word split, to separate the loop shape from the representation. */
  @Benchmark
  public long probeArraysFullWord(final Probe probe, final ProbeFold fold) {
    final long[] values = fold.values;
    for (int leaf = 0; leaf < probe.leaves; leaf++) {
      final long[] mask = probe.masks[leaf];
      final long[] join = probe.heapLanes[0][leaf];
      final long[] f1 = probe.heapLanes[1][leaf];
      final long[] f2 = probe.heapLanes[2][leaf];
      final long[] f3 = probe.heapLanes[3][leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        if (word == -1L) {
          for (int r = 0; r < 64; r++) {
            probeRowArrays(probe, fold, join, f1, f2, f3, values, rowBase + r);
          }
        } else {
          while (word != 0L) {
            final int bit = Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            probeRowArrays(probe, fold, join, f1, f2, f3, values, rowBase + bit);
          }
        }
      }
    }
    return fold.checksum();
  }

  private static void probeRowArrays(final Probe probe, final ProbeFold fold, final long[] join, final long[] f1,
      final long[] f2, final long[] f3, final long[] values, final int row) {
    final long joinKey = join[row];
    int at = joinKey < probe.keyMinimum || joinKey >= probe.keyMinimum + probe.keySpan
        ? -1
        : probe.denseHeads[(int) (joinKey - probe.keyMinimum)];
    if (at < 0) {
      return;
    }
    values[1] = f1[row];
    values[2] = f2[row];
    values[3] = f3[row];
    for (; at >= 0; at = probe.nextRow[at]) {
      final int group = (int) probe.buildValues[at * 2 + 1];
      fold.sums[group] += values[1] + values[3];
      fold.counts[group]++;
    }
  }

  // ==================== pack: the counting sort of one side ====================

  /** Scratch for the packing arms: bucket counts, a destination array (heap) and slab (shipped). */
  @State(Scope.Thread)
  @SuppressWarnings("NullAway.Init") // JMH initialises the state in @Setup.
  public static class PackScratch {
    int[] bucketStart;
    int[] fill;
    long[] rows;
    MemorySegment slab;

    @Setup
    public void setUp(final Packed packed) {
      bucketStart = new int[packed.slots + 1];
      fill = new int[packed.slots];
      rows = new long[packed.probe.count * STRIDE];
      slab = allocate(BucketedSide.slabBytes(packed.slots, packed.probe.count));
    }

    @TearDown
    public void tearDown() {
      ALLOCATOR.releaseSlab(slab);
    }
  }

  /** Main's heap pack of the probe side: count, then scatter five-long records into a long[]. */
  @Benchmark
  public long packArrays(final Packed packed, final PackScratch scratch) {
    final int[] bucketStart = scratch.bucketStart;
    Arrays.fill(bucketStart, 0);
    final int slots = packed.slots;
    final HeapSide side = packed.heapProbe;
    final long[][] masks = side.masks;
    for (int leaf = 0; leaf < masks.length; leaf++) {
      final long[] mask = masks[leaf];
      final long[] join = side.join[leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        while (word != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long slot = join[row];
          if (slot >= 0 && slot < slots) {
            bucketStart[(int) slot + 1]++;
          }
        }
      }
    }
    for (int slot = 0; slot < slots; slot++) {
      bucketStart[slot + 1] += bucketStart[slot];
    }
    final int[] fill = scratch.fill;
    System.arraycopy(bucketStart, 0, fill, 0, slots);
    final long[] rows = scratch.rows;
    for (int leaf = 0; leaf < masks.length; leaf++) {
      final long[] mask = masks[leaf];
      final long[] join = side.join[leaf];
      final long[] from = side.from[leaf];
      final long[] to = side.to[leaf];
      final long[] margin = side.margin[leaf];
      final long[] distinct = side.distinct[leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        while (word != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long slot = join[row];
          if (slot < 0 || slot >= slots) {
            continue;
          }
          final int at = fill[(int) slot]++ * STRIDE;
          rows[at] = from[row];
          rows[at + 1] = to[row];
          rows[at + 2] = margin[row];
          rows[at + 3] = distinct[row];
          rows[at + 4] = 0L;
        }
      }
    }
    return bucketStart[slots] + rows[0];
  }

  /** The shipped count and scatter of the probe side into a slab. */
  @Benchmark
  public long packShipped(final Packed packed, final PackScratch scratch) {
    final int[] bucketStart = scratch.bucketStart;
    Arrays.fill(bucketStart, 0);
    final int total = BucketedSide.countByBucket(packed.probeLanes, 0L, packed.slots, bucketStart);
    final BucketedSide side =
        BucketedSide.pack(scratch.slab, packed.probeLanes, 0L, packed.slots, 0L, bucketStart, total);
    return side.count + SegmentAccess.getLong(side.slab, side.rowsBase);
  }
}
