/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.BitUnpackSimd;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * <b>Package note:</b> lives in {@code io.sirix.index.projection} (split across the sirix-core and
 * sirix-benchmarks artifacts, classpath-legal) so the vector arms and the unpack arms invoke the
 * SHIPPED package-private kernels — {@code ProjectionVectorKernels} and
 * {@code ProjectionIndexRowGroupCodec.unpackInto} — instead of copies that could drift from the
 * code whose dispatch thresholds this benchmark calibrates.
 *
 * A/B profile behind the projection scan stages' vector-vs-scalar dispatch: the shipped scalar
 * word-mask loops (bit-for-bit copies) against Vector-API variants, isolated and in situ behind the
 * block unpacker, plus the end-to-end fold kernels for the Amdahl context. The crossovers this
 * measures are the ones {@code ProjectionVectorKernels} encodes ({@code COMPARE_WALK_MAX_BITS},
 * {@code FOLD_WALK_MAX_BITS}) — re-run here to recalibrate them on new hardware, especially
 * sub-4-lane machines, which were unmeasurable on the original AVX-512 profiling host (its 128-bit
 * mask conversions hit a JVM slow path; see the 128-bit arms below before trusting them on x86).
 *
 * <p>
 * Scores are per row ({@code @OperationsPerInvocation}); every invocation streams the whole
 * 2&nbsp;MiB pool so repeated passes cannot train the branch predictor on a memorized block. Run
 * for example with:
 * {@code ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="ProjectionFoldKernelBenchmark"
 * -Pjmh.warmupIterations=5 -Pjmh.iterations=5 -Pjmh.fork=1}
 *
 * <p>
 * Verdicts this benchmark has already produced (512-bit species, 8 long lanes): dense compare 4.1 →
 * 0.21&nbsp;ns/row, walk ahead only ≤ ~2 candidate bits; masked fold walk ahead ≤ ~8 surviving
 * bits; dict-id EQ 1.54 → 0.19 dense; and the scalar windowed unpacker BEAT the
 * {@link BitUnpackSimd} group unpack 1.7 vs 4.5&nbsp;ns/row when the destination is a materialized
 * scratch block, which is why {@code ProjectionIndexRowGroupCodec.unpackInto} stays scalar.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ProjectionFoldKernelBenchmark {

  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
  private static final int LANES = SPECIES.length();
  private static final VectorSpecies<Long> S256 = LongVector.SPECIES_256;
  private static final VectorSpecies<Long> S128 = LongVector.SPECIES_128;

  /**
   * 256 blocks x 1024 values = 2 MiB — larger than L1 so the branch predictor cannot memorize a
   * block.
   */
  private static final int POOL_BLOCKS = 256;
  private static final int BLOCK_VALUES = 1024;
  private static final int POOL_VALUES = POOL_BLOCKS * BLOCK_VALUES;
  private static final int WORDS_PER_BLOCK = BLOCK_VALUES >>> 6;

  private static final long LIT = 511L; // ~50% selectivity over [0, 1024) — the branchy scalar's worst case

  /** Every lane mask, indexed by its own bit pattern; {@code 1 << LANES} entries is 16 at 256-bit. */
  private static final VectorMask<Long>[] LANE_MASKS = buildLaneMasks();
  private static final int LANE_MASK_INDEX = (1 << LANES) - 1;

  @SuppressWarnings("unchecked")
  private static VectorMask<Long>[] buildLaneMasks() {
    final VectorMask<Long>[] masks = new VectorMask[1 << LANES];
    for (int i = 0; i < masks.length; i++) {
      masks[i] = VectorMask.fromLong(SPECIES, i);
    }
    return masks;
  }

  private static final long LO = 256L;
  private static final long HI = 768L;
  private static final int TARGET_ID = 3; // ~5% hit rate over a 20-entry dictionary

  // ==================== states ====================

  /** Mutable per-thread scratch; benchmark-scoped states below stay immutable after setup. */
  @State(Scope.Thread)
  public static class WorkerScratch {
    final long[] acc = new long[4];
    final long[] block = new long[BLOCK_VALUES];
  }

  /** Long-value pool shared by the compare and fold benchmarks. */
  @State(Scope.Benchmark)
  public static class Values {
    final long[] vals = new long[POOL_VALUES];

    @Setup
    public void setUp() {
      final Random rnd = new Random(0xF01DF01DL);
      for (int i = 0; i < POOL_VALUES; i++) {
        vals[i] = rnd.nextLong(0, 1024);
      }
      // Parity: scalar and vector kernels must agree before anything is timed.
      for (int t = 0; t < 200; t++) {
        final int base = rnd.nextInt(POOL_VALUES >>> 6) << 6;
        final long cand = rnd.nextLong();
        if (scalarGtWord(vals, base, LIT) != vectorGtWord(vals, base, LIT)
            || scalarBetweenWord(vals, base, LO, HI) != vectorBetweenWord(vals, base, LO, HI)
            || scalarSparseGtWord(vals, base, cand, LIT) != (cand & vectorGtWord(vals, base, LIT))) {
          throw new AssertionError("scalar/vector compare divergence at base " + base);
        }
      }
      final long[] a = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
      final long[] b = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
      scalarFoldDenseBlock(vals, 0, a);
      vectorFoldDenseBlock(vals, 0, b);
      if (!Arrays.equals(a, b)) {
        throw new AssertionError("scalar/vector fold divergence");
      }
    }
  }

  /** One candidate word per pool 64-row word with exactly {@code density} set bits. */
  @State(Scope.Benchmark)
  public static class Masks {
    @Param({"1", "2", "4", "8", "16", "32", "48"})
    int density;

    final long[] candidates = new long[POOL_VALUES >>> 6];

    @Setup
    public void setUp() {
      final Random rnd = new Random(density * 7919L);
      for (int i = 0; i < candidates.length; i++) {
        long w = 0L;
        int set = 0;
        while (set < density) {
          final long bit = 1L << rnd.nextInt(64);
          if ((w & bit) == 0L) {
            w |= bit;
            set++;
          }
        }
        candidates[i] = w;
      }
    }
  }

  /** Dict-id pool for the string-EQ compare benchmarks. */
  @State(Scope.Benchmark)
  public static class Ids {
    final int[] ids = new int[POOL_VALUES];

    @Setup
    public void setUp() {
      final Random rnd = new Random(0x1D5C0DEL);
      for (int i = 0; i < POOL_VALUES; i++) {
        ids[i] = rnd.nextInt(20);
      }
      for (int t = 0; t < 100; t++) {
        final int base = rnd.nextInt(POOL_VALUES >>> 6) << 6;
        final long cand = rnd.nextLong();
        long walk = 0L;
        long c = cand;
        while (c != 0L) {
          final int bit = Long.numberOfTrailingZeros(c);
          c &= c - 1L;
          if (ids[base + bit] == TARGET_ID) {
            walk |= 1L << bit;
          }
        }
        if (walk != (cand & vectorEqIdsWord(ids, base, TARGET_ID))) {
          throw new AssertionError("string-eq scalar/vector divergence at base " + base);
        }
      }
    }
  }

  /**
   * Bit-packed pool for the unpack A/B. The scalar arm is a verified copy of
   * {@code ProjectionIndexRowGroupCodec.unpackInto}'s positional core (package-private in
   * sirix-core), checked lane-for-lane against {@link BitUnpackSimd#decodeAt} in setup so the copy
   * cannot drift from wire truth.
   */
  @State(Scope.Benchmark)
  public static class Packed {
    @Param({"8", "16", "32"})
    int width;

    final byte[][] packed = new byte[POOL_BLOCKS][];
    final MemorySegment[] segs = new MemorySegment[POOL_BLOCKS];
    final int[] lastStarts = new int[POOL_BLOCKS];
    long wlit;
    BitUnpackSimd.Plan plan;

    @Setup
    public void setUp() {
      BitUnpackSimd.setWarmupRemainingForTesting(0);
      wlit = (1L << (width - 1)) - 1L; // mid-range literal: ~50% selectivity at every width
      final Random rnd = new Random(width * 0xBEEFL);
      final long mask = BitUnpackSimd.maskFor(width);
      plan = BitUnpackSimd.planFor(width);
      final int lanes = BitUnpackSimd.lanes();
      final long[] refVals = new long[BLOCK_VALUES];
      for (int b = 0; b < POOL_BLOCKS; b++) {
        final long[] values = new long[BLOCK_VALUES];
        for (int i = 0; i < BLOCK_VALUES; i++) {
          values[i] = rnd.nextLong() & mask;
        }
        // 72 slack bytes keep both the scalar windowed loads and the vector two-load window
        // in bounds for every group, like a run sitting mid-segment with trailing data.
        final byte[] block = new byte[((BLOCK_VALUES * width + 7) >>> 3) + 72];
        long acc = 0L;
        int avail = 0;
        int pos = 0;
        for (int i = 0; i < BLOCK_VALUES; i++) {
          acc |= (values[i] & mask) << avail;
          avail += width;
          while (avail >= 8) {
            block[pos++] = (byte) acc;
            acc >>>= 8;
            avail -= 8;
          }
        }
        if (avail > 0) {
          block[pos] = (byte) acc;
        }
        packed[b] = block;
        segs[b] = MemorySegment.ofArray(block);
        lastStarts[b] = Math.min(BLOCK_VALUES - lanes, BitUnpackSimd.lastVectorGroupStart(block.length, 0, width));
        // Parity: local scalar copy vs the independent BitUnpackSimd decoder, per value.
        ProjectionIndexRowGroupCodec.unpackInto(block, 0, BLOCK_VALUES, width, 0L, refVals, 0);
        for (int i = 0; i < BLOCK_VALUES; i++) {
          if (refVals[i] != BitUnpackSimd.decodeAt(segs[b], 0, width, mask, i)) {
            throw new AssertionError("unpack copy divergence at width " + width + " value " + i);
          }
        }
      }
    }
  }

  /** Store-backed end-to-end context for the shipped fold kernels. */
  @State(Scope.Benchmark)
  public static class Store {
    @Param({"true", "false"})
    boolean allPresent;

    ProjectionColumnStore store;
    ProjectionColumnStore.ColumnSegmentFetcher fetcher;
    ProjectionIndexScan.ColumnPredicate[] gt;
    ProjectionIndexScan.ColumnPredicate[] gtBool;
    int totalRows;

    @Setup
    public void setUp() {
      final byte[] kinds =
          {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN};
      final Random rnd = new Random(allPresent
          ? 42
          : 43);
      final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
      final List<ProjectionIndexHOTStorage.RowGroupDirectory> directories = new ArrayList<>();
      // Derived, not hardcoded: the store must hold exactly POOL_VALUES rows so the end-to-end
      // numbers stay comparable with the kernel benchmarks over the same pool. The old literal
      // 1024 was calibrated when MAX_ROWS was 256; MAX_ROWS is 1024 now, which made the store 4x
      // too large and tripped the assertion below in @Setup — so this benchmark never ran at all.
      final int leaves = POOL_VALUES / ProjectionIndexRowGroupPage.MAX_ROWS;
      long nextOffset = 1_000;
      for (int leaf = 0; leaf < leaves; leaf++) {
        final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds.clone());
        final int rows = ProjectionIndexRowGroupPage.MAX_ROWS;
        final long[] longs = new long[kinds.length];
        final boolean[] bools = new boolean[kinds.length];
        final String[] strings = new String[kinds.length];
        final boolean[] present = new boolean[kinds.length];
        final boolean[] unrep = new boolean[kinds.length];
        final boolean[] nonIntegral = new boolean[kinds.length];
        final boolean[] nonDoubleSource = new boolean[kinds.length];
        long recordKey = leaf * 100_000L + 1;
        for (int r = 0; r < rows; r++) {
          longs[0] = rnd.nextLong(0, 1024);
          bools[1] = rnd.nextBoolean();
          present[0] = allPresent || rnd.nextInt(10) != 0;
          present[1] = allPresent || rnd.nextInt(10) != 0;
          page.appendRow(recordKey++, longs, bools, strings, present, unrep, nonIntegral, nonDoubleSource);
        }
        totalRows += rows;
        final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
            ProjectionIndexColumnSegmentCodec.encode(page.serialize());
        final int n = encoded.columnSegmentIds().length;
        final int[] ids = new int[n];
        final long[] offsets = new long[n];
        for (int i = 0; i < n; i++) {
          ids[i] = encoded.columnSegmentIds()[i];
          offsets[i] = nextOffset;
          segmentsByOffset.put(nextOffset, encoded.segments()[i]);
          nextOffset += 1 + encoded.segments()[i].length;
        }
        directories.add(new ProjectionIndexHOTStorage.RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets,
            new byte[ids.length][]));
      }
      fetcher = wanted -> {
        final byte[][] out = new byte[wanted.length][];
        for (int i = 0; i < wanted.length; i++) {
          out[i] = segmentsByOffset.get(wanted[i]);
        }
        return out;
      };
      store = new ProjectionColumnStore(directories);
      gt = new ProjectionIndexScan.ColumnPredicate[] {
          ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 511L)};
      gtBool = new ProjectionIndexScan.ColumnPredicate[] {
          ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 511L),
          ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)};
      if (totalRows != POOL_VALUES) {
        throw new AssertionError("store rows " + totalRows + " != " + POOL_VALUES);
      }
    }
  }

  // ==================== dense compare word ====================

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareDenseGtScalar(final Values s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= scalarGtWord(s.vals, b, LIT);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareDenseGtVector(final Values s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= vectorGtWord(s.vals, b, LIT);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareDenseBetweenScalar(final Values s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= scalarBetweenWord(s.vals, b, LO, HI);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareDenseBetweenVector(final Values s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= vectorBetweenWord(s.vals, b, LO, HI);
    }
    return bh;
  }

  // ==================== sparse compare word ====================

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareSparseGtScalarWalk(final Values s, final Masks m) {
    long bh = 0L;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= scalarSparseGtWord(s.vals, b, m.candidates[wi++], LIT);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareSparseGtVectorAndMask(final Values s, final Masks m) {
    long bh = 0L;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= m.candidates[wi++] & vectorGtWord(s.vals, b, LIT);
    }
    return bh;
  }

  // ==================== aggregate fold ====================

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldDenseScalar(final Values s, final WorkerScratch w) {
    resetAcc(w.acc);
    for (int b = 0; b < POOL_VALUES; b += BLOCK_VALUES) {
      scalarFoldDenseBlock(s.vals, b, w.acc);
    }
    return w.acc[1] ^ w.acc[2] ^ w.acc[3];
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldDenseVector(final Values s, final WorkerScratch w) {
    resetAcc(w.acc);
    for (int b = 0; b < POOL_VALUES; b += BLOCK_VALUES) {
      vectorFoldDenseBlock(s.vals, b, w.acc);
    }
    return w.acc[1] ^ w.acc[2] ^ w.acc[3];
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldMaskedScalarWalk(final Values s, final Masks m, final WorkerScratch w) {
    // Accumulators live in locals across the whole pass, exactly like the shipped
    // ProjectionColumnSegmentFoldScan.foldMaskedBlock: reloading acc[] per 64-row word would
    // charge the scalar arm memory traffic the production loop never pays, biasing the
    // crossover these arms exist to locate.
    long count = 0;
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      long word = m.candidates[wi++];
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final long v = s.vals[b + bit];
        count++;
        sum += v;
        if (v < min) {
          min = v;
        }
        if (v > max) {
          max = v;
        }
      }
    }
    w.acc[0] = count;
    w.acc[1] = sum;
    w.acc[2] = min;
    w.acc[3] = max;
    return count ^ sum ^ min ^ max;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldMaskedVectorTableFull(final Values s, final Masks m) {
    // Production shape today: table mask, but the extrema still go through MASKED lanewise
    // MIN/MAX, which AVX2 has no instruction for at 64-bit width.
    LongVector vsum = LongVector.zero(SPECIES);
    LongVector vmin = LongVector.broadcast(SPECIES, Long.MAX_VALUE);
    LongVector vmax = LongVector.broadcast(SPECIES, Long.MIN_VALUE);
    long count = 0;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      final long word = m.candidates[wi++];
      count += Long.bitCount(word);
      for (int k = 0; k < 64; k += LANES) {
        final VectorMask<Long> vm = LANE_MASKS[(int) (word >>> k) & LANE_MASK_INDEX];
        final LongVector v = LongVector.fromArray(SPECIES, s.vals, b + k);
        vsum = vsum.add(v, vm);
        vmin = vmin.lanewise(VectorOperators.MIN, v, vm);
        vmax = vmax.lanewise(VectorOperators.MAX, v, vm);
      }
    }
    return count ^ vsum.reduceLanes(VectorOperators.ADD) ^ vmin.reduceLanes(VectorOperators.MIN)
        ^ vmax.reduceLanes(VectorOperators.MAX);
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldMaskedVectorTableBlend(final Values s, final Masks m) {
    // Neutralise the unselected lanes to the fold identity, then reduce UNMASKED. The complement
    // mask needs no second table: the low LANES bits of (~word >>> k) are exactly the complement
    // of the low LANES bits of (word >>> k).
    LongVector vsum = LongVector.zero(SPECIES);
    LongVector vmin = LongVector.broadcast(SPECIES, Long.MAX_VALUE);
    LongVector vmax = LongVector.broadcast(SPECIES, Long.MIN_VALUE);
    long count = 0;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      final long word = m.candidates[wi++];
      count += Long.bitCount(word);
      for (int k = 0; k < 64; k += LANES) {
        final VectorMask<Long> vm = LANE_MASKS[(int) (word >>> k) & LANE_MASK_INDEX];
        final VectorMask<Long> nm = LANE_MASKS[(int) (~word >>> k) & LANE_MASK_INDEX];
        final LongVector v = LongVector.fromArray(SPECIES, s.vals, b + k);
        vsum = vsum.add(v, vm);
        vmin = vmin.min(v.blend(Long.MAX_VALUE, nm));
        vmax = vmax.max(v.blend(Long.MIN_VALUE, nm));
      }
    }
    return count ^ vsum.reduceLanes(VectorOperators.ADD) ^ vmin.reduceLanes(VectorOperators.MIN)
        ^ vmax.reduceLanes(VectorOperators.MAX);
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldMaskedVectorSumOnlyTable(final Values s, final Masks m) {
    // Same masked add, but the lane mask comes from a precomputed table instead of being
    // materialised per lane group. With LANES lanes there are only 1<<LANES distinct masks
    // (16 here), so the whole domain is enumerable and the per-group cost becomes an array load.
    LongVector vsum = LongVector.zero(SPECIES);
    long count = 0;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      final long word = m.candidates[wi++];
      count += Long.bitCount(word);
      for (int k = 0; k < 64; k += LANES) {
        final VectorMask<Long> vm = LANE_MASKS[(int) (word >>> k) & LANE_MASK_INDEX];
        vsum = vsum.add(LongVector.fromArray(SPECIES, s.vals, b + k), vm);
      }
    }
    return count ^ vsum.reduceLanes(VectorOperators.ADD);
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldDenseVectorSumOnly(final Values s) {
    // No mask at all: the same lane adds over the same array. Isolates the cost of building a
    // VectorMask per 4 lanes, which on AVX2 has no k-register to load into.
    LongVector vsum = LongVector.zero(SPECIES);
    for (int b = 0; b < POOL_VALUES; b += LANES) {
      vsum = vsum.add(LongVector.fromArray(SPECIES, s.vals, b));
    }
    return vsum.reduceLanes(VectorOperators.ADD);
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldMaskedVectorSumOnly(final Values s, final Masks m) {
    // Control for foldMaskedVector: identical loop with the two min/max lanewise ops removed.
    // AVX2 has no vpminsq/vpmaxsq, so masked 64-bit min/max is emulated; this isolates that cost
    // from the masked add, which does map to a single instruction.
    LongVector vsum = LongVector.zero(SPECIES);
    long count = 0;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      final long word = m.candidates[wi++];
      count += Long.bitCount(word);
      for (int k = 0; k < 64; k += LANES) {
        final VectorMask<Long> vm = VectorMask.fromLong(SPECIES, word >>> k);
        final LongVector v = LongVector.fromArray(SPECIES, s.vals, b + k);
        vsum = vsum.add(v, vm);
      }
    }
    return count ^ vsum.reduceLanes(VectorOperators.ADD);
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldMaskedVector(final Values s, final Masks m) {
    LongVector vsum = LongVector.zero(SPECIES);
    LongVector vmin = LongVector.broadcast(SPECIES, Long.MAX_VALUE);
    LongVector vmax = LongVector.broadcast(SPECIES, Long.MIN_VALUE);
    long count = 0;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      final long word = m.candidates[wi++];
      count += Long.bitCount(word);
      for (int k = 0; k < 64; k += LANES) {
        final VectorMask<Long> vm = VectorMask.fromLong(SPECIES, word >>> k);
        final LongVector v = LongVector.fromArray(SPECIES, s.vals, b + k);
        vsum = vsum.add(v, vm);
        vmin = vmin.lanewise(VectorOperators.MIN, v, vm);
        vmax = vmax.lanewise(VectorOperators.MAX, v, vm);
      }
    }
    return count ^ vsum.reduceLanes(VectorOperators.ADD) ^ vmin.reduceLanes(VectorOperators.MIN)
        ^ vmax.reduceLanes(VectorOperators.MAX);
  }

  // ==================== species scaling (crossover recalibration) ====================

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareDenseGtVector256(final Values s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= gtWord256(s.vals, b, LIT);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareDenseGtVector128(final Values s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= gtWord128(s.vals, b, LIT);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long compareSparseGtVector256AndMask(final Values s, final Masks m) {
    long bh = 0L;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= m.candidates[wi++] & gtWord256(s.vals, b, LIT);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long foldMaskedVector256(final Values s, final Masks m) {
    LongVector vsum = LongVector.zero(S256);
    LongVector vmin = LongVector.broadcast(S256, Long.MAX_VALUE);
    LongVector vmax = LongVector.broadcast(S256, Long.MIN_VALUE);
    long count = 0;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      final long word = m.candidates[wi++];
      count += Long.bitCount(word);
      for (int k = 0; k < 64; k += 4) {
        final VectorMask<Long> vm = VectorMask.fromLong(S256, word >>> k);
        final LongVector v = LongVector.fromArray(S256, s.vals, b + k);
        vsum = vsum.add(v, vm);
        vmin = vmin.lanewise(VectorOperators.MIN, v, vm);
        vmax = vmax.lanewise(VectorOperators.MAX, v, vm);
      }
    }
    return count ^ vsum.reduceLanes(VectorOperators.ADD) ^ vmin.reduceLanes(VectorOperators.MIN)
        ^ vmax.reduceLanes(VectorOperators.MAX);
  }

  // ==================== string-eq dict-id compare ====================

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long stringEqDenseScalarWalk(final Ids s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      long out = 0L;
      long c = -1L;
      while (c != 0L) {
        final int bit = Long.numberOfTrailingZeros(c);
        c &= c - 1L;
        if (s.ids[b + bit] == TARGET_ID) {
          out |= 1L << bit;
        }
      }
      bh ^= out;
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long stringEqDenseVector(final Ids s) {
    long bh = 0L;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= vectorEqIdsWord(s.ids, b, TARGET_ID);
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long stringEqSparseScalarWalk(final Ids s, final Masks m) {
    long bh = 0L;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      long out = 0L;
      long c = m.candidates[wi++];
      while (c != 0L) {
        final int bit = Long.numberOfTrailingZeros(c);
        c &= c - 1L;
        if (s.ids[b + bit] == TARGET_ID) {
          out |= 1L << bit;
        }
      }
      bh ^= out;
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long stringEqSparseVectorAndMask(final Ids s, final Masks m) {
    long bh = 0L;
    int wi = 0;
    for (int b = 0; b < POOL_VALUES; b += 64) {
      bh ^= m.candidates[wi++] & vectorEqIdsWord(s.ids, b, TARGET_ID);
    }
    return bh;
  }

  // ==================== unpack ====================

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long unpackScalarWindowed(final Packed s, final WorkerScratch w) {
    long bh = 0L;
    for (int b = 0; b < POOL_BLOCKS; b++) {
      ProjectionIndexRowGroupCodec.unpackInto(s.packed[b], 0, BLOCK_VALUES, s.width, 0L, w.block, 0);
      bh ^= w.block[0] ^ w.block[BLOCK_VALUES - 1];
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long unpackVectorGroups(final Packed s, final WorkerScratch w) {
    long bh = 0L;
    final long mask = BitUnpackSimd.maskFor(s.width);
    for (int b = 0; b < POOL_BLOCKS; b++) {
      final MemorySegment seg = s.segs[b];
      final int lastStart = s.lastStarts[b];
      final int lanes = BitUnpackSimd.lanes();
      int i = 0;
      for (; i <= lastStart; i += lanes) {
        s.plan.unpack(seg, 0, i).intoArray(w.block, i);
      }
      for (; i < BLOCK_VALUES; i++) {
        w.block[i] = BitUnpackSimd.decodeAt(seg, 0, s.width, mask, i);
      }
      bh ^= w.block[0] ^ w.block[BLOCK_VALUES - 1];
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long unpackThenCompareScalar(final Packed s, final WorkerScratch w) {
    long bh = 0L;
    for (int b = 0; b < POOL_BLOCKS; b++) {
      ProjectionIndexRowGroupCodec.unpackInto(s.packed[b], 0, BLOCK_VALUES, s.width, 0L, w.block, 0);
      for (int word = 0; word < WORDS_PER_BLOCK; word++) {
        bh ^= scalarGtWord(w.block, word << 6, s.wlit);
      }
    }
    return bh;
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long unpackThenCompareVector(final Packed s, final WorkerScratch w) {
    long bh = 0L;
    for (int b = 0; b < POOL_BLOCKS; b++) {
      ProjectionIndexRowGroupCodec.unpackInto(s.packed[b], 0, BLOCK_VALUES, s.width, 0L, w.block, 0);
      for (int word = 0; word < WORDS_PER_BLOCK; word++) {
        bh ^= vectorGtWord(w.block, word << 6, s.wlit);
      }
    }
    return bh;
  }

  // ==================== end-to-end fold kernels ====================

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long endToEndCountGt(final Store s) {
    return ProjectionColumnSegmentFoldScan.conjunctiveCount(s.store, s.gt, s.fetcher);
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long endToEndCountGtBool(final Store s) {
    return ProjectionColumnSegmentFoldScan.conjunctiveCount(s.store, s.gtBool, s.fetcher);
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long endToEndAggregateGt(final Store s, final WorkerScratch w) {
    resetAcc(w.acc);
    ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(s.store, s.gt, 0, w.acc, s.fetcher);
    return w.acc[0] ^ w.acc[1];
  }

  /**
   * The same aggregate as {@link #endToEndAggregateGt}, asking only for the slots a
   * {@code count}/{@code sum}/{@code avg} query reads. The delta is the emulated 64-bit min/max this
   * ISA has no instruction for.
   */
  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long endToEndAggregateMinGt(final Store s, final WorkerScratch w) {
    resetAcc(w.acc);
    ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(s.store, s.gt, 0, w.acc, s.fetcher,
        ProjectionColumnSegmentFoldScan.AGG_COUNT | ProjectionColumnSegmentFoldScan.AGG_MIN);
    return w.acc[0] ^ w.acc[2];
  }

  @Benchmark
  @OperationsPerInvocation(POOL_VALUES)
  public long endToEndAggregateSumGt(final Store s, final WorkerScratch w) {
    resetAcc(w.acc);
    ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(s.store, s.gt, 0, w.acc, s.fetcher,
        ProjectionColumnSegmentFoldScan.AGG_COUNT | ProjectionColumnSegmentFoldScan.AGG_SUM);
    return w.acc[0] ^ w.acc[1];
  }

  // ==================== scalar kernels: bit-for-bit copies of the shipped loops ====================

  /** Copy of the pre-vectorization dense GT compare arm (the loop the vector kernel replaced). */
  private static long scalarGtWord(final long[] vals, final int rowBase, final long lit) {
    long out = 0L;
    for (int k = 0; k < 64; k++) {
      if (vals[rowBase + k] > lit) {
        out |= 1L << k;
      }
    }
    return out;
  }

  private static long scalarBetweenWord(final long[] vals, final int rowBase, final long lo, final long hi) {
    long out = 0L;
    for (int k = 0; k < 64; k++) {
      final long v = vals[rowBase + k];
      if (v >= lo && v < hi) {
        out |= 1L << k;
      }
    }
    return out;
  }

  /**
   * Copy of the shipped sparse ntz walk, GT-specialised (the loop-invariant op switch predicts
   * perfectly).
   */
  private static long scalarSparseGtWord(final long[] vals, final int rowBase, final long candidates, final long lit) {
    long out = 0L;
    long c = candidates;
    while (c != 0L) {
      final int bit = Long.numberOfTrailingZeros(c);
      c &= c - 1L;
      if (vals[rowBase + bit] > lit) {
        out |= 1L << bit;
      }
    }
    return out;
  }

  /** Copy of the pre-vectorization dense fold arm, looped over a full block. */
  private static void scalarFoldDenseBlock(final long[] vals, final int base, final long[] acc) {
    long count = acc[0];
    long sum = acc[1];
    long min = acc[2];
    long max = acc[3];
    for (int w = 0; w < WORDS_PER_BLOCK; w++) {
      final int rowBase = base + (w << 6);
      for (int k = 0; k < 64; k++) {
        final long v = vals[rowBase + k];
        sum += v;
        if (v < min) {
          min = v;
        }
        if (v > max) {
          max = v;
        }
      }
      count += 64;
    }
    acc[0] = count;
    acc[1] = sum;
    acc[2] = min;
    acc[3] = max;
  }

  /** Copy of the shipped sparse fold ntz walk. */
  private static void scalarFoldMaskedWord(final long[] vals, final int rowBase, final long maskWord,
      final long[] acc) {
    long count = acc[0];
    long sum = acc[1];
    long min = acc[2];
    long max = acc[3];
    long word = maskWord;
    while (word != 0L) {
      final int bit = Long.numberOfTrailingZeros(word);
      word &= word - 1L;
      final long v = vals[rowBase + bit];
      count++;
      sum += v;
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }
    acc[0] = count;
    acc[1] = sum;
    acc[2] = min;
    acc[3] = max;
  }


  // ==================== Vector API candidates ====================

  /** The SHIPPED dense compare kernel (see the package note). */
  private static long vectorGtWord(final long[] vals, final int rowBase, final long lit) {
    return ProjectionVectorKernels.compareWord(vals, rowBase, ProjectionIndexScan.Op.GT, lit, 0L);
  }

  /** The SHIPPED between-compare kernel (see the package note). */
  private static long vectorBetweenWord(final long[] vals, final int rowBase, final long lo, final long hi) {
    return ProjectionVectorKernels.compareWord(vals, rowBase, ProjectionIndexScan.Op.BETWEEN_GE_LT, lo, hi);
  }

  private static long gtWord256(final long[] vals, final int rowBase, final long lit) {
    long out = 0L;
    for (int k = 0; k < 64; k += 4) {
      out |= LongVector.fromArray(S256, vals, rowBase + k).compare(VectorOperators.GT, lit).toLong() << k;
    }
    return out;
  }

  private static long gtWord128(final long[] vals, final int rowBase, final long lit) {
    long out = 0L;
    for (int k = 0; k < 64; k += 2) {
      out |= LongVector.fromArray(S128, vals, rowBase + k).compare(VectorOperators.GT, lit).toLong() << k;
    }
    return out;
  }

  /** The SHIPPED dict-id equality kernel (see the package note). */
  private static long vectorEqIdsWord(final int[] ids, final int rowBase, final int target) {
    return ProjectionVectorKernels.equalsIdWord(ids, rowBase, target);
  }

  private static void vectorFoldDenseBlock(final long[] vals, final int base, final long[] acc) {
    LongVector vsum = LongVector.zero(SPECIES);
    LongVector vmin = LongVector.broadcast(SPECIES, Long.MAX_VALUE);
    LongVector vmax = LongVector.broadcast(SPECIES, Long.MIN_VALUE);
    for (int k = 0; k < BLOCK_VALUES; k += LANES) {
      final LongVector v = LongVector.fromArray(SPECIES, vals, base + k);
      vsum = vsum.add(v);
      vmin = vmin.min(v);
      vmax = vmax.max(v);
    }
    acc[0] += BLOCK_VALUES;
    acc[1] += vsum.reduceLanes(VectorOperators.ADD);
    acc[2] = Math.min(acc[2], vmin.reduceLanes(VectorOperators.MIN));
    acc[3] = Math.max(acc[3], vmax.reduceLanes(VectorOperators.MAX));
  }

  private static void resetAcc(final long[] acc) {
    acc[0] = 0;
    acc[1] = 0;
    acc[2] = Long.MAX_VALUE;
    acc[3] = Long.MIN_VALUE;
  }
}
