/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A/B profile for the {@link ProjectionColumnSegmentFoldScan} inner loops: the current
 * scalar word-mask kernels (bit-for-bit copies of the shipped loops) against
 * {@link LongVector} variants, isolated and in situ (fused behind the real
 * {@link ProjectionIndexRowGroupCodec#unpackInto} block unpacker), plus the end-to-end
 * fold kernels for the Amdahl context. The verdict this prints decides whether the
 * fold-scan compare/fold arms adopt the Vector API — see the fold-scan class javadoc
 * for the recorded outcome.
 *
 * <p>Run directly ({@code --enable-preview --add-modules=jdk.incubator.vector}):
 * {@code java -cp <test runtime classpath> io.sirix.index.projection.ProjectionFoldKernelMicrobench}
 */
public final class ProjectionFoldKernelMicrobench {

  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
  private static final int LANES = SPECIES.length();

  /** 256 blocks x 1024 values = 2 MiB — larger than L1 so repeated passes cannot train the branch predictor on a memorized block. */
  private static final int POOL_BLOCKS = 256;
  private static final int BLOCK_VALUES = 1024;
  private static final int POOL_VALUES = POOL_BLOCKS * BLOCK_VALUES;
  private static final int WORDS_PER_BLOCK = BLOCK_VALUES >>> 6;

  public static void main(final String[] args) {
    System.out.println("== ProjectionColumnSegmentFoldScan kernel A/B ==");
    System.out.printf("species=%s (%d long lanes)%n", SPECIES, LANES);

    final Random rnd = new Random(0xF01DF01DL);
    final long[] vals = new long[POOL_VALUES];
    for (int i = 0; i < POOL_VALUES; i++) {
      vals[i] = rnd.nextLong(0, 1024);
    }
    final long lit = 511L;   // ~50% selectivity — worst case for the branchy scalar compare
    final long lo = 256L;
    final long hi = 768L;

    parityCheck(vals, lit, lo, hi);

    System.out.println("-- dense compare word (64 candidates/word, mask production) --");
    run("GT scalar (shipped denseCompareWord)", POOL_VALUES, () -> {
      long bh = 0L;
      for (int b = 0; b < POOL_VALUES; b += 64) {
        bh ^= scalarGtWord(vals, b, lit);
      }
      return bh;
    });
    run("GT vector", POOL_VALUES, () -> {
      long bh = 0L;
      for (int b = 0; b < POOL_VALUES; b += 64) {
        bh ^= vectorGtWord(vals, b, lit);
      }
      return bh;
    });
    run("BETWEEN_GE_LT scalar (shipped)", POOL_VALUES, () -> {
      long bh = 0L;
      for (int b = 0; b < POOL_VALUES; b += 64) {
        bh ^= scalarBetweenWord(vals, b, lo, hi);
      }
      return bh;
    });
    run("BETWEEN_GE_LT vector", POOL_VALUES, () -> {
      long bh = 0L;
      for (int b = 0; b < POOL_VALUES; b += 64) {
        bh ^= vectorBetweenWord(vals, b, lo, hi);
      }
      return bh;
    });

    System.out.println("-- sparse compare word (ntz walk vs full-vector-compare & candidates) --");
    for (final int density : new int[] { 1, 4, 8, 16, 32, 48 }) {
      final long[] candidates = candidateWords(density, new Random(density * 7919L));
      run(String.format("GT scalar ntz walk, %2d/64 set", density), POOL_VALUES, () -> {
        long bh = 0L;
        int wi = 0;
        for (int b = 0; b < POOL_VALUES; b += 64) {
          bh ^= scalarSparseGtWord(vals, b, candidates[wi++], lit);
        }
        return bh;
      });
      run(String.format("GT vector & mask,   %2d/64 set", density), POOL_VALUES, () -> {
        long bh = 0L;
        int wi = 0;
        for (int b = 0; b < POOL_VALUES; b += 64) {
          bh ^= candidates[wi++] & vectorGtWord(vals, b, lit);
        }
        return bh;
      });
    }

    System.out.println("-- dense aggregate fold (count/sum/min/max over full blocks) --");
    final long[] acc = new long[4];
    run("fold scalar (shipped dense arm)", POOL_VALUES, () -> {
      resetAcc(acc);
      for (int b = 0; b < POOL_VALUES; b += BLOCK_VALUES) {
        scalarFoldDenseBlock(vals, b, acc);
      }
      return acc[1] ^ acc[2] ^ acc[3];
    });
    run("fold vector", POOL_VALUES, () -> {
      resetAcc(acc);
      for (int b = 0; b < POOL_VALUES; b += BLOCK_VALUES) {
        vectorFoldDenseBlock(vals, b, acc);
      }
      return acc[1] ^ acc[2] ^ acc[3];
    });

    System.out.println("-- masked aggregate fold (surviving-rows fold at mask density) --");
    for (final int density : new int[] { 4, 16, 32, 48 }) {
      final long[] candidates = candidateWords(density, new Random(density * 104729L));
      run(String.format("fold scalar ntz walk, %2d/64 set", density), POOL_VALUES, () -> {
        resetAcc(acc);
        int wi = 0;
        for (int b = 0; b < POOL_VALUES; b += 64) {
          scalarFoldMaskedWord(vals, b, candidates[wi++], acc);
        }
        return acc[1] ^ acc[2] ^ acc[3];
      });
      run(String.format("fold vector masked,   %2d/64 set", density), POOL_VALUES, () -> {
        resetAcc(acc);
        int wi = 0;
        LongVector vsum = LongVector.zero(SPECIES);
        LongVector vmin = LongVector.broadcast(SPECIES, Long.MAX_VALUE);
        LongVector vmax = LongVector.broadcast(SPECIES, Long.MIN_VALUE);
        long count = 0;
        for (int b = 0; b < POOL_VALUES; b += 64) {
          final long word = candidates[wi++];
          count += Long.bitCount(word);
          for (int k = 0; k < 64; k += LANES) {
            final VectorMask<Long> m = VectorMask.fromLong(SPECIES, word >>> k);
            final LongVector v = LongVector.fromArray(SPECIES, vals, b + k);
            vsum = vsum.add(v, m);
            vmin = vmin.lanewise(VectorOperators.MIN, v, m);
            vmax = vmax.lanewise(VectorOperators.MAX, v, m);
          }
        }
        acc[0] += count;
        acc[1] += vsum.reduceLanes(VectorOperators.ADD);
        acc[2] = Math.min(acc[2], vmin.reduceLanes(VectorOperators.MIN));
        acc[3] = Math.max(acc[3], vmax.reduceLanes(VectorOperators.MAX));
        return acc[1] ^ acc[2] ^ acc[3];
      });
    }

    System.out.println("-- in situ: real block unpacker + dense compare (the shipped pipeline shape) --");
    final long[] scratch = new long[BLOCK_VALUES];
    for (final int width : new int[] { 8, 16, 32 }) {
      final byte[][] packed = packPool(vals, width);
      // Mid-range literal for the packed domain: ~50% selectivity at every width.
      final long wlit = (1L << (width - 1)) - 1L;
      run(String.format("unpack w=%2d only", width), POOL_VALUES, () -> {
        long bh = 0L;
        for (int b = 0; b < POOL_BLOCKS; b++) {
          ProjectionIndexRowGroupCodec.unpackInto(packed[b], 0, BLOCK_VALUES, width, 0L, scratch, 0);
          bh ^= scratch[0];
        }
        return bh;
      });
      run(String.format("unpack w=%2d + GT scalar", width), POOL_VALUES, () -> {
        long bh = 0L;
        for (int b = 0; b < POOL_BLOCKS; b++) {
          ProjectionIndexRowGroupCodec.unpackInto(packed[b], 0, BLOCK_VALUES, width, 0L, scratch, 0);
          for (int w = 0; w < WORDS_PER_BLOCK; w++) {
            bh ^= scalarGtWord(scratch, w << 6, wlit);
          }
        }
        return bh;
      });
      run(String.format("unpack w=%2d + GT vector", width), POOL_VALUES, () -> {
        long bh = 0L;
        for (int b = 0; b < POOL_BLOCKS; b++) {
          ProjectionIndexRowGroupCodec.unpackInto(packed[b], 0, BLOCK_VALUES, width, 0L, scratch, 0);
          for (int w = 0; w < WORDS_PER_BLOCK; w++) {
            bh ^= vectorGtWord(scratch, w << 6, wlit);
          }
        }
        return bh;
      });
    }

    System.out.println("-- end-to-end fold kernels (store-backed, for the Amdahl context) --");
    endToEnd(true);
    endToEnd(false);
  }

  // ==================== scalar kernels: bit-for-bit copies of the shipped loops ====================

  /** Copy of {@code denseCompareWord}'s GT arm. */
  private static long scalarGtWord(final long[] vals, final int rowBase, final long lit) {
    long out = 0L;
    for (int k = 0; k < 64; k++) {
      if (vals[rowBase + k] > lit) {
        out |= 1L << k;
      }
    }
    return out;
  }

  /** Copy of {@code denseCompareWord}'s BETWEEN_GE_LT arm. */
  private static long scalarBetweenWord(final long[] vals, final int rowBase, final long lo,
      final long hi) {
    long out = 0L;
    for (int k = 0; k < 64; k++) {
      final long v = vals[rowBase + k];
      if (v >= lo && v < hi) {
        out |= 1L << k;
      }
    }
    return out;
  }

  /** Copy of {@code evalNumericBlock}'s sparse arm, GT-specialised (the loop-invariant op switch
   *  predicts perfectly, so specialisation does not flatter the scalar side). */
  private static long scalarSparseGtWord(final long[] vals, final int rowBase, final long candidates,
      final long lit) {
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

  /** Copy of {@code foldMaskedBlock}'s dense (word == -1) arm, looped over a full block. */
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

  /** Copy of {@code foldMaskedBlock}'s sparse ntz arm. */
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

  private static long vectorGtWord(final long[] vals, final int rowBase, final long lit) {
    long out = 0L;
    for (int k = 0; k < 64; k += LANES) {
      out |= LongVector.fromArray(SPECIES, vals, rowBase + k)
          .compare(VectorOperators.GT, lit).toLong() << k;
    }
    return out;
  }

  private static long vectorBetweenWord(final long[] vals, final int rowBase, final long lo,
      final long hi) {
    long out = 0L;
    for (int k = 0; k < 64; k += LANES) {
      final LongVector v = LongVector.fromArray(SPECIES, vals, rowBase + k);
      out |= v.compare(VectorOperators.GE, lo).and(v.compare(VectorOperators.LT, hi)).toLong() << k;
    }
    return out;
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

  // ==================== end-to-end context ====================

  private static void endToEnd(final boolean allPresent) {
    final byte[] kinds = {
        ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN
    };
    final Random rnd = new Random(allPresent ? 42 : 43);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>();
    final int leaves = 1024;
    long nextOffset = 1_000;
    int totalRows = 0;
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
        page.appendRow(recordKey++, longs, bools, strings, present, unrep, nonIntegral,
            nonDoubleSource);
      }
      totalRows += rows;
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int n = encoded.columnSegmentIds().length;
      final int[] ids = new int[n];
      final long[] offsets = new long[n];
      for (int i = 0; i < n; i++) {
        ids[i] = encoded.columnSegmentIds()[i] & 0xFF;
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    final ColumnPredicate[] gt = { ColumnPredicate.numeric(0, Op.GT, 511L) };
    final ColumnPredicate[] gtBool = {
        ColumnPredicate.numeric(0, Op.GT, 511L), ColumnPredicate.booleanEq(1, true)
    };
    final long[] acc = new long[4];
    final int rows = totalRows;
    final String tag = allPresent ? "dense" : "sparse-90%";
    run(String.format("count GT (%s)", tag), rows,
        () -> ProjectionColumnSegmentFoldScan.conjunctiveCount(store, gt, fetcher));
    run(String.format("count GT & bool (%s)", tag), rows,
        () -> ProjectionColumnSegmentFoldScan.conjunctiveCount(store, gtBool, fetcher));
    run(String.format("aggregate GT (%s)", tag), rows, () -> {
      resetAcc(acc);
      ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(store, gt, 0, acc, fetcher);
      return acc[1] ^ acc[0];
    });
  }

  // ==================== scaffolding ====================

  private static void parityCheck(final long[] vals, final long lit, final long lo, final long hi) {
    final Random rnd = new Random(11);
    for (int i = 0; i < 200; i++) {
      final int base = rnd.nextInt(POOL_VALUES >>> 6) << 6;
      final long cand = rnd.nextLong();
      if (scalarGtWord(vals, base, lit) != vectorGtWord(vals, base, lit)
          || scalarBetweenWord(vals, base, lo, hi) != vectorBetweenWord(vals, base, lo, hi)
          || scalarSparseGtWord(vals, base, cand, lit) != (cand & vectorGtWord(vals, base, lit))) {
        throw new AssertionError("scalar/vector kernel divergence at base " + base);
      }
    }
    final long[] a = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
    final long[] b = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
    scalarFoldDenseBlock(vals, 0, a);
    vectorFoldDenseBlock(vals, 0, b);
    if (!Arrays.equals(a, b)) {
      throw new AssertionError("scalar/vector fold divergence: " + Arrays.toString(a) + " vs "
          + Arrays.toString(b));
    }
    System.out.println("parity: scalar and vector kernels agree");
  }

  /** One candidate word per pool 64-row word with exactly {@code density} set bits. */
  private static long[] candidateWords(final int density, final Random rnd) {
    final long[] words = new long[POOL_VALUES >>> 6];
    for (int i = 0; i < words.length; i++) {
      long w = 0L;
      int set = 0;
      while (set < density) {
        final long bit = 1L << rnd.nextInt(64);
        if ((w & bit) == 0L) {
          w |= bit;
          set++;
        }
      }
      words[i] = w;
    }
    return words;
  }

  /** LSB-first little-endian packer mirroring the codec's BitWriter (widths &le; 56 here). */
  private static byte[][] packPool(final long[] vals, final int width) {
    final long mask = (1L << width) - 1L;
    final byte[][] out = new byte[POOL_BLOCKS][];
    for (int b = 0; b < POOL_BLOCKS; b++) {
      // 8 slack bytes keep the unpacker's windowed loads in bounds, like a real segment tail.
      final byte[] packed = new byte[((BLOCK_VALUES * width + 7) >>> 3) + 8];
      long acc = 0L;
      int avail = 0;
      int pos = 0;
      for (int i = 0; i < BLOCK_VALUES; i++) {
        acc |= (vals[b * BLOCK_VALUES + i] & mask) << avail;
        avail += width;
        while (avail >= 8) {
          packed[pos++] = (byte) acc;
          acc >>>= 8;
          avail -= 8;
        }
      }
      if (avail > 0) {
        packed[pos] = (byte) acc;
      }
      out[b] = packed;
    }
    return out;
  }

  private static void resetAcc(final long[] acc) {
    acc[0] = 0;
    acc[1] = 0;
    acc[2] = Long.MAX_VALUE;
    acc[3] = Long.MIN_VALUE;
  }

  private static void run(final String label, final int rowsPerOp, final CountOp op) {
    long blackhole = 0L;
    for (int i = 0; i < 15; i++) {
      blackhole ^= op.run();
    }
    final int measureIters = 30;
    final long[] timingsNs = new long[measureIters];
    for (int i = 0; i < measureIters; i++) {
      final long t0 = System.nanoTime();
      blackhole ^= op.run();
      timingsNs[i] = System.nanoTime() - t0;
    }
    Arrays.sort(timingsNs);
    final double medianNsPerRow = timingsNs[measureIters >>> 1] / (double) rowsPerOp;
    final double bestNsPerRow = timingsNs[0] / (double) rowsPerOp;
    System.out.printf("  %-38s median=%6.3f ns/row  best=%6.3f ns/row  bh=%d%n",
        label, medianNsPerRow, bestNsPerRow, blackhole & 1L);
  }

  @FunctionalInterface
  private interface CountOp {
    long run();
  }

  private ProjectionFoldKernelMicrobench() {
  }
}
