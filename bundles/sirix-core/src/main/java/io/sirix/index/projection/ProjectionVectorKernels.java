/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexScan.Op;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Shared Vector-API compare kernel and dispatch thresholds for the column-sliced scan
 * stages ({@link ProjectionColumnScan}, {@link ProjectionColumnSegmentFoldScan}).
 *
 * <p><b>Measured basis</b> ({@code ProjectionFoldKernelBenchmark}, 512-bit species, 8 long
 * lanes, ~50% selectivity — the branchy scalar's worst case): the scalar compare-to-bitmask
 * loop costs ~4.1&nbsp;ns/row on dense words (the conditional {@code out |= 1L << k} is a
 * loop-carried dependence C2 cannot auto-vectorize, and the branch mispredicts), the vector
 * compare ~0.21&nbsp;ns/row — flat in density, because {@link VectorOperators#GT}-style
 * lane compares plus {@code toLong()} produce the mask word branch-free. The
 * {@code numberOfTrailingZeros} walk stays cheaper only on nearly-empty words
 * (~6.5&nbsp;ns per set bit vs the vector's ~13&nbsp;ns fixed cost per word), which is what
 * the {@link #COMPARE_WALK_MAX_BITS} / {@link #FOLD_WALK_MAX_BITS} crossovers encode.
 *
 * <p>The mask-combining word ops themselves (presence ANDs, tree AND/OR, popcounts) stay
 * bit-parallel on plain longs — 64 rows per op is already the full register width, and the
 * same profile showed nothing to reclaim there.
 */
final class ProjectionVectorKernels {

  static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
  static final int LANES = SPECIES.length();

  /**
   * Every lane mask, indexed by its own bit pattern.
   *
   * <p>{@link VectorMask#fromLong} is not free on an ISA without mask registers: AVX-512 loads a
   * bit pattern straight into a k-register, AVX2 has to materialise a full-width lane mask from
   * those bits. In a fold that builds one mask per lane group, that materialisation dominated
   * everything else — measured at 4.9-10.9 ns/row against 0.117 ns/row for the same lane adds
   * unmasked, a 46-98x penalty for the mask alone.
   *
   * <p>The domain is tiny, which is what makes the table possible at all: {@code LANES} lanes admit
   * exactly {@code 1 << LANES} distinct masks — 16 at 256-bit, 256 at 512-bit — so every mask the
   * kernels can ever need is enumerable up front and the per-group cost becomes an array load.
   * Measured 0.39-0.41 ns/row, flat in selectivity where the constructed form degraded with it.
   */
  private static final VectorMask<Long>[] LANE_MASKS = buildLaneMasks();

  /** Low {@code LANES} bits — the index into {@link #LANE_MASKS}. */
  private static final int LANE_MASK_INDEX = (1 << LANES) - 1;

  @SuppressWarnings("unchecked")
  private static VectorMask<Long>[] buildLaneMasks() {
    final VectorMask<Long>[] masks = new VectorMask[1 << LANES];
    for (int i = 0; i < masks.length; i++) {
      masks[i] = VectorMask.fromLong(SPECIES, i);
    }
    return masks;
  }

  /**
   * The lane mask for the low {@code LANES} bits of {@code bits} — a table load, not a construction.
   *
   * <p>Drop-in for {@code VectorMask.fromLong(SPECIES, bits)}: {@code fromLong} already ignores
   * bits above the lane count, and masking to {@link #LANE_MASK_INDEX} reproduces that exactly.
   */
  static VectorMask<Long> laneMask(final long bits) {
    return LANE_MASKS[(int) bits & LANE_MASK_INDEX];
  }
  static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
  static final int INT_LANES = INT_SPECIES.length();

  /**
   * Sparse-word compare dispatch: at most this many candidate bits go to the ntz walk;
   * denser words take {@code candidates & compareWord(...)}. Measured crossover ≈ 2 set
   * bits at 8 lanes (walk 0.125&nbsp;ns/row at 1/64 vs vector 0.21; vector already ahead
   * at 4/64) and ≈ 4 at 4 lanes, where the vector's fixed per-word cost is the same but
   * the same box ran it at half throughput. Sub-4-lane species (128-bit NEON on Graviton and
   * Apple Silicon, pre-AVX x86) were unmeasurable on the profiling host — its 128-bit mask
   * conversions hit a JVM slow path far off any real narrow-lane machine — so they follow the
   * 4-lane value rather than a walk-biased guess: the cost model above (fixed per-word vector
   * cost vs ~6.5&nbsp;ns per set bit) favours the vector arm past ~2 bits at every width, and
   * inventing a wider walk band would be a measurement-free regression on that hardware.
   * Re-run {@code ProjectionFoldKernelBenchmark} there to replace the inherited value.
   */
  static final int COMPARE_WALK_MAX_BITS = LANES >= 8 ? 2 : 4;

  /**
   * Sparse-word aggregate-fold dispatch: at most this many surviving bits go to the ntz
   * walk; denser words take the masked vector accumulate. Measured crossover ≈ 8 set bits
   * at both 8 and 4 lanes (walk 0.099&nbsp;ns/row at 4/64 vs masked vector 0.194; vector
   * 2.4× ahead by 16/64); sub-4-lane species inherit it for the reason above.
   */
  static final int FOLD_WALK_MAX_BITS = 8;

  /**
   * {@link #COMPARE_WALK_MAX_BITS} for the INT-lane {@link #equalsIdWord} dispatch. Int
   * species carry twice the lanes of long species on the same register width, so the vector
   * kernel's fixed per-word cost halves relative to the walk at every width — the crossover
   * scales with {@link #INT_LANES}, not {@link #LANES} (a 256-bit host has 4 long lanes but
   * 8 int lanes, and gating the id compare by the long threshold would walk words the int
   * kernel already wins). Same measured basis and the same sub-4-lane handling as above.
   */
  static final int COMPARE_WALK_MAX_BITS_INT = INT_LANES >= 8 ? 2 : 4;

  private ProjectionVectorKernels() {
  }

  /**
   * Branch-free compare of the 64 values {@code vals[rowBase .. rowBase + 64)} against the
   * predicate, returning the match bitmask (bit {@code k} ⇔ value {@code rowBase + k}
   * matches). Callers must guarantee the full 64-value window is readable; rows past the
   * leaf's tail are handled by ANDing the caller's tail-masked candidate word with the
   * result, never inside the compare.
   */
  static long compareWord(final long[] vals, final int rowBase, final Op op, final long lit,
      final long high) {
    return switch (op) {
      case GT, LT, GE, LE, EQ -> compareWordSingleBound(vals, rowBase, op, lit);
      case BETWEEN_GT_LT, BETWEEN_GT_LE, BETWEEN_GE_LT, BETWEEN_GE_LE ->
          compareWordBetween(vals, rowBase, op, lit, high);
    };
  }

  /**
   * The five one-sided comparisons. Split from the range ones so each switch stays small; the
   * operator still reaches {@code compare} as a compile-time constant, which is what makes these
   * intrinsify.
   */
  private static long compareWordSingleBound(final long[] vals, final int rowBase, final Op op,
      final long lit) {
    long out = 0L;
    switch (op) {
      case GT -> {
        for (int k = 0; k < 64; k += LANES) {
          out |= LongVector.fromArray(SPECIES, vals, rowBase + k)
              .compare(VectorOperators.GT, lit).toLong() << k;
        }
      }
      case LT -> {
        for (int k = 0; k < 64; k += LANES) {
          out |= LongVector.fromArray(SPECIES, vals, rowBase + k)
              .compare(VectorOperators.LT, lit).toLong() << k;
        }
      }
      case GE -> {
        for (int k = 0; k < 64; k += LANES) {
          out |= LongVector.fromArray(SPECIES, vals, rowBase + k)
              .compare(VectorOperators.GE, lit).toLong() << k;
        }
      }
      case LE -> {
        for (int k = 0; k < 64; k += LANES) {
          out |= LongVector.fromArray(SPECIES, vals, rowBase + k)
              .compare(VectorOperators.LE, lit).toLong() << k;
        }
      }
      case EQ -> {
        for (int k = 0; k < 64; k += LANES) {
          out |= LongVector.fromArray(SPECIES, vals, rowBase + k)
              .compare(VectorOperators.EQ, lit).toLong() << k;
        }
      }
      default -> {
        // unreachable: the caller dispatches the range operators elsewhere
      }
    }
    return out;
  }

  /** The four range comparisons, each a pair of constant-operator compares AND-ed per lane. */
  private static long compareWordBetween(final long[] vals, final int rowBase, final Op op,
      final long lit, final long high) {
    long out = 0L;
    switch (op) {
      case BETWEEN_GT_LT -> {
        for (int k = 0; k < 64; k += LANES) {
          final LongVector v = LongVector.fromArray(SPECIES, vals, rowBase + k);
          out |= v.compare(VectorOperators.GT, lit).and(v.compare(VectorOperators.LT, high))
              .toLong() << k;
        }
      }
      case BETWEEN_GT_LE -> {
        for (int k = 0; k < 64; k += LANES) {
          final LongVector v = LongVector.fromArray(SPECIES, vals, rowBase + k);
          out |= v.compare(VectorOperators.GT, lit).and(v.compare(VectorOperators.LE, high))
              .toLong() << k;
        }
      }
      case BETWEEN_GE_LT -> {
        for (int k = 0; k < 64; k += LANES) {
          final LongVector v = LongVector.fromArray(SPECIES, vals, rowBase + k);
          out |= v.compare(VectorOperators.GE, lit).and(v.compare(VectorOperators.LT, high))
              .toLong() << k;
        }
      }
      case BETWEEN_GE_LE -> {
        for (int k = 0; k < 64; k += LANES) {
          final LongVector v = LongVector.fromArray(SPECIES, vals, rowBase + k);
          out |= v.compare(VectorOperators.GE, lit).and(v.compare(VectorOperators.LE, high))
              .toLong() << k;
        }
      }
      default -> {
        // unreachable: the caller dispatches the one-sided operators elsewhere
      }
    }
    return out;
  }

  /**
   * Branch-free dict-id equality over the 64 ids {@code ids[rowBase .. rowBase + 64)},
   * returning the match bitmask — the string-EQ twin of {@link #compareWord}. Measured
   * 8× over the shipped ntz walk on dense words (1.54 vs 0.19&nbsp;ns/row, 16 int lanes)
   * and flat in density, with the walk ahead only below the
   * {@link #COMPARE_WALK_MAX_BITS_INT} crossover. The same full-window contract applies:
   * callers guarantee 64 readable ids and AND their tail-masked candidate word with the
   * result.
   */
  static long equalsIdWord(final int[] ids, final int rowBase, final int targetId) {
    long out = 0L;
    for (int k = 0; k < 64; k += INT_LANES) {
      out |= IntVector.fromArray(INT_SPECIES, ids, rowBase + k)
          .compare(VectorOperators.EQ, targetId).toLong() << k;
    }
    return out;
  }
}
