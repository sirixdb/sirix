/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexScan.Op;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Shared Vector-API compare kernel and dispatch thresholds for the column-sliced scan
 * stages ({@link ProjectionColumnScan}, {@link ProjectionColumnSegmentFoldScan}).
 *
 * <p><b>Measured basis</b> ({@code ProjectionFoldKernelMicrobench}, 512-bit species, 8 long
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
   * Sparse-word compare dispatch: at most this many candidate bits go to the ntz walk;
   * denser words take {@code candidates & compareWord(...)}. Measured crossover ≈ 2 set
   * bits (walk 0.125&nbsp;ns/row at 1/64 vs vector 0.21; vector already ahead at 4/64).
   */
  static final int COMPARE_WALK_MAX_BITS = 2;

  /**
   * Sparse-word aggregate-fold dispatch: at most this many surviving bits go to the ntz
   * walk; denser words take the masked vector accumulate. Measured crossover ≈ 8 set bits
   * (walk 0.099&nbsp;ns/row at 4/64 vs masked vector 0.194; vector 2.4× ahead by 16/64).
   */
  static final int FOLD_WALK_MAX_BITS = 8;

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
    }
    return out;
  }
}
