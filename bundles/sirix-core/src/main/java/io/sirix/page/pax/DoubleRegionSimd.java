/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * SIMD kernels over {@link DoubleRegion}'s plain value column.
 *
 * <p>Nothing to unpack: the values are stored as the IEEE doubles the compare wants, so the whole
 * kernel is one load and one two-sided compare per eight lanes. The two-sided range does NOT use
 * the unsigned-subtract collapse the long kernels use — that trick is integer arithmetic — but a
 * double compare is a single instruction anyway, so two of them and a mask AND is already the
 * floor.
 *
 * <p>NaN never matches either bound, which is IEEE semantics and also the right answer: JSON has
 * no NaN literal, so a NaN here could only be corruption, and corrupt values failing every
 * predicate is the conservative reading.
 */
public final class DoubleRegionSimd {

  private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
  private static final int LANES = SPECIES.length();

  private DoubleRegionSimd() {
    throw new AssertionError("no instances");
  }

  /**
   * Count values in {@code [start, end)} within the inclusive bound {@code [lo, hi]}.
   *
   * <p>A bound, not operators, for the reason spelled out in {@link NumberRegionSimd}: parameters
   * do not constant-fold, and open bounds are pre-converted by the planner ({@code v > t} is
   * {@code v >= nextUp(t)} for finite doubles).
   */
  public static long countRange(final MemorySegment payload, final DoubleRegion.Header h,
      final int start, final int end, final double lo, final double hi) {
    if (start >= end || !(lo <= hi)) {
      return 0L;  // an empty or NaN bound matches nothing
    }
    long count = 0;
    int i = start;
    if (BitUnpackSimd.vectorProfitable(end - start)) {
      final DoubleVector loV = DoubleVector.broadcast(SPECIES, lo);
      final DoubleVector hiV = DoubleVector.broadcast(SPECIES, hi);
      for (; i <= end - LANES; i += LANES) {
        final DoubleVector v = DoubleVector.fromMemorySegment(SPECIES, payload,
            h.valuesOffset + (long) i * Double.BYTES, ByteOrder.LITTLE_ENDIAN);
        count += v.compare(VectorOperators.GE, loV)
                  .and(v.compare(VectorOperators.LE, hiV))
                  .trueCount();
      }
    }
    for (; i < end; i++) {
      final double v = DoubleRegion.decodeValueAt(payload, h, i);
      if (v >= lo && v <= hi) {
        count++;
      }
    }
    return count;
  }
}
