/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.vector.ops;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated Euclidean (L2) distance computation.
 *
 * <p>Uses {@link FloatVector#SPECIES_PREFERRED} so the JVM automatically picks
 * the widest SIMD register available on the host (SSE / AVX-256 / AVX-512 /
 * SVE). Tail elements that do not fill a full vector lane are handled with a
 * scalar loop.</p>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class SimdL2Distance implements DistanceFunction {

  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
  private static final int SPECIES_LENGTH = SPECIES.length();

  /** Singleton – class is stateless. */
  static final SimdL2Distance INSTANCE = new SimdL2Distance();

  SimdL2Distance() {
  }

  @Override
  public float distance(final float[] a, final float[] b) {
    final int dimension = a.length;
    final int upperBound = SPECIES.loopBound(dimension);

    FloatVector sumVec = FloatVector.zero(SPECIES);
    int i = 0;

    // --- SIMD main loop ---
    for (; i < upperBound; i += SPECIES_LENGTH) {
      final FloatVector va = FloatVector.fromArray(SPECIES, a, i);
      final FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
      final FloatVector diff = va.sub(vb);
      sumVec = diff.fma(diff, sumVec);  // sumVec += diff * diff
    }

    float sum = sumVec.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);

    // --- Scalar tail ---
    for (; i < dimension; i++) {
      final float diff = a[i] - b[i];
      sum += diff * diff;
    }

    return (float) Math.sqrt(sum);
  }

  @Override
  public float distance(final MemorySegment a, final MemorySegment b, final int dimension) {
    final int upperBound = SPECIES.loopBound(dimension);

    FloatVector sumVec = FloatVector.zero(SPECIES);
    int i = 0;

    // --- SIMD main loop over MemorySegment ---
    for (; i < upperBound; i += SPECIES_LENGTH) {
      final long byteOffset = (long) i * Float.BYTES;
      final FloatVector va = FloatVector.fromMemorySegment(SPECIES, a, byteOffset, java.nio.ByteOrder.nativeOrder());
      final FloatVector vb = FloatVector.fromMemorySegment(SPECIES, b, byteOffset, java.nio.ByteOrder.nativeOrder());
      final FloatVector diff = va.sub(vb);
      sumVec = diff.fma(diff, sumVec);
    }

    float sum = sumVec.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);

    // --- Scalar tail ---
    for (; i < dimension; i++) {
      final long byteOffset = (long) i * Float.BYTES;
      final float fa = a.get(ValueLayout.JAVA_FLOAT, byteOffset);
      final float fb = b.get(ValueLayout.JAVA_FLOAT, byteOffset);
      final float diff = fa - fb;
      sum += diff * diff;
    }

    return (float) Math.sqrt(sum);
  }
}
