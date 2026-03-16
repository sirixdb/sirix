/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.vector.ops;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated inner-product (dot-product) distance computation.
 *
 * <p>Returns {@code 1 - dotProduct(a, b)}, converting similarity into a
 * distance metric. For normalised vectors this is equivalent to cosine
 * distance.</p>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class SimdInnerProductDistance implements DistanceFunction {

  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
  private static final int SPECIES_LENGTH = SPECIES.length();

  /** Singleton – class is stateless. */
  static final SimdInnerProductDistance INSTANCE = new SimdInnerProductDistance();

  SimdInnerProductDistance() {
  }

  @Override
  public float distance(final float[] a, final float[] b) {
    final int dimension = a.length;
    final int upperBound = SPECIES.loopBound(dimension);

    FloatVector dotVec = FloatVector.zero(SPECIES);
    int i = 0;

    // --- SIMD main loop ---
    for (; i < upperBound; i += SPECIES_LENGTH) {
      final FloatVector va = FloatVector.fromArray(SPECIES, a, i);
      final FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
      dotVec = va.fma(vb, dotVec);  // dotVec += a * b
    }

    float dot = dotVec.reduceLanes(VectorOperators.ADD);

    // --- Scalar tail ---
    for (; i < dimension; i++) {
      dot += a[i] * b[i];
    }

    return 1.0f - dot;
  }

  @Override
  public float distance(final MemorySegment a, final MemorySegment b, final int dimension) {
    final int upperBound = SPECIES.loopBound(dimension);

    FloatVector dotVec = FloatVector.zero(SPECIES);
    int i = 0;

    // --- SIMD main loop over MemorySegment ---
    for (; i < upperBound; i += SPECIES_LENGTH) {
      final long byteOffset = (long) i * Float.BYTES;
      final FloatVector va = FloatVector.fromMemorySegment(SPECIES, a, byteOffset, java.nio.ByteOrder.nativeOrder());
      final FloatVector vb = FloatVector.fromMemorySegment(SPECIES, b, byteOffset, java.nio.ByteOrder.nativeOrder());
      dotVec = va.fma(vb, dotVec);
    }

    float dot = dotVec.reduceLanes(VectorOperators.ADD);

    // --- Scalar tail ---
    for (; i < dimension; i++) {
      final long byteOffset = (long) i * Float.BYTES;
      final float fa = a.get(ValueLayout.JAVA_FLOAT, byteOffset);
      final float fb = b.get(ValueLayout.JAVA_FLOAT, byteOffset);
      dot += fa * fb;
    }

    return 1.0f - dot;
  }
}
