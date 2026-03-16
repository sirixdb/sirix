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
 * SIMD-accelerated cosine distance computation.
 *
 * <p>Performs a single-pass accumulation of dot product, normA, and normB using
 * three vector accumulators so that the vectors are read from memory only once
 * (cache-friendly). The result is {@code 1 - similarity}, i.e. a distance in
 * the range {@code [0, 2]}.</p>
 *
 * <p>Edge case: if either vector has zero magnitude the distance is defined as
 * {@code 1.0f} (completely dissimilar).</p>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class SimdCosineDistance implements DistanceFunction {

  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
  private static final int SPECIES_LENGTH = SPECIES.length();

  /** Singleton – class is stateless. */
  static final SimdCosineDistance INSTANCE = new SimdCosineDistance();

  SimdCosineDistance() {
  }

  @Override
  public float distance(final float[] a, final float[] b) {
    final int dimension = a.length;
    final int upperBound = SPECIES.loopBound(dimension);

    FloatVector dotVec = FloatVector.zero(SPECIES);
    FloatVector normAVec = FloatVector.zero(SPECIES);
    FloatVector normBVec = FloatVector.zero(SPECIES);
    int i = 0;

    // --- SIMD main loop: single pass, three accumulators ---
    for (; i < upperBound; i += SPECIES_LENGTH) {
      final FloatVector va = FloatVector.fromArray(SPECIES, a, i);
      final FloatVector vb = FloatVector.fromArray(SPECIES, b, i);
      dotVec = va.fma(vb, dotVec);       // dot += a * b
      normAVec = va.fma(va, normAVec);   // normA += a * a
      normBVec = vb.fma(vb, normBVec);   // normB += b * b
    }

    float dot = dotVec.reduceLanes(VectorOperators.ADD);
    float normA = normAVec.reduceLanes(VectorOperators.ADD);
    float normB = normBVec.reduceLanes(VectorOperators.ADD);

    // --- Scalar tail ---
    for (; i < dimension; i++) {
      final float fa = a[i];
      final float fb = b[i];
      dot += fa * fb;
      normA += fa * fa;
      normB += fb * fb;
    }

    return finalize(dot, normA, normB);
  }

  @Override
  public float distance(final MemorySegment a, final MemorySegment b, final int dimension) {
    final int upperBound = SPECIES.loopBound(dimension);

    FloatVector dotVec = FloatVector.zero(SPECIES);
    FloatVector normAVec = FloatVector.zero(SPECIES);
    FloatVector normBVec = FloatVector.zero(SPECIES);
    int i = 0;

    // --- SIMD main loop over MemorySegment ---
    for (; i < upperBound; i += SPECIES_LENGTH) {
      final long byteOffset = (long) i * Float.BYTES;
      final FloatVector va = FloatVector.fromMemorySegment(SPECIES, a, byteOffset, java.nio.ByteOrder.nativeOrder());
      final FloatVector vb = FloatVector.fromMemorySegment(SPECIES, b, byteOffset, java.nio.ByteOrder.nativeOrder());
      dotVec = va.fma(vb, dotVec);
      normAVec = va.fma(va, normAVec);
      normBVec = vb.fma(vb, normBVec);
    }

    float dot = dotVec.reduceLanes(VectorOperators.ADD);
    float normA = normAVec.reduceLanes(VectorOperators.ADD);
    float normB = normBVec.reduceLanes(VectorOperators.ADD);

    // --- Scalar tail ---
    for (; i < dimension; i++) {
      final long byteOffset = (long) i * Float.BYTES;
      final float fa = a.get(ValueLayout.JAVA_FLOAT, byteOffset);
      final float fb = b.get(ValueLayout.JAVA_FLOAT, byteOffset);
      dot += fa * fb;
      normA += fa * fa;
      normB += fb * fb;
    }

    return finalize(dot, normA, normB);
  }

  private static float finalize(final float dot, final float normA, final float normB) {
    final float denominator = (float) (Math.sqrt(normA) * Math.sqrt(normB));
    if (denominator == 0.0f) {
      return 1.0f;
    }
    // Clamp to [-1, 1] to guard against floating-point drift
    final float similarity = Math.max(-1.0f, Math.min(1.0f, dot / denominator));
    return 1.0f - similarity;
  }
}
