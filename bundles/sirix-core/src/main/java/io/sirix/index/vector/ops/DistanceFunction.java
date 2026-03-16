/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.vector.ops;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A function that computes the distance between two vectors.
 *
 * <p>Implementations should be thread-safe and allocation-free on the hot path.
 * Two overloads are provided: one for plain {@code float[]} arrays and one for
 * zero-copy {@link MemorySegment} access that avoids materialising intermediate
 * arrays when vectors are already stored in off-heap or slotted-page memory.</p>
 */
@FunctionalInterface
public interface DistanceFunction {

  /**
   * Computes the distance between two float arrays of equal length.
   *
   * @param a first vector (must not be null)
   * @param b second vector (must not be null, same length as {@code a})
   * @return the distance (>= 0)
   */
  float distance(float[] a, float[] b);

  /**
   * Computes the distance between two vectors stored in {@link MemorySegment}s.
   *
   * <p>The default implementation copies the segments into temporary arrays and
   * delegates to {@link #distance(float[], float[])}. Concrete SIMD
   * implementations override this for zero-copy reads.</p>
   *
   * @param a         first vector segment (at least {@code dimension * 4} bytes)
   * @param b         second vector segment (at least {@code dimension * 4} bytes)
   * @param dimension number of float elements in each vector
   * @return the distance (>= 0)
   */
  default float distance(MemorySegment a, MemorySegment b, int dimension) {
    final float[] arrayA = new float[dimension];
    final float[] arrayB = new float[dimension];
    MemorySegment.copy(a, ValueLayout.JAVA_FLOAT, 0, arrayA, 0, dimension);
    MemorySegment.copy(b, ValueLayout.JAVA_FLOAT, 0, arrayB, 0, dimension);
    return distance(arrayA, arrayB);
  }
}
