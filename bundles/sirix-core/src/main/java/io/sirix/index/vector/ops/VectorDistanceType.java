/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.vector.ops;

/**
 * Supported vector distance metrics.
 *
 * <p>Each enum value provides a singleton {@link DistanceFunction} obtained via
 * {@link #getDistanceFunction(int)}. The {@code dimension} parameter is
 * reserved for future use (e.g. specialised kernels for specific widths) and
 * is currently ignored by all built-in implementations.</p>
 */
public enum VectorDistanceType {

  /** Euclidean (L2) distance: {@code sqrt(sum((a_i - b_i)^2))}. */
  L2 {
    @Override
    public DistanceFunction getDistanceFunction(final int dimension) {
      return SimdL2Distance.INSTANCE;
    }
  },

  /**
   * Cosine distance: {@code 1 - cos(a, b)}.
   *
   * <p>Range: {@code [0, 2]}. A value of {@code 0} means identical direction,
   * {@code 1} means orthogonal, {@code 2} means opposite direction.</p>
   */
  COSINE {
    @Override
    public DistanceFunction getDistanceFunction(final int dimension) {
      return SimdCosineDistance.INSTANCE;
    }
  },

  /**
   * Inner-product distance: {@code 1 - dot(a, b)}.
   *
   * <p>For normalised vectors this is equivalent to cosine distance.</p>
   */
  INNER_PRODUCT {
    @Override
    public DistanceFunction getDistanceFunction(final int dimension) {
      return SimdInnerProductDistance.INSTANCE;
    }
  };

  /**
   * Returns a {@link DistanceFunction} suitable for vectors of the given
   * dimension.
   *
   * @param dimension the number of float elements per vector (must be &gt; 0)
   * @return a reusable, thread-safe distance function
   */
  public abstract DistanceFunction getDistanceFunction(int dimension);
}
