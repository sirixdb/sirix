package io.sirix.index.vector.hnsw;

import io.sirix.index.vector.VectorNode;
import io.sirix.index.vector.ops.VectorDistanceType;

/**
 * Immutable parameter set for HNSW graph construction and search.
 *
 * @param m               number of bidirectional links per node (default 16)
 * @param mMax0           max links at layer 0 (default 2*m)
 * @param efConstruction  search width during construction (default 200)
 * @param efSearch        search width during query (default 50)
 * @param mL              level generation factor (default 1/ln(m))
 * @param distanceType    distance metric type
 * @param dimension       vector dimensionality
 */
public record HnswParams(
    int m,
    int mMax0,
    int efConstruction,
    int efSearch,
    double mL,
    VectorDistanceType distanceType,
    int dimension
) {

  /**
   * Validates all parameters on construction.
   */
  public HnswParams {
    if (m <= 0) {
      throw new IllegalArgumentException("m must be positive: " + m);
    }
    if (mMax0 <= 0) {
      throw new IllegalArgumentException("mMax0 must be positive: " + mMax0);
    }
    if (efConstruction <= 0) {
      throw new IllegalArgumentException("efConstruction must be positive: " + efConstruction);
    }
    if (efSearch <= 0) {
      throw new IllegalArgumentException("efSearch must be positive: " + efSearch);
    }
    if (mL <= 0.0) {
      throw new IllegalArgumentException("mL must be positive: " + mL);
    }
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be positive: " + dimension);
    }
    if (dimension > VectorNode.MAX_SUPPORTED_DIMENSION) {
      throw new IllegalArgumentException(
          "dimension " + dimension + " exceeds maximum " + VectorNode.MAX_SUPPORTED_DIMENSION);
    }
    if (distanceType == null) {
      throw new IllegalArgumentException("distanceType must not be null");
    }
  }

  /**
   * Creates default HNSW parameters for the given dimension and distance type.
   *
   * @param dimension    vector dimensionality
   * @param distanceType distance metric
   * @return default parameter set
   */
  public static HnswParams defaults(final int dimension, final VectorDistanceType distanceType) {
    final int defaultM = 16;
    return new HnswParams(
        defaultM,
        defaultM * 2,
        200,
        50,
        1.0 / Math.log(defaultM),
        distanceType,
        dimension
    );
  }

  /**
   * Returns the max neighbor count for a given layer.
   *
   * @param layer the HNSW layer
   * @return mMax0 for layer 0, m for all other layers
   */
  public int maxNeighbors(final int layer) {
    return layer == 0 ? mMax0 : m;
  }

  /**
   * Creates a new builder pre-populated with the given dimension and distance type.
   *
   * @param dimension    vector dimensionality
   * @param distanceType distance metric
   * @return a new builder
   */
  public static Builder builder(final int dimension, final VectorDistanceType distanceType) {
    return new Builder(dimension, distanceType);
  }

  /**
   * Mutable builder for {@link HnswParams}.
   */
  public static final class Builder {
    private int m = 16;
    private int mMax0 = 32;
    private int efConstruction = 200;
    private int efSearch = 50;
    private double mL;
    private final VectorDistanceType distanceType;
    private final int dimension;

    private Builder(final int dimension, final VectorDistanceType distanceType) {
      this.dimension = dimension;
      this.distanceType = distanceType;
      this.mL = 1.0 / Math.log(16);
    }

    public Builder m(final int m) {
      this.m = m;
      this.mMax0 = m * 2;
      this.mL = 1.0 / Math.log(m);
      return this;
    }

    public Builder mMax0(final int mMax0) {
      this.mMax0 = mMax0;
      return this;
    }

    public Builder efConstruction(final int efConstruction) {
      this.efConstruction = efConstruction;
      return this;
    }

    public Builder efSearch(final int efSearch) {
      this.efSearch = efSearch;
      return this;
    }

    public Builder mL(final double mL) {
      this.mL = mL;
      return this;
    }

    public HnswParams build() {
      return new HnswParams(m, mMax0, efConstruction, efSearch, mL, distanceType, dimension);
    }
  }
}
