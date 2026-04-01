package io.sirix.query.compiler.optimizer.stats;

/**
 * Equi-width histogram for value distribution estimation.
 *
 * <p>Stores frequency counts for equi-width buckets over a value range.
 * Used by {@link SelectivityEstimator} to provide data-driven selectivity
 * estimates instead of hardcoded defaults.</p>
 *
 * <p>Design follows PostgreSQL's approach: equi-width buckets with separate
 * tracking of most-common values (MCVs). For simplicity, this first version
 * only implements equi-width buckets without MCV tracking.</p>
 *
 * <p>The histogram is immutable after construction via the {@link Builder}.</p>
 */
public final class Histogram {

  /** Default number of buckets. */
  public static final int DEFAULT_BUCKET_COUNT = 64;

  private final double minValue;
  private final double maxValue;
  private final double bucketWidth;
  private final long[] bucketCounts;
  private final long totalCount;
  private final long distinctCount;

  private Histogram(double minValue, double maxValue, long[] bucketCounts,
                    long totalCount, long distinctCount) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.bucketCounts = bucketCounts;
    this.bucketWidth = bucketCounts.length > 0
        ? (maxValue - minValue) / bucketCounts.length
        : 0.0;
    this.totalCount = totalCount;
    this.distinctCount = distinctCount;
  }

  /**
   * Estimate the selectivity of an equality predicate: value = constant.
   *
   * <p>Uses 1/NDV (number of distinct values) when distinctCount is known,
   * falling back to uniform distribution within the matching bucket.</p>
   *
   * @param value the constant value to match
   * @return estimated selectivity in (0, 1]
   */
  public double estimateEqualitySelectivity(double value) {
    if (totalCount <= 0) {
      return SelectivityEstimator.DEFAULT_SELECTIVITY;
    }
    if (value < minValue || value > maxValue) {
      return SelectivityEstimator.MIN_SELECTIVITY;
    }
    if (distinctCount > 0) {
      return Math.max(SelectivityEstimator.MIN_SELECTIVITY,
          1.0 / distinctCount);
    }
    // Fallback: uniform distribution within the bucket
    final int bucket = bucketIndex(value);
    if (bucketCounts[bucket] == 0) {
      return SelectivityEstimator.MIN_SELECTIVITY;
    }
    return Math.max(SelectivityEstimator.MIN_SELECTIVITY,
        (double) bucketCounts[bucket] / totalCount);
  }

  /**
   * Estimate the selectivity of a range predicate: value IN [low, high].
   *
   * <p>Sums the fraction of each overlapping bucket that falls within
   * the range, assuming uniform distribution within each bucket.</p>
   *
   * @param low  the lower bound (inclusive)
   * @param high the upper bound (inclusive)
   * @return estimated selectivity in [MIN_SELECTIVITY, 1.0]
   */
  public double estimateRangeSelectivity(double low, double high) {
    if (totalCount <= 0 || bucketWidth <= 0) {
      return SelectivityEstimator.DEFAULT_SELECTIVITY;
    }
    if (high < minValue || low > maxValue) {
      return SelectivityEstimator.MIN_SELECTIVITY;
    }
    // Clamp to histogram range
    final double clampedLow = Math.max(low, minValue);
    final double clampedHigh = Math.min(high, maxValue);

    // Narrow iteration to only the overlapping bucket range
    final int startBucket = bucketIndex(clampedLow);
    final int endBucket = bucketIndex(clampedHigh);

    double matchingCount = 0.0;
    for (int i = startBucket; i <= endBucket; i++) {
      final double bucketLow = minValue + i * bucketWidth;
      final double bucketHigh = bucketLow + bucketWidth;

      final double overlapLow = Math.max(clampedLow, bucketLow);
      final double overlapHigh = Math.min(clampedHigh, bucketHigh);

      if (overlapHigh > overlapLow) {
        final double fraction = (overlapHigh - overlapLow) / bucketWidth;
        matchingCount += bucketCounts[i] * fraction;
      }
    }

    final double selectivity = matchingCount / totalCount;
    return Math.max(SelectivityEstimator.MIN_SELECTIVITY,
        Math.min(1.0, selectivity));
  }

  /**
   * Estimate the selectivity of a strict less-than predicate: {@code value < threshold}.
   *
   * <p>Subtracts a small epsilon from the threshold to exclude the boundary,
   * since {@link #estimateRangeSelectivity} is inclusive on both ends.</p>
   *
   * @param threshold the upper bound (exclusive)
   * @return estimated selectivity
   */
  public double estimateLessThanSelectivity(double threshold) {
    if (bucketWidth <= 0 || totalCount <= 0) {
      return estimateRangeSelectivity(minValue, threshold);
    }
    // Subtract a fraction of bucket width to make the upper bound exclusive
    final double epsilon = bucketWidth * 1e-6;
    return estimateRangeSelectivity(minValue, threshold - epsilon);
  }

  /**
   * Estimate the selectivity of a less-than-or-equal predicate: {@code value <= threshold}.
   *
   * @param threshold the upper bound (inclusive)
   * @return estimated selectivity
   */
  public double estimateLessThanOrEqualSelectivity(double threshold) {
    return estimateRangeSelectivity(minValue, threshold);
  }

  /**
   * Estimate the selectivity of a strict greater-than predicate: value > threshold.
   *
   * <p>Adds a small epsilon to the threshold to exclude the boundary,
   * since {@link #estimateRangeSelectivity} is inclusive on both ends.</p>
   *
   * @param threshold the lower bound (exclusive)
   * @return estimated selectivity
   */
  public double estimateGreaterThanSelectivity(double threshold) {
    if (bucketWidth <= 0 || totalCount <= 0) {
      return estimateRangeSelectivity(threshold, maxValue);
    }
    // Add a fraction of bucket width to make the lower bound exclusive
    final double epsilon = bucketWidth * 1e-6;
    return estimateRangeSelectivity(threshold + epsilon, maxValue);
  }

  /**
   * Estimate the selectivity of a greater-than-or-equal predicate: value >= threshold.
   *
   * @param threshold the lower bound (inclusive)
   * @return estimated selectivity
   */
  public double estimateGreaterThanOrEqualSelectivity(double threshold) {
    return estimateRangeSelectivity(threshold, maxValue);
  }

  public double minValue() {
    return minValue;
  }

  public double maxValue() {
    return maxValue;
  }

  public int bucketCount() {
    return bucketCounts.length;
  }

  public long totalCount() {
    return totalCount;
  }

  public long distinctCount() {
    return distinctCount;
  }

  public long bucketCountAt(int index) {
    return bucketCounts[index];
  }

  private int bucketIndex(double value) {
    if (bucketWidth <= 0) {
      return 0;
    }
    final int idx = (int) ((value - minValue) / bucketWidth);
    return Math.max(0, Math.min(idx, bucketCounts.length - 1));
  }

  /**
   * Builder for constructing histograms from raw data.
   */
  public static final class Builder {

    private final int numBuckets;
    private double minValue = Double.MAX_VALUE;
    private double maxValue = -Double.MAX_VALUE;
    private long distinctCount;
    // Accumulate raw values for bucket assignment
    private double[] values;
    private int valueCount;

    public Builder() {
      this(DEFAULT_BUCKET_COUNT);
    }

    public Builder(int numBuckets) {
      if (numBuckets < 1) {
        throw new IllegalArgumentException("numBuckets must be >= 1: " + numBuckets);
      }
      this.numBuckets = numBuckets;
      this.values = new double[256];
      this.valueCount = 0;
    }

    /**
     * Add a value observation to the histogram builder.
     *
     * <p>NaN values are silently skipped — they would corrupt min/max tracking
     * and produce meaningless bucket boundaries. Infinite values are also
     * rejected because they would make {@code maxValue - minValue} infinite,
     * yielding zero-width or infinite-width buckets.</p>
     *
     * @param value the observed value (must be finite)
     * @return this builder
     */
    public Builder addValue(double value) {
      if (Double.isNaN(value) || Double.isInfinite(value)) {
        return this; // skip — would corrupt min/max and bucket math
      }
      if (valueCount == values.length) {
        final double[] newValues = new double[values.length * 2];
        System.arraycopy(values, 0, newValues, 0, values.length);
        values = newValues;
      }
      values[valueCount++] = value;
      if (value < minValue) {
        minValue = value;
      }
      if (value > maxValue) {
        maxValue = value;
      }
      return this;
    }

    /**
     * Set the number of distinct values (if known from external source).
     *
     * @param distinctCount number of distinct values
     * @return this builder
     */
    public Builder setDistinctCount(long distinctCount) {
      this.distinctCount = distinctCount;
      return this;
    }

    /**
     * Build the histogram from accumulated values.
     *
     * @return an immutable Histogram
     */
    public Histogram build() {
      if (valueCount == 0) {
        return new Histogram(0, 0, new long[numBuckets], 0, 0);
      }

      // Handle single-value case
      if (minValue == maxValue) {
        final long[] buckets = new long[numBuckets];
        buckets[0] = valueCount;
        return new Histogram(minValue, maxValue + 1.0, buckets, valueCount, distinctCount);
      }

      final double range = maxValue - minValue;
      final double width = range / numBuckets;
      final long[] buckets = new long[numBuckets];

      for (int i = 0; i < valueCount; i++) {
        int idx = (int) ((values[i] - minValue) / width);
        idx = Math.max(0, Math.min(idx, numBuckets - 1));
        buckets[idx]++;
      }

      return new Histogram(minValue, maxValue, buckets, valueCount, distinctCount);
    }
  }
}
