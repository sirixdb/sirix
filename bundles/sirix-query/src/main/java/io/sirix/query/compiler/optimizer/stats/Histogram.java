package io.sirix.query.compiler.optimizer.stats;

import java.util.Arrays;

/**
 * Equi-width histogram with Most-Common-Values (MCV) tracking.
 *
 * <p>Stores frequency counts for equi-width buckets over a value range,
 * plus exact frequencies for the top-K most common values (MCVs).
 * Used by {@link SelectivityEstimator} to provide data-driven selectivity
 * estimates instead of hardcoded defaults.</p>
 *
 * <p>Design follows PostgreSQL's approach: equi-width buckets for the
 * general distribution, with separate MCV tracking for skewed values.
 * Bucket counts represent only the non-MCV portion of the data, so
 * MCV and bucket estimates are additive without double-counting.</p>
 *
 * <p>The histogram is immutable after construction via the {@link Builder}.</p>
 */
public final class Histogram {

  /** Default number of buckets. */
  public static final int DEFAULT_BUCKET_COUNT = 64;

  /** Default number of most-common values to track. */
  public static final int DEFAULT_MCV_CAPACITY = 10;

  private final double minValue;
  private final double maxValue;
  private final double bucketWidth;
  private final long[] bucketCounts;
  private final long totalCount;
  private final long distinctCount;

  // Most-Common-Values: sorted by frequency descending
  private final double[] mcvValues;
  private final long[] mcvFrequencies;
  private final int mcvCount;
  private final long mcvTotalCount;

  private Histogram(double minValue, double maxValue, long[] bucketCounts,
                    long totalCount, long distinctCount,
                    double[] mcvValues, long[] mcvFrequencies, long mcvTotalCount) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.bucketCounts = bucketCounts;
    this.bucketWidth = bucketCounts.length > 0
        ? (maxValue - minValue) / bucketCounts.length
        : 0.0;
    this.totalCount = totalCount;
    this.distinctCount = distinctCount;
    this.mcvValues = mcvValues;
    this.mcvFrequencies = mcvFrequencies;
    this.mcvCount = mcvValues != null ? mcvValues.length : 0;
    this.mcvTotalCount = mcvTotalCount;
  }

  /**
   * Estimate the selectivity of an equality predicate: value = constant.
   *
   * <p>When MCVs are available, checks them first for exact frequency.
   * For non-MCV values, estimates using the remaining (non-MCV) fraction
   * distributed across the remaining distinct values. Falls back to
   * 1/NDV or uniform bucket distribution when no MCVs exist.</p>
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

    // Check MCVs first (linear scan — mcvCount is small, typically <=10)
    if (mcvCount > 0) {
      for (int i = 0; i < mcvCount; i++) {
        if (Double.compare(mcvValues[i], value) == 0) {
          return Math.max(SelectivityEstimator.MIN_SELECTIVITY,
              (double) mcvFrequencies[i] / totalCount);
        }
      }
      // Value is not an MCV — estimate among remaining non-MCV values
      final double mcvFraction = (double) mcvTotalCount / totalCount;
      final long nonMcvDistinct = Math.max(1L, distinctCount - mcvCount);
      return Math.max(SelectivityEstimator.MIN_SELECTIVITY,
          (1.0 - mcvFraction) / nonMcvDistinct);
    }

    // No MCVs — fallback to 1/NDV
    if (distinctCount > 0) {
      return Math.max(SelectivityEstimator.MIN_SELECTIVITY,
          1.0 / distinctCount);
    }
    // Last resort: uniform distribution within the bucket
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
   * the range (representing non-MCV data), plus exact frequencies of
   * any MCVs that fall within the range. Bucket counts are already
   * reduced by MCV frequencies during construction, so the two
   * contributions are additive without double-counting.</p>
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

    // Add MCV contribution: sum frequencies of MCVs within the range
    long mcvRangeCount = 0;
    for (int i = 0; i < mcvCount; i++) {
      if (mcvValues[i] >= clampedLow && mcvValues[i] <= clampedHigh) {
        mcvRangeCount += mcvFrequencies[i];
      }
    }

    final double totalMatching = mcvRangeCount + matchingCount;
    final double selectivity = totalMatching / totalCount;
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

  /**
   * @return defensive copy of MCV values (sorted by frequency descending), empty if no MCVs
   */
  public double[] mcvValues() {
    return mcvCount > 0 ? mcvValues.clone() : new double[0];
  }

  /**
   * @return defensive copy of MCV frequencies (parallel to {@link #mcvValues()}), empty if no MCVs
   */
  public long[] mcvFrequencies() {
    return mcvCount > 0 ? mcvFrequencies.clone() : new long[0];
  }

  /**
   * @return the number of tracked MCVs
   */
  public int mcvCount() {
    return mcvCount;
  }

  /**
   * @return sum of all MCV frequencies
   */
  public long mcvTotalCount() {
    return mcvTotalCount;
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
    private int mcvCapacity = DEFAULT_MCV_CAPACITY;
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
     * Set the maximum number of most-common values to track.
     * Set to 0 to disable MCV tracking entirely.
     *
     * @param capacity MCV capacity (must be >= 0)
     * @return this builder
     */
    public Builder setMcvCapacity(int capacity) {
      if (capacity < 0) {
        throw new IllegalArgumentException("mcvCapacity must be >= 0: " + capacity);
      }
      this.mcvCapacity = capacity;
      return this;
    }

    /**
     * Build the histogram from accumulated values.
     *
     * <p>Sorts the internal value array to count frequencies, extracts
     * top-K MCVs, builds equi-width buckets from all values, then
     * subtracts MCV frequencies from bucket counts so that buckets
     * represent only the non-MCV distribution.</p>
     *
     * <p>Note: this method sorts the internal array and should only
     * be called once per builder instance.</p>
     *
     * @return an immutable Histogram
     */
    public Histogram build() {
      if (valueCount == 0) {
        return new Histogram(0, 0, new long[numBuckets], 0, 0, null, null, 0);
      }

      // Handle single-value case
      if (minValue == maxValue) {
        final long[] buckets = new long[numBuckets];
        buckets[0] = valueCount;
        double[] mcvVals = null;
        long[] mcvFreqs = null;
        long mcvTotal = 0;
        if (mcvCapacity > 0) {
          mcvVals = new double[]{minValue};
          mcvFreqs = new long[]{valueCount};
          mcvTotal = valueCount;
          buckets[0] = 0; // all values are in the MCV
        }
        return new Histogram(minValue, maxValue + 1.0, buckets, valueCount, distinctCount,
            mcvVals, mcvFreqs, mcvTotal);
      }

      // Sort for frequency counting
      Arrays.sort(values, 0, valueCount);

      // Count frequencies of each distinct value (consecutive equal groups)
      final int maxDistinct = (int) Math.min(valueCount,
          distinctCount > 0 ? distinctCount : valueCount);
      final double[] freqValues = new double[maxDistinct];
      final long[] freqCounts = new long[maxDistinct];
      int freqSize = 0;

      int runStart = 0;
      while (runStart < valueCount) {
        final double currentVal = values[runStart];
        int runEnd = runStart + 1;
        while (runEnd < valueCount && Double.compare(values[runEnd], currentVal) == 0) {
          runEnd++;
        }
        if (freqSize < maxDistinct) {
          freqValues[freqSize] = currentVal;
          freqCounts[freqSize] = runEnd - runStart;
          freqSize++;
        }
        runStart = runEnd;
      }

      // Extract top-K MCVs by frequency (simple selection for small K)
      final int effectiveMcvCount = Math.min(mcvCapacity, freqSize);
      double[] mcvVals = null;
      long[] mcvFreqs = null;
      long mcvTotal = 0;

      if (effectiveMcvCount > 0) {
        final int[] topIndices = new int[effectiveMcvCount];
        final boolean[] taken = new boolean[freqSize];
        int found = 0;

        for (int k = 0; k < effectiveMcvCount; k++) {
          long maxFreq = -1;
          int maxIdx = -1;
          for (int j = 0; j < freqSize; j++) {
            if (!taken[j] && freqCounts[j] > maxFreq) {
              maxFreq = freqCounts[j];
              maxIdx = j;
            }
          }
          if (maxIdx < 0 || maxFreq <= 1) {
            break; // no more values with frequency > 1 worth tracking
          }
          topIndices[found] = maxIdx;
          taken[maxIdx] = true;
          mcvTotal += freqCounts[maxIdx];
          found++;
        }

        if (found > 0) {
          mcvVals = new double[found];
          mcvFreqs = new long[found];
          for (int k = 0; k < found; k++) {
            mcvVals[k] = freqValues[topIndices[k]];
            mcvFreqs[k] = freqCounts[topIndices[k]];
          }
        }
      }

      // Build equi-width buckets from all values
      final double range = maxValue - minValue;
      final double width = range / numBuckets;
      final long[] buckets = new long[numBuckets];

      for (int i = 0; i < valueCount; i++) {
        int idx = (int) ((values[i] - minValue) / width);
        idx = Math.max(0, Math.min(idx, numBuckets - 1));
        buckets[idx]++;
      }

      // Subtract MCV values from bucket counts so buckets represent non-MCV distribution
      if (mcvVals != null) {
        for (int k = 0; k < mcvVals.length; k++) {
          int idx = (int) ((mcvVals[k] - minValue) / width);
          idx = Math.max(0, Math.min(idx, numBuckets - 1));
          buckets[idx] = Math.max(0, buckets[idx] - mcvFreqs[k]);
        }
      }

      return new Histogram(minValue, maxValue, buckets, valueCount, distinctCount,
          mcvVals, mcvFreqs, mcvTotal);
    }
  }
}
