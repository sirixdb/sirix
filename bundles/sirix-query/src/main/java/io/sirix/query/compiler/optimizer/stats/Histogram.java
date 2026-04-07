package io.sirix.query.compiler.optimizer.stats;

import java.util.Arrays;

/**
 * Histogram with Most-Common-Values (MCV) tracking for selectivity estimation.
 *
 * <p>Supports two bucket modes:
 * <ul>
 *   <li><b>Equi-width</b> (default): Fixed-width buckets spanning equal value ranges.
 *       Fast O(1) bucket lookup via arithmetic. Good for uniform distributions.</li>
 *   <li><b>Equi-depth</b>: Each bucket contains approximately the same number of values.
 *       Requires O(log B) binary search for bucket lookup. Provides more uniform
 *       estimation error across skewed distributions.</li>
 * </ul></p>
 *
 * <p>Both modes support MCV tracking: top-K most frequent values are stored
 * separately with exact frequencies. Bucket counts represent only the non-MCV
 * portion of the data, so MCV and bucket estimates are additive.</p>
 *
 * <p>Design follows PostgreSQL's approach. Immutable after construction via {@link Builder}.</p>
 */
public final class Histogram {

  /** Default number of buckets. */
  public static final int DEFAULT_BUCKET_COUNT = 64;

  /** Default number of most-common values to track. */
  public static final int DEFAULT_MCV_CAPACITY = 10;

  /** Histogram bucket modes. */
  public enum HistogramType {
    /** Fixed-width buckets: O(1) lookup, good for uniform distributions. */
    EQUI_WIDTH,
    /** Equal-count buckets: O(log B) lookup, better for skewed distributions. */
    EQUI_DEPTH
  }

  private final double minValue;
  private final double maxValue;
  private final double bucketWidth; // equi-width only; 0 for equi-depth
  private final long[] bucketCounts;
  private final long totalCount;
  private final long distinctCount;
  private final HistogramType type;

  // Equi-depth: upper boundary of each bucket (sorted ascending)
  // bucketBoundaries[i] = upper bound of bucket i (inclusive)
  // null for equi-width histograms
  private final double[] bucketBoundaries;

  // Most-Common-Values: sorted by frequency descending
  private final double[] mcvValues;
  private final long[] mcvFrequencies;
  private final int mcvCount;
  private final long mcvTotalCount;

  private Histogram(double minValue, double maxValue, long[] bucketCounts,
                    long totalCount, long distinctCount,
                    double[] mcvValues, long[] mcvFrequencies, long mcvTotalCount,
                    HistogramType type, double[] bucketBoundaries) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.bucketCounts = bucketCounts;
    this.totalCount = totalCount;
    this.distinctCount = distinctCount;
    this.mcvValues = mcvValues;
    this.mcvFrequencies = mcvFrequencies;
    this.mcvCount = mcvValues != null ? mcvValues.length : 0;
    this.mcvTotalCount = mcvTotalCount;
    this.type = type;
    this.bucketBoundaries = bucketBoundaries;
    this.bucketWidth = (type == HistogramType.EQUI_WIDTH && bucketCounts.length > 0)
        ? (maxValue - minValue) / bucketCounts.length
        : 0.0;
  }

  /**
   * Estimate the selectivity of an equality predicate: value = constant.
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
   * <p>Works for both equi-width and equi-depth histograms. For equi-depth,
   * per-bucket width is computed from the bucket boundaries array.</p>
   */
  public double estimateRangeSelectivity(double low, double high) {
    if (totalCount <= 0) {
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
      final double bLow = getBucketLow(i);
      final double bHigh = getBucketHigh(i);
      final double bWidth = bHigh - bLow;

      if (bWidth <= 0) {
        // Zero-width bucket (all values equal) — count fully if in range
        matchingCount += bucketCounts[i];
        continue;
      }

      final double overlapLow = Math.max(clampedLow, bLow);
      final double overlapHigh = Math.min(clampedHigh, bHigh);

      if (overlapHigh > overlapLow) {
        final double fraction = (overlapHigh - overlapLow) / bWidth;
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

  public double estimateLessThanSelectivity(double threshold) {
    final double eps = getEpsilon();
    if (eps <= 0 || totalCount <= 0) {
      return estimateRangeSelectivity(minValue, threshold);
    }
    return estimateRangeSelectivity(minValue, threshold - eps);
  }

  public double estimateLessThanOrEqualSelectivity(double threshold) {
    return estimateRangeSelectivity(minValue, threshold);
  }

  public double estimateGreaterThanSelectivity(double threshold) {
    final double eps = getEpsilon();
    if (eps <= 0 || totalCount <= 0) {
      return estimateRangeSelectivity(threshold, maxValue);
    }
    return estimateRangeSelectivity(threshold + eps, maxValue);
  }

  public double estimateGreaterThanOrEqualSelectivity(double threshold) {
    return estimateRangeSelectivity(threshold, maxValue);
  }

  // --- Accessors ---

  public double minValue() { return minValue; }
  public double maxValue() { return maxValue; }
  public int bucketCount() { return bucketCounts.length; }
  public long totalCount() { return totalCount; }
  public long distinctCount() { return distinctCount; }
  public long bucketCountAt(int index) { return bucketCounts[index]; }
  public HistogramType histogramType() { return type; }

  public double[] mcvValues() {
    return mcvCount > 0 ? mcvValues.clone() : new double[0];
  }

  public long[] mcvFrequencies() {
    return mcvCount > 0 ? mcvFrequencies.clone() : new long[0];
  }

  public int mcvCount() { return mcvCount; }
  public long mcvTotalCount() { return mcvTotalCount; }

  /**
   * @return defensive copy of equi-depth bucket upper boundaries, empty for equi-width
   */
  public double[] bucketBoundaries() {
    return bucketBoundaries != null ? bucketBoundaries.clone() : new double[0];
  }

  // --- Internal helpers ---

  /** Get the lower bound of bucket i. */
  private double getBucketLow(int i) {
    if (bucketBoundaries != null) {
      return i == 0 ? minValue : bucketBoundaries[i - 1];
    }
    return minValue + i * bucketWidth;
  }

  /** Get the upper bound of bucket i. */
  private double getBucketHigh(int i) {
    if (bucketBoundaries != null) {
      return bucketBoundaries[i];
    }
    return minValue + (i + 1) * bucketWidth;
  }

  /** Get a small epsilon for exclusive boundary adjustment. */
  private double getEpsilon() {
    if (bucketBoundaries != null) {
      // Equi-depth: use a fraction of the overall range
      final double range = maxValue - minValue;
      return range > 0 ? range * 1e-9 : 0.0;
    }
    return bucketWidth > 0 ? bucketWidth * 1e-6 : 0.0;
  }

  private int bucketIndex(double value) {
    if (bucketBoundaries != null) {
      // Equi-depth: binary search on boundaries
      int idx = Arrays.binarySearch(bucketBoundaries, value);
      if (idx < 0) {
        idx = -(idx + 1); // insertion point
      }
      return Math.max(0, Math.min(idx, bucketCounts.length - 1));
    }
    // Equi-width: arithmetic
    if (bucketWidth <= 0) {
      return 0;
    }
    final int idx = (int) ((value - minValue) / bucketWidth);
    return Math.max(0, Math.min(idx, bucketCounts.length - 1));
  }

  // --- Builder ---

  public static final class Builder {

    private final int numBuckets;
    private double minValue = Double.MAX_VALUE;
    private double maxValue = -Double.MAX_VALUE;
    private long distinctCount;
    private int mcvCapacity = DEFAULT_MCV_CAPACITY;
    private HistogramType histogramType = HistogramType.EQUI_WIDTH;
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

    public Builder addValue(double value) {
      if (Double.isNaN(value) || Double.isInfinite(value)) {
        return this;
      }
      if (valueCount == values.length) {
        final double[] newValues = new double[values.length * 2];
        System.arraycopy(values, 0, newValues, 0, values.length);
        values = newValues;
      }
      values[valueCount++] = value;
      if (value < minValue) minValue = value;
      if (value > maxValue) maxValue = value;
      return this;
    }

    public Builder setDistinctCount(long distinctCount) {
      this.distinctCount = distinctCount;
      return this;
    }

    public Builder setMcvCapacity(int capacity) {
      if (capacity < 0) {
        throw new IllegalArgumentException("mcvCapacity must be >= 0: " + capacity);
      }
      this.mcvCapacity = capacity;
      return this;
    }

    /**
     * Set the histogram type (equi-width or equi-depth).
     * Default is {@link HistogramType#EQUI_WIDTH}.
     */
    public Builder setHistogramType(HistogramType type) {
      this.histogramType = type;
      return this;
    }

    public Histogram build() {
      if (valueCount == 0) {
        return new Histogram(0, 0, new long[numBuckets], 0, 0,
            null, null, 0, histogramType, null);
      }

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
          buckets[0] = 0;
        }
        return new Histogram(minValue, maxValue + 1.0, buckets, valueCount, distinctCount,
            mcvVals, mcvFreqs, mcvTotal, histogramType, null);
      }

      // Sort for frequency counting and equi-depth partitioning
      Arrays.sort(values, 0, valueCount);

      // --- MCV extraction (shared by both modes) ---
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
          if (maxIdx < 0 || maxFreq <= 1) break;
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

      // --- Build buckets based on histogram type ---
      if (histogramType == HistogramType.EQUI_DEPTH) {
        return buildEquiDepth(mcvVals, mcvFreqs, mcvTotal);
      } else {
        return buildEquiWidth(mcvVals, mcvFreqs, mcvTotal);
      }
    }

    private Histogram buildEquiWidth(double[] mcvVals, long[] mcvFreqs, long mcvTotal) {
      final double range = maxValue - minValue;
      final double width = range / numBuckets;
      final long[] buckets = new long[numBuckets];

      for (int i = 0; i < valueCount; i++) {
        int idx = (int) ((values[i] - minValue) / width);
        idx = Math.max(0, Math.min(idx, numBuckets - 1));
        buckets[idx]++;
      }

      subtractMcvFromBuckets(buckets, mcvVals, mcvFreqs, minValue, width);

      return new Histogram(minValue, maxValue, buckets, valueCount, distinctCount,
          mcvVals, mcvFreqs, mcvTotal, HistogramType.EQUI_WIDTH, null);
    }

    private Histogram buildEquiDepth(double[] mcvVals, long[] mcvFreqs, long mcvTotal) {
      final int effectiveBuckets = Math.min(numBuckets, valueCount);
      final int valuesPerBucket = valueCount / effectiveBuckets;
      final int remainder = valueCount % effectiveBuckets;

      final long[] buckets = new long[effectiveBuckets];
      final double[] boundaries = new double[effectiveBuckets];

      int pos = 0;
      for (int b = 0; b < effectiveBuckets; b++) {
        final int count = valuesPerBucket + (b < remainder ? 1 : 0);
        int endPos = Math.min(pos + count, valueCount);

        // Boundary = last value in this bucket
        boundaries[b] = values[endPos - 1];

        // Handle ties: if next value equals boundary, extend this bucket
        // to avoid splitting identical values across buckets
        while (endPos < valueCount && Double.compare(values[endPos], boundaries[b]) == 0) {
          endPos++;
        }

        buckets[b] = endPos - pos;
        pos = endPos;

        // If all remaining values were absorbed by ties, fill remaining buckets empty
        if (pos >= valueCount) {
          for (int r = b + 1; r < effectiveBuckets; r++) {
            boundaries[r] = maxValue;
            buckets[r] = 0;
          }
          break;
        }
      }
      // Last boundary must be maxValue
      boundaries[effectiveBuckets - 1] = maxValue;

      // Subtract MCVs from buckets
      if (mcvVals != null) {
        for (int k = 0; k < mcvVals.length; k++) {
          int idx = Arrays.binarySearch(boundaries, mcvVals[k]);
          if (idx < 0) idx = -(idx + 1);
          idx = Math.max(0, Math.min(idx, effectiveBuckets - 1));
          buckets[idx] = Math.max(0, buckets[idx] - mcvFreqs[k]);
        }
      }

      return new Histogram(minValue, maxValue, buckets, valueCount, distinctCount,
          mcvVals, mcvFreqs, mcvTotal, HistogramType.EQUI_DEPTH, boundaries);
    }

    private static void subtractMcvFromBuckets(long[] buckets, double[] mcvVals, long[] mcvFreqs,
                                                double minValue, double width) {
      if (mcvVals != null) {
        for (int k = 0; k < mcvVals.length; k++) {
          int idx = (int) ((mcvVals[k] - minValue) / width);
          idx = Math.max(0, Math.min(idx, buckets.length - 1));
          buckets[idx] = Math.max(0, buckets[idx] - mcvFreqs[k]);
        }
      }
    }
  }
}
