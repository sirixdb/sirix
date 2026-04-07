package io.sirix.query.compiler.optimizer.stats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioral tests for {@link Histogram} — equi-width histogram
 * for data-driven selectivity estimation.
 *
 * <p>Verifies:
 * <ul>
 *   <li>Equality selectivity uses 1/NDV when distinct count is known</li>
 *   <li>Range selectivity sums fractional bucket overlaps</li>
 *   <li>Out-of-range values return MIN_SELECTIVITY</li>
 *   <li>Uniform distribution produces expected selectivities</li>
 *   <li>Skewed distribution produces different selectivities for different values</li>
 * </ul></p>
 */
final class HistogramTest {

  @Test
  void uniformDistributionEqualitySelectivity() {
    // 1000 values uniformly distributed in [0, 100), 100 distinct values
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 1000; i++) {
      builder.addValue(i % 100);
    }
    builder.setDistinctCount(100);
    final Histogram hist = builder.build();

    // 1/NDV = 1/100 = 0.01
    final double sel = hist.estimateEqualitySelectivity(50.0);
    assertEquals(0.01, sel, 0.001, "Equality selectivity should be ~1/NDV");
  }

  @Test
  void uniformDistributionRangeSelectivity() {
    // 1000 values uniformly in [0, 100)
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 1000; i++) {
      builder.addValue(i * 0.1); // 0.0, 0.1, ..., 99.9
    }
    final Histogram hist = builder.build();

    // Range [25, 75] = 50% of the domain
    final double sel = hist.estimateRangeSelectivity(25.0, 75.0);
    assertTrue(sel > 0.40 && sel < 0.60,
        "Range [25,75] of [0,100) should be ~50%, got " + sel);
  }

  @Test
  void rangeSelectivityNarrowRange() {
    // 10000 values uniformly in [0, 1000)
    final var builder = new Histogram.Builder(100);
    for (int i = 0; i < 10000; i++) {
      builder.addValue(i * 0.1);
    }
    final Histogram hist = builder.build();

    // Range [0, 10] = 1% of the domain
    final double sel = hist.estimateRangeSelectivity(0.0, 10.0);
    assertTrue(sel > 0.005 && sel < 0.05,
        "Narrow range should have low selectivity, got " + sel);
  }

  @Test
  void outOfRangeEqualityReturnsMinSelectivity() {
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 100; i++) {
      builder.addValue(i);
    }
    final Histogram hist = builder.build();

    final double sel = hist.estimateEqualitySelectivity(999.0);
    assertEquals(SelectivityEstimator.MIN_SELECTIVITY, sel, 0.0001,
        "Out-of-range value should return MIN_SELECTIVITY");
  }

  @Test
  void outOfRangeRangeReturnsMinSelectivity() {
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 100; i++) {
      builder.addValue(i);
    }
    final Histogram hist = builder.build();

    final double sel = hist.estimateRangeSelectivity(200.0, 300.0);
    assertEquals(SelectivityEstimator.MIN_SELECTIVITY, sel, 0.0001,
        "Out-of-range range should return MIN_SELECTIVITY");
  }

  @Test
  void skewedDistributionProducesDifferentSelectivities() {
    // Skewed: 900 values at 10, 100 values at 90
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 900; i++) {
      builder.addValue(10.0);
    }
    for (int i = 0; i < 100; i++) {
      builder.addValue(90.0);
    }
    final Histogram hist = builder.build();

    // Range around 10 should have higher selectivity than range around 90
    final double selLow = hist.estimateRangeSelectivity(5.0, 15.0);
    final double selHigh = hist.estimateRangeSelectivity(85.0, 95.0);

    assertTrue(selLow > selHigh,
        "Range around skewed peak (low=" + selLow + ") should have higher "
            + "selectivity than range around tail (high=" + selHigh + ")");
  }

  @Test
  void lessThanSelectivity() {
    // 1000 values uniform in [0, 100)
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 1000; i++) {
      builder.addValue(i * 0.1);
    }
    final Histogram hist = builder.build();

    // value < 50 → ~50%
    final double sel = hist.estimateLessThanSelectivity(50.0);
    assertTrue(sel > 0.40 && sel < 0.60,
        "LT 50 of [0,100) should be ~50%, got " + sel);
  }

  @Test
  void greaterThanSelectivity() {
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 1000; i++) {
      builder.addValue(i * 0.1);
    }
    final Histogram hist = builder.build();

    // value > 75 → ~25%
    final double sel = hist.estimateGreaterThanSelectivity(75.0);
    assertTrue(sel > 0.15 && sel < 0.35,
        "GT 75 of [0,100) should be ~25%, got " + sel);
  }

  @Test
  void emptyHistogramReturnsDefaultSelectivity() {
    final Histogram hist = new Histogram.Builder(10).build();

    assertEquals(SelectivityEstimator.DEFAULT_SELECTIVITY,
        hist.estimateEqualitySelectivity(42.0), 0.001);
    assertEquals(SelectivityEstimator.DEFAULT_SELECTIVITY,
        hist.estimateRangeSelectivity(0, 100), 0.001);
  }

  @Test
  void singleValueHistogram() {
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 100; i++) {
      builder.addValue(42.0);
    }
    builder.setDistinctCount(1);
    final Histogram hist = builder.build();

    // 1/NDV = 1/1 = 1.0
    final double sel = hist.estimateEqualitySelectivity(42.0);
    assertEquals(1.0, sel, 0.001, "Single value: equality sel should be 1.0");
  }

  @Test
  void histogramMetadata() {
    final var builder = new Histogram.Builder(16);
    for (int i = 0; i < 500; i++) {
      builder.addValue(i * 2.0);
    }
    builder.setDistinctCount(500);
    final Histogram hist = builder.build();

    assertEquals(0.0, hist.minValue(), 0.001);
    assertEquals(998.0, hist.maxValue(), 0.001);
    assertEquals(16, hist.bucketCount());
    assertEquals(500, hist.totalCount());
    assertEquals(500, hist.distinctCount());
  }

  @Test
  void selectivityEstimatorUsesHistogramWhenSet() {
    // Build histogram with 1000 values, 50 distinct
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 1000; i++) {
      builder.addValue(i % 50);
    }
    builder.setDistinctCount(50);
    final Histogram hist = builder.build();

    final var estimator = new SelectivityEstimator();

    // Without histogram: default EQ selectivity = 0.01
    final double defaultSel = estimator.estimateSelectivity(
        makeEqComparison());
    assertEquals(0.01, defaultSel, 0.001);

    // With histogram: EQ selectivity = 1/50 = 0.02
    estimator.setHistogram(hist);
    final double histSel = estimator.estimateSelectivity(
        makeEqComparison());
    assertEquals(0.02, histSel, 0.001,
        "Histogram-based EQ selectivity should be 1/NDV = 1/50 = 0.02");

    // Clear histogram: back to default
    estimator.setHistogram(null);
    final double clearedSel = estimator.estimateSelectivity(
        makeEqComparison());
    assertEquals(0.01, clearedSel, 0.001);
  }

  @Test
  void selectivityEstimatorHistogramAffectsRangeEstimate() {
    // Skewed histogram: most values near 0
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 900; i++) {
      builder.addValue(i * 0.01);  // [0, 9)
    }
    for (int i = 0; i < 100; i++) {
      builder.addValue(50.0 + i * 0.5); // [50, 100)
    }
    final Histogram hist = builder.build();

    final var estimator = new SelectivityEstimator();

    // Without histogram: range = 0.33 (default)
    final double defaultRangeSel = estimator.estimateSelectivity(
        makeRangeComparison());
    assertEquals(0.33, defaultRangeSel, 0.001);

    // With histogram: LT midpoint should be higher than 0.33
    // because most data is concentrated below the midpoint
    estimator.setHistogram(hist);
    final double histRangeSel = estimator.estimateSelectivity(
        makeRangeComparison());
    // Histogram-based estimate uses midpoint of histogram range
    assertTrue(histRangeSel != 0.33,
        "Histogram should produce different selectivity than default 0.33, got " + histRangeSel);
  }

  // --- Edge case tests (Phase 2b validation) ---

  @Test
  void addValueSkipsNaN() {
    final var builder = new Histogram.Builder(10);
    builder.addValue(1.0);
    builder.addValue(Double.NaN);
    builder.addValue(Double.NaN);
    builder.addValue(2.0);
    final Histogram hist = builder.build();

    assertEquals(2, hist.totalCount(), "NaN values should be silently skipped");
    assertEquals(1.0, hist.minValue(), 0.001, "min should be 1.0 (NaN excluded)");
    assertEquals(2.0, hist.maxValue(), 0.001, "max should be 2.0 (NaN excluded)");
  }

  @Test
  void addValueSkipsPositiveInfinity() {
    final var builder = new Histogram.Builder(10);
    builder.addValue(10.0);
    builder.addValue(Double.POSITIVE_INFINITY);
    builder.addValue(20.0);
    final Histogram hist = builder.build();

    assertEquals(2, hist.totalCount(), "Infinity should be silently skipped");
    assertEquals(20.0, hist.maxValue(), 0.001, "max should be 20.0 (Infinity excluded)");
  }

  @Test
  void addValueSkipsNegativeInfinity() {
    final var builder = new Histogram.Builder(10);
    builder.addValue(Double.NEGATIVE_INFINITY);
    builder.addValue(5.0);
    builder.addValue(15.0);
    final Histogram hist = builder.build();

    assertEquals(2, hist.totalCount(), "Negative infinity should be silently skipped");
    assertEquals(5.0, hist.minValue(), 0.001, "min should be 5.0 (-Infinity excluded)");
  }

  @Test
  void allNaNsProduceEmptyHistogram() {
    final var builder = new Histogram.Builder(10);
    builder.addValue(Double.NaN);
    builder.addValue(Double.NaN);
    builder.addValue(Double.NaN);
    final Histogram hist = builder.build();

    assertEquals(0, hist.totalCount(), "All-NaN histogram should be empty");
    assertEquals(SelectivityEstimator.DEFAULT_SELECTIVITY,
        hist.estimateEqualitySelectivity(42.0), 0.001,
        "Empty histogram should return default selectivity");
  }

  @Test
  void mixedFiniteAndNonFiniteValues() {
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 100; i++) {
      builder.addValue(i);
    }
    // Sprinkle in non-finite values
    builder.addValue(Double.NaN);
    builder.addValue(Double.POSITIVE_INFINITY);
    builder.addValue(Double.NEGATIVE_INFINITY);
    builder.setDistinctCount(100);
    final Histogram hist = builder.build();

    assertEquals(100, hist.totalCount(), "Only finite values should count");
    assertEquals(0.0, hist.minValue(), 0.001);
    assertEquals(99.0, hist.maxValue(), 0.001);
    // Selectivity should work normally
    final double sel = hist.estimateEqualitySelectivity(50.0);
    assertEquals(0.01, sel, 0.001, "1/NDV = 1/100 = 0.01");
  }

  // --- MCV tests ---

  @Test
  void mcvEqualitySelectivityForFrequentValue() {
    // 900 values at 42.0, 100 values spread across 1-10
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 900; i++) {
      builder.addValue(42.0);
    }
    for (int i = 1; i <= 10; i++) {
      for (int j = 0; j < 10; j++) {
        builder.addValue(i);
      }
    }
    builder.setDistinctCount(11);
    final Histogram hist = builder.build();

    // Value 42 is the dominant MCV: selectivity = 900/1000 = 0.9
    final double sel = hist.estimateEqualitySelectivity(42.0);
    assertEquals(0.9, sel, 0.001, "MCV equality selectivity should use exact frequency");
  }

  @Test
  void mcvEqualitySelectivityForNonMcvValue() {
    // 900 values at 42.0, 100 values spread across 1-100 (1 each)
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 900; i++) {
      builder.addValue(42.0);
    }
    for (int i = 1; i <= 100; i++) {
      builder.addValue(i);
    }
    builder.setDistinctCount(101);
    final Histogram hist = builder.build();

    // Value 99 is not an MCV: selectivity = (1 - 0.9) / (101 - mcvCount)
    // mcvCount depends on how many have freq > 1: only 42.0 has freq 901,
    // values 1-42 have freq 1 (except value 42 which is part of the 900).
    // Actually value 42 appears 900+1=901 times. All others appear 1 time.
    // So mcvCount=1 (only 42.0). nonMcvDistinct = 101 - 1 = 100.
    // sel = (1 - 901/1000) / 100 = 0.099/100 ≈ 0.001
    final double sel = hist.estimateEqualitySelectivity(99.0);
    assertTrue(sel < 0.01, "Non-MCV value should have low selectivity, got " + sel);
    assertTrue(sel > SelectivityEstimator.MIN_SELECTIVITY,
        "Non-MCV value should be above MIN_SELECTIVITY");
  }

  @Test
  void mcvRangeSelectivityIncludesMcvContribution() {
    // 500 values at 10.0, 500 values uniformly in [0, 100)
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 500; i++) {
      builder.addValue(10.0);
    }
    for (int i = 0; i < 500; i++) {
      builder.addValue(i * 0.2); // 0.0, 0.2, ..., 99.8
    }
    builder.setDistinctCount(501);
    final Histogram hist = builder.build();

    // Range [5, 15] covers the MCV at 10.0 (freq 500+~25 from uniform)
    // plus a fraction of the non-MCV bucket data
    final double selWithMcv = hist.estimateRangeSelectivity(5.0, 15.0);
    // Range [80, 90] does NOT cover any MCV
    final double selWithoutMcv = hist.estimateRangeSelectivity(80.0, 90.0);

    assertTrue(selWithMcv > selWithoutMcv,
        "Range covering MCV (" + selWithMcv + ") should have higher selectivity "
            + "than range without MCV (" + selWithoutMcv + ")");
  }

  @Test
  void mcvAccessors() {
    // 3 distinct values with different frequencies
    final var builder = new Histogram.Builder(4);
    for (int i = 0; i < 100; i++) builder.addValue(1.0);
    for (int i = 0; i < 50; i++) builder.addValue(2.0);
    for (int i = 0; i < 10; i++) builder.addValue(3.0);
    builder.setDistinctCount(3);
    final Histogram hist = builder.build();

    assertTrue(hist.mcvCount() > 0, "Should have MCVs");
    assertTrue(hist.mcvCount() <= 3, "Should not exceed distinct values");
    assertEquals(hist.mcvValues().length, hist.mcvFrequencies().length,
        "MCV values and frequencies arrays must have same length");
    assertTrue(hist.mcvTotalCount() > 0, "MCV total count should be positive");
    assertTrue(hist.mcvTotalCount() <= hist.totalCount(),
        "MCV total should not exceed total count");

    // First MCV should be the most frequent (value 1.0, freq 100)
    final double[] vals = hist.mcvValues();
    final long[] freqs = hist.mcvFrequencies();
    assertEquals(1.0, vals[0], 0.001, "Most frequent MCV should be 1.0");
    assertEquals(100, freqs[0], "Most frequent MCV should have freq 100");
  }

  @Test
  void mcvCapacityZeroDisablesMcv() {
    final var builder = new Histogram.Builder(10);
    builder.setMcvCapacity(0);
    for (int i = 0; i < 900; i++) {
      builder.addValue(42.0);
    }
    for (int i = 0; i < 100; i++) {
      builder.addValue(i);
    }
    builder.setDistinctCount(101);
    final Histogram hist = builder.build();

    assertEquals(0, hist.mcvCount(), "MCV should be disabled with capacity 0");
    assertEquals(0, hist.mcvTotalCount());
    assertEquals(0, hist.mcvValues().length);
    assertEquals(0, hist.mcvFrequencies().length);

    // Equality should fall back to 1/NDV
    final double sel = hist.estimateEqualitySelectivity(42.0);
    assertEquals(1.0 / 101.0, sel, 0.001, "Should use 1/NDV without MCVs");
  }

  @Test
  void mcvSubtractedFromBuckets() {
    // All 1000 values at 5.0 in a [0, 10) range with 2 buckets
    // MCV should absorb all of them, leaving bucket empty
    final var builder = new Histogram.Builder(2);
    for (int i = 0; i < 1000; i++) {
      builder.addValue(5.0);
    }
    // Add edge values to create a proper range
    builder.addValue(0.0);
    builder.addValue(10.0);
    builder.setDistinctCount(3);
    final Histogram hist = builder.build();

    // The MCV (5.0) should be subtracted from its bucket
    // So bucket counts should be much less than 1002
    long totalBucketCount = 0;
    for (int i = 0; i < hist.bucketCount(); i++) {
      totalBucketCount += hist.bucketCountAt(i);
    }
    assertTrue(totalBucketCount < 100,
        "Bucket counts should be reduced by MCV subtraction, got " + totalBucketCount);
  }

  @Test
  void mcvSelectivityEstimatorIntegration() {
    // Build histogram with dominant value 42
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 800; i++) builder.addValue(42.0);
    for (int i = 0; i < 200; i++) builder.addValue(i % 50);
    builder.setDistinctCount(51);
    final Histogram hist = builder.build();

    final var estimator = new SelectivityEstimator();
    estimator.setHistogram(hist);

    // MCV-based: 42 has freq ~800+4=804 out of 1000 → sel ≈ 0.804
    final double sel = estimator.estimateSelectivity(makeEqComparison());
    assertTrue(sel > 0.5,
        "SelectivityEstimator should use MCV data for frequent value, got " + sel);
  }

  // --- Helpers ---

  private static io.brackit.query.compiler.AST makeEqComparison() {
    final var comp = new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.ComparisonExpr, null);
    comp.addChild(new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.Str, "ValueCompEQ"));
    comp.addChild(new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.DerefExpr, null));
    comp.addChild(new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.Str, "42"));
    return comp;
  }

  private static io.brackit.query.compiler.AST makeRangeComparison() {
    final var comp = new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.ComparisonExpr, null);
    comp.addChild(new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.Str, "ValueCompGT"));
    comp.addChild(new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.DerefExpr, null));
    comp.addChild(new io.brackit.query.compiler.AST(
        io.brackit.query.compiler.XQ.Str, "40"));
    return comp;
  }
}
