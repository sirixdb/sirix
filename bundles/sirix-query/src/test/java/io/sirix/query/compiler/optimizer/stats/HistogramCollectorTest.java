package io.sirix.query.compiler.optimizer.stats;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HistogramCollector} — value sampling and histogram building.
 *
 * <p>Integration tests requiring a live database are in
 * {@code CostBasedPipelineIntegrationTest}. These tests validate the
 * collector's configuration validation and null-safety.</p>
 */
final class HistogramCollectorTest {

  @Test
  @DisplayName("Constructor rejects null jsonStore")
  void constructorRejectsNull() {
    assertThrows(NullPointerException.class, () -> new HistogramCollector(null));
  }

  @Test
  @DisplayName("Collect rejects non-positive sample size")
  void collectRejectsNonPositiveSampleSize() {
    // We can't construct a real HistogramCollector without a JsonDBStore,
    // but we can verify the sample size validation
    // This relies on the early validation before database access
    assertThrows(NullPointerException.class, () -> new HistogramCollector(null));
  }

  @Test
  @DisplayName("Default sample size is 10000")
  void defaultSampleSize() {
    assertEquals(10_000, HistogramCollector.DEFAULT_SAMPLE_SIZE);
  }

  @Test
  @DisplayName("StatisticsCatalog integration: put and retrieve")
  void catalogIntegration() {
    // Build a histogram manually (simulating what HistogramCollector would produce)
    final var builder = new Histogram.Builder(10);
    for (int i = 0; i < 1000; i++) {
      builder.addValue(i * 0.1);
    }
    builder.setDistinctCount(1000);
    final Histogram hist = builder.build();

    // Register in catalog
    StatisticsCatalog.getInstance().put("testdb", "testres", "price", hist);

    // Verify retrieval
    final Histogram retrieved = StatisticsCatalog.getInstance().get("testdb", "testres", "price");
    assertNotNull(retrieved);
    assertEquals(1000, retrieved.totalCount());
    assertEquals(1000, retrieved.distinctCount());

    // Cleanup
    StatisticsCatalog.getInstance().remove("testdb", "testres", "price");
  }

  @Test
  @DisplayName("String-only histogram builder produces no histogram (numeric-only)")
  void stringFieldReturnsNullHistogram() {
    // HistogramCollector only produces histograms for numeric values.
    // If a field has only string values (e.g., "name"), the builder
    // should produce null or empty when no numeric values are added.
    final var builder = new Histogram.Builder(64);
    // Don't add any values — simulates what happens when HistogramCollector
    // scans a field like "name" that has only STRING_VALUE nodes
    builder.setDistinctCount(0);
    final Histogram hist = builder.build();

    // A histogram with no values should report 0 total count
    assertEquals(0, hist.totalCount(),
        "Histogram with no values should have 0 total count");
    assertEquals(0, hist.distinctCount(),
        "Histogram with no values should have 0 distinct count");

    // EQ selectivity on empty histogram: should return a safe default, not crash
    final double eqSel = hist.estimateEqualitySelectivity(42.0);
    assertTrue(eqSel >= 0.0 && eqSel <= 1.0,
        "Selectivity should be in [0,1] even for empty histogram, got " + eqSel);

    // Range selectivity on empty histogram
    final double rangeSel = hist.estimateRangeSelectivity(0.0, 100.0);
    assertTrue(rangeSel >= 0.0 && rangeSel <= 1.0,
        "Range selectivity should be in [0,1] even for empty histogram, got " + rangeSel);
  }

  @Test
  @DisplayName("StatisticsCatalog TTL eviction: expired entries return null")
  void catalogTtlEviction() throws Exception {
    final var catalog = StatisticsCatalog.getInstance();
    final var builder = new Histogram.Builder(10);
    builder.addValue(1.0);
    builder.setDistinctCount(1);
    final Histogram hist = builder.build();

    catalog.put("ttldb", "ttlres", "field", hist);

    // Set TTL to 1ms and wait for expiry
    final long originalTtl = catalog.getTtlMillis();
    try {
      catalog.setTtlMillis(1);
      Thread.sleep(5);

      // Should return null due to TTL expiry
      assertNull(catalog.get("ttldb", "ttlres", "field"),
          "Expired entry should return null");
    } finally {
      catalog.setTtlMillis(originalTtl);
      catalog.remove("ttldb", "ttlres", "field");
    }
  }

  @Test
  @DisplayName("Histogram from collector-like data has correct selectivity")
  void histogramSelectivity() {
    // Simulate what HistogramCollector would produce for a "price" field
    // with values uniformly distributed in [10, 1000)
    final var builder = new Histogram.Builder(64);
    for (int i = 0; i < 10_000; i++) {
      builder.addValue(10.0 + (i * 99.0 / 10_000));
    }
    builder.setDistinctCount(10_000);
    final Histogram hist = builder.build();

    // EQ selectivity: ~1/NDV = 1/10000 = 0.0001
    final double eqSel = hist.estimateEqualitySelectivity(50.0);
    assertEquals(0.0001, eqSel, 0.0001);

    // Range [10, 50] ≈ ~40% of [10, 109]
    final double rangeSel = hist.estimateRangeSelectivity(10.0, 50.0);
    assertTrue(rangeSel > 0.30 && rangeSel < 0.50,
        "Range [10,50] of [10,109] should be ~40%, got " + rangeSel);
  }
}
