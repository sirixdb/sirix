/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.metrics;

import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that cache-size gauges are wired correctly to the live
 * {@link io.sirix.cache.BufferManagerImpl}:
 *
 * <ul>
 *   <li>Suppliers never throw, even before any database has opened
 *       (peek-returns-null path).</li>
 *   <li>After a database opens and a commit happens, the {@code _max_bytes}
 *       gauges report &gt; 0 (proves the BufferManager is observable).</li>
 *   <li>{@code _size_bytes} gauges always return a non-negative value.</li>
 * </ul>
 *
 * <p>This test deliberately does NOT assert exact occupancy: cache promotion is
 * a policy detail (most-recent-page shortcut, TIL bypass during writes, etc.)
 * and would couple a metrics test to the cache implementation.
 */
final class CacheSizeMetricsIntegrationTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.closeEverything();
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void cacheSizeGauges_areWiredAndReportSensibleValues() {
    // Capture supplier references via a bridge so we can poll without depending
    // on Micrometer at the test level.
    final Map<String, LongSupplier> gauges = new HashMap<>();
    SirixMetricsRegistry.install((name, help, supplier) -> gauges.put(name, supplier));

    final LongSupplier recordPageSize = gauges.get("sirix_record_page_cache_size_bytes");
    final LongSupplier hotLeafSize = gauges.get("sirix_hot_leaf_page_cache_size_bytes");
    final LongSupplier fragmentSize = gauges.get("sirix_record_page_fragment_cache_size_bytes");
    final LongSupplier maxRecordPage = gauges.get("sirix_record_page_cache_max_bytes");
    final LongSupplier maxHotLeaf = gauges.get("sirix_hot_leaf_page_cache_max_bytes");
    final LongSupplier maxFragment = gauges.get("sirix_record_page_fragment_cache_max_bytes");
    final LongSupplier allocatorBytes = gauges.get("sirix_allocator_physical_memory_bytes");
    assertNotNull(recordPageSize, "record-page size gauge must be registered");
    assertNotNull(hotLeafSize, "HOT-leaf size gauge must be registered");
    assertNotNull(fragmentSize, "record-page-fragment size gauge must be registered");
    assertNotNull(maxRecordPage, "record-page max gauge must be registered");
    assertNotNull(maxHotLeaf, "HOT-leaf max gauge must be registered");
    assertNotNull(maxFragment, "fragment max gauge must be registered");
    assertNotNull(allocatorBytes, "allocator gauge must be registered");

    // Poll-before-open must not throw or NPE. May legitimately return 0.
    assertTrue(recordPageSize.getAsLong() >= 0L);
    assertTrue(hotLeafSize.getAsLong() >= 0L);
    assertTrue(fragmentSize.getAsLong() >= 0L);
    assertTrue(allocatorBytes.getAsLong() >= 0L);

    JsonTestHelper.createDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        wtx.insertStringValueAsFirstChild("seed");
        wtx.commit();
      }
      session.close();
    }

    // After a database has been opened the BufferManager is initialized, so
    // the configured max byte budgets must be > 0 across all three ShardedPageCache
    // instances. This proves the wiring from supplier → BufferManagerImpl getters
    // → ShardedPageCache.maxWeightBytes.
    assertTrue(maxRecordPage.getAsLong() > 0L,
        "record-page max should be > 0 after BufferManager init; got " + maxRecordPage.getAsLong());
    assertTrue(maxHotLeaf.getAsLong() > 0L,
        "HOT-leaf max should be > 0 after BufferManager init; got " + maxHotLeaf.getAsLong());
    assertTrue(maxFragment.getAsLong() > 0L,
        "fragment max should be > 0 after BufferManager init; got " + maxFragment.getAsLong());

    // Current sizes must be non-negative and bounded by their max (sanity).
    assertTrue(recordPageSize.getAsLong() >= 0L && recordPageSize.getAsLong() <= maxRecordPage.getAsLong());
    assertTrue(hotLeafSize.getAsLong() >= 0L && hotLeafSize.getAsLong() <= maxHotLeaf.getAsLong());
    assertTrue(fragmentSize.getAsLong() >= 0L && fragmentSize.getAsLong() <= maxFragment.getAsLong());

    // Allocator gauge must be non-negative; on Linux + after BufferManager init
    // it's typically > 0 once any segments have been allocated, but on other
    // platforms (or before allocation) it's legitimately 0.
    assertTrue(allocatorBytes.getAsLong() >= 0L);
  }
}
