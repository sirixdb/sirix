/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.metrics;

import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end verification that {@link TransactionMetrics} counters move in lock-step
 * with the {@code AbstractResourceSession} put/remove sites. Catches regressions where
 * a future refactor splits the trx-tracking maps but forgets to update the metrics.
 *
 * <p>Asserts deltas (not absolute values) because the counters are process-wide static —
 * concurrent test execution would race on absolute values; deltas around a known window
 * are stable.
 */
final class TransactionMetricsIntegrationTest {

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
  void readOnlyTrx_open_incrementsActiveAndTotal() {
    JsonTestHelper.createDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);

      final long activeBefore = TransactionMetrics.activeReadOnlyTrx();
      final long totalBefore = TransactionMetrics.totalReadOnlyTrxOpened();

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals(activeBefore + 1, TransactionMetrics.activeReadOnlyTrx(),
            "active RO trx should increment on open");
        assertEquals(totalBefore + 1, TransactionMetrics.totalReadOnlyTrxOpened(),
            "total RO opened should increment on open");
      }

      assertEquals(activeBefore, TransactionMetrics.activeReadOnlyTrx(),
          "active RO trx should decrement back on close");
      assertEquals(totalBefore + 1, TransactionMetrics.totalReadOnlyTrxOpened(),
          "total opened counter must remain monotonic after close");

      session.close();
    }
  }

  @Test
  void readWriteTrx_open_incrementsActiveAndTotal() {
    JsonTestHelper.createDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);

      final long activeBefore = TransactionMetrics.activeReadWriteTrx();
      final long totalBefore = TransactionMetrics.totalReadWriteTrxOpened();

      try (final var wtx = session.beginNodeTrx()) {
        assertEquals(activeBefore + 1, TransactionMetrics.activeReadWriteTrx(),
            "active RW trx should increment on open");
        assertEquals(totalBefore + 1, TransactionMetrics.totalReadWriteTrxOpened(),
            "total RW opened should increment on open");
      }

      assertEquals(activeBefore, TransactionMetrics.activeReadWriteTrx(),
          "active RW trx should decrement back on close");
      assertEquals(totalBefore + 1, TransactionMetrics.totalReadWriteTrxOpened(),
          "total opened counter must remain monotonic after close");

      session.close();
    }
  }

  @Test
  void multipleReadOnlyTrx_areCountedIndependently() {
    JsonTestHelper.createDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);

      // First open a write trx and commit so a revision exists, then open multiple readers.
      try (final var wtx = session.beginNodeTrx()) {
        wtx.commit();
      }

      final long activeBefore = TransactionMetrics.activeReadOnlyTrx();

      try (final var r1 = session.beginNodeReadOnlyTrx();
           final var r2 = session.beginNodeReadOnlyTrx();
           final var r3 = session.beginNodeReadOnlyTrx()) {
        assertEquals(activeBefore + 3, TransactionMetrics.activeReadOnlyTrx(),
            "three concurrent RO trx should all be reflected in the active gauge");
      }

      assertEquals(activeBefore, TransactionMetrics.activeReadOnlyTrx(),
          "all three RO trx should decrement back");

      session.close();
    }
  }
}
