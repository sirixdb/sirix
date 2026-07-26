/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Contract tests for {@link TransactionMetrics}. Asserts the increment/decrement
 * pairing and the monotonic totals' invariant (never decrement).
 */
final class TransactionMetricsTest {

  @BeforeEach
  void reset() {
    TransactionMetrics.resetForTesting();
  }

  @Test
  void activeReadOnlyTrx_incrementsAndDecrements() {
    TransactionMetrics.onReadOnlyTrxOpened();
    TransactionMetrics.onReadOnlyTrxOpened();
    assertEquals(2L, TransactionMetrics.activeReadOnlyTrx());
    assertEquals(2L, TransactionMetrics.totalReadOnlyTrxOpened());

    TransactionMetrics.onReadOnlyTrxClosed();
    assertEquals(1L, TransactionMetrics.activeReadOnlyTrx());
    assertEquals(2L, TransactionMetrics.totalReadOnlyTrxOpened(),
        "total opened counter must be monotonic");

    TransactionMetrics.onReadOnlyTrxClosed();
    assertEquals(0L, TransactionMetrics.activeReadOnlyTrx());
  }

  @Test
  void activeReadWriteTrx_incrementsAndDecrements() {
    TransactionMetrics.onReadWriteTrxOpened();
    assertEquals(1L, TransactionMetrics.activeReadWriteTrx());
    assertEquals(1L, TransactionMetrics.totalReadWriteTrxOpened());

    TransactionMetrics.onReadWriteTrxClosed();
    assertEquals(0L, TransactionMetrics.activeReadWriteTrx());
    assertEquals(1L, TransactionMetrics.totalReadWriteTrxOpened(),
        "total opened counter must be monotonic");
  }

  @Test
  void readOnlyAndReadWrite_areIndependent() {
    TransactionMetrics.onReadOnlyTrxOpened();
    TransactionMetrics.onReadWriteTrxOpened();
    TransactionMetrics.onReadWriteTrxOpened();

    assertEquals(1L, TransactionMetrics.activeReadOnlyTrx());
    assertEquals(2L, TransactionMetrics.activeReadWriteTrx());
    assertEquals(1L, TransactionMetrics.totalReadOnlyTrxOpened());
    assertEquals(2L, TransactionMetrics.totalReadWriteTrxOpened());
  }

  @Test
  void reset_zeroesEverything() {
    TransactionMetrics.onReadOnlyTrxOpened();
    TransactionMetrics.onReadWriteTrxOpened();
    TransactionMetrics.resetForTesting();

    assertEquals(0L, TransactionMetrics.activeReadOnlyTrx());
    assertEquals(0L, TransactionMetrics.activeReadWriteTrx());
    assertEquals(0L, TransactionMetrics.totalReadOnlyTrxOpened());
    assertEquals(0L, TransactionMetrics.totalReadWriteTrxOpened());
  }
}
