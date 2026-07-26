/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counters for node transactions opened against any
 * {@link io.sirix.api.ResourceSession}.
 *
 * <p>Why static? The Prometheus scrape model is pull-based: the metrics registry
 * polls a {@link java.util.function.LongSupplier} when a scrape arrives. Per-session
 * state would force the registry to enumerate every open {@code Database} and
 * {@code ResourceSession} on every scrape — racy and expensive. Static counters
 * trade per-session locality for O(1) scrape cost and lock-free updates, which is
 * the right deal for a server-wide observability signal.
 *
 * <p>Counter conventions:
 * <ul>
 *   <li><b>{@code ACTIVE_*}</b> gauges return the current number of open
 *       transactions. They decrement on close. Useful for capacity / leak detection.</li>
 *   <li><b>{@code TOTAL_*_OPENED}</b> counters are monotonic and used by Prometheus
 *       {@code rate()} to derive opens-per-second. They never decrement.</li>
 * </ul>
 *
 * <p>The "active" gauges use {@link AtomicLong} because read-modify-write semantics
 * (increment-then-read for self-checks, decrement-then-fail-on-negative for debug
 * assertions) are needed. The "total opened" counters use {@link LongAdder} because
 * they're write-mostly under contention and only read at scrape time.
 *
 * <p>Wired into {@link SirixMetricsRegistry} in that class's static initializer.
 */
public final class TransactionMetrics {

  private static final AtomicLong ACTIVE_NODE_READ_TRX = new AtomicLong();
  private static final AtomicLong ACTIVE_NODE_WRITE_TRX = new AtomicLong();
  private static final LongAdder TOTAL_NODE_READ_TRX_OPENED = new LongAdder();
  private static final LongAdder TOTAL_NODE_WRITE_TRX_OPENED = new LongAdder();

  private TransactionMetrics() {
    // Static-only.
  }

  /** Called from {@code AbstractResourceSession} after a read-only trx is registered. */
  public static void onReadOnlyTrxOpened() {
    ACTIVE_NODE_READ_TRX.incrementAndGet();
    TOTAL_NODE_READ_TRX_OPENED.increment();
  }

  /** Called from {@code AbstractResourceSession} after a read-only trx is removed. */
  public static void onReadOnlyTrxClosed() {
    ACTIVE_NODE_READ_TRX.decrementAndGet();
  }

  /** Called from {@code AbstractResourceSession} after a read/write trx is registered. */
  public static void onReadWriteTrxOpened() {
    ACTIVE_NODE_WRITE_TRX.incrementAndGet();
    TOTAL_NODE_WRITE_TRX_OPENED.increment();
  }

  /** Called from {@code AbstractResourceSession} after a read/write trx is removed. */
  public static void onReadWriteTrxClosed() {
    ACTIVE_NODE_WRITE_TRX.decrementAndGet();
  }

  public static long activeReadOnlyTrx() {
    return ACTIVE_NODE_READ_TRX.get();
  }

  public static long activeReadWriteTrx() {
    return ACTIVE_NODE_WRITE_TRX.get();
  }

  public static long totalReadOnlyTrxOpened() {
    return TOTAL_NODE_READ_TRX_OPENED.sum();
  }

  public static long totalReadWriteTrxOpened() {
    return TOTAL_NODE_WRITE_TRX_OPENED.sum();
  }

  /** Test-only: reset all counters. */
  public static void resetForTesting() {
    ACTIVE_NODE_READ_TRX.set(0);
    ACTIVE_NODE_WRITE_TRX.set(0);
    TOTAL_NODE_READ_TRX_OPENED.reset();
    TOTAL_NODE_WRITE_TRX_OPENED.reset();
  }
}
