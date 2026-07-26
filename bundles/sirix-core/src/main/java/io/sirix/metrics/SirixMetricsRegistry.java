/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.metrics;

import io.sirix.access.Databases;
import io.sirix.cache.Allocators;
import io.sirix.cache.BufferManager;
import io.sirix.cache.BufferManagerImpl;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.ShardedPageCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Lightweight metrics SPI for Sirix internals. Sirix-core declares which gauges and
 * counters exist and what they read; an embedding application (typically
 * {@code sirix-rest-api} via Micrometer/Prometheus, or any JMX/OTEL bridge) calls
 * {@link #install(Bridge)} once at startup to forward the readings to its observability
 * stack.
 *
 * <p>Design choices:
 * <ul>
 *   <li><b>No dependency from sirix-core to a metrics library.</b> Embedders bring their
 *       own (Micrometer, Dropwizard, OTEL); Sirix-core only ships the SPI.</li>
 *   <li><b>Pull-based gauges.</b> Internal counters are already maintained as
 *       {@link java.util.concurrent.atomic.LongAdder} / {@link java.util.concurrent.atomic.AtomicLong};
 *       the SPI does not require Sirix to push events. The bridge sample-reads at its own
 *       cadence (Prometheus scrape, JMX poll, etc.).</li>
 *   <li><b>Static gauge inventory.</b> All built-in gauges register in this class's
 *       static initializer. Extensions can add gauges via {@link #registerGauge} before
 *       or after {@link #install}; bridges installed later see all registrations to date,
 *       and a bridge installed before late registrations will receive forwarded calls.</li>
 *   <li><b>Idempotent multiple installs.</b> Each install iterates the full registration
 *       list — duplicate gauge names on the bridge side are the bridge's problem.</li>
 * </ul>
 */
public final class SirixMetricsRegistry {

  /** Registers a gauge / counter with the embedder's observability stack. */
  @FunctionalInterface
  public interface Bridge {

    /**
     * Register a long-valued gauge. The bridge polls {@code supplier} when its scrape /
     * collection cadence fires.
     *
     * @param name    fully-qualified metric name in Prometheus convention (snake_case,
     *                {@code _total} suffix for monotonic counters, base unit suffix for
     *                others, e.g. {@code _bytes})
     * @param help    one-line human-readable description
     * @param supplier reads the current value; must be cheap and side-effect-free
     */
    void registerGauge(String name, String help, LongSupplier supplier);
  }

  /** Internal record of a single gauge registration. */
  private record GaugeReg(String name, String help, LongSupplier supplier) {}

  // CoW lists would be wrong here — registrations and bridges are append-only at startup,
  // never iterated under concurrent modification, so the cheap-write/synchronized pattern
  // matches the access better. Both lists are guarded by synchronizing on REGISTRATIONS.
  private static final List<GaugeReg> REGISTRATIONS = new ArrayList<>();
  private static final List<Bridge> BRIDGES = new ArrayList<>();

  static {
    // Built-in gauges. Counters that increment monotonically use the _total suffix per
    // Prometheus convention; size gauges use _bytes. ShardedPageCache exposes static
    // accessor methods so we can read its global LongAdders without per-instance state.
    registerGauge("sirix_record_page_cache_hits_total",
        "Total record-page cache hits since process start",
        ShardedPageCache::getCacheHits);
    registerGauge("sirix_record_page_cache_misses_total",
        "Total record-page cache misses since process start",
        ShardedPageCache::getCacheMisses);
    registerGauge("sirix_record_page_cache_evictions_total",
        "Total record-page cache evictions since process start",
        ShardedPageCache::getCacheEvictions);

    // Node-transaction counters. "active" gauges are point-in-time; "_total"
    // counters are monotonic, consumed by Prometheus rate() to derive opens/sec.
    registerGauge("sirix_active_node_read_only_transactions",
        "Currently-open read-only node transactions across all resources",
        TransactionMetrics::activeReadOnlyTrx);
    registerGauge("sirix_active_node_read_write_transactions",
        "Currently-open read-write node transactions across all resources",
        TransactionMetrics::activeReadWriteTrx);
    registerGauge("sirix_node_read_only_transactions_opened_total",
        "Total read-only node transactions opened since process start",
        TransactionMetrics::totalReadOnlyTrxOpened);
    registerGauge("sirix_node_read_write_transactions_opened_total",
        "Total read-write node transactions opened since process start",
        TransactionMetrics::totalReadWriteTrxOpened);

    // Cache size gauges. Suppliers peek the global BufferManager so a scrape on
    // an idle server doesn't force lazy init (2 GB default allocation).
    registerGauge("sirix_record_page_cache_size_bytes",
        "Current bytes held in the record-page cache",
        () -> bufferManagerSupplier(BufferManagerImpl::getRecordPageCacheCurrentWeightBytes));
    registerGauge("sirix_record_page_cache_max_bytes",
        "Configured max bytes for the record-page cache",
        () -> bufferManagerSupplier(BufferManagerImpl::getRecordPageCacheMaxWeightBytes));
    registerGauge("sirix_record_page_fragment_cache_size_bytes",
        "Current bytes held in the record-page-fragment cache",
        () -> bufferManagerSupplier(BufferManagerImpl::getRecordPageFragmentCacheCurrentWeightBytes));
    registerGauge("sirix_record_page_fragment_cache_max_bytes",
        "Configured max bytes for the record-page-fragment cache",
        () -> bufferManagerSupplier(BufferManagerImpl::getRecordPageFragmentCacheMaxWeightBytes));
    registerGauge("sirix_hot_leaf_page_cache_size_bytes",
        "Current bytes held in the HOT-leaf page cache",
        () -> bufferManagerSupplier(BufferManagerImpl::getHOTLeafPageCacheCurrentWeightBytes));
    registerGauge("sirix_hot_leaf_page_cache_max_bytes",
        "Configured max bytes for the HOT-leaf page cache",
        () -> bufferManagerSupplier(BufferManagerImpl::getHOTLeafPageCacheMaxWeightBytes));

    // Off-heap allocator commitment, read from the ACTIVE allocator via
    // Allocators.getInstance() — reading a concrete implementation directly
    // reports a flat 0 whenever the other one is configured (the default is
    // FrameSlotAllocator on Linux, so pinning LinuxMemorySegmentAllocator
    // here measured the wrong, inactive allocator).
    registerGauge("sirix_allocator_physical_memory_bytes",
        "Off-heap physical memory committed by the active memory-segment allocator",
        SirixMetricsRegistry::allocatorPhysicalMemoryBytesOrZero);
  }

  /**
   * Polls a per-cache size gauge by peeking the global {@link BufferManager}.
   * Returns 0 when no database has been opened yet (the buffer manager is null)
   * or when the implementation isn't the size-aware {@link BufferManagerImpl}
   * subtype — a clean zero is more useful in a scrape than a NPE.
   */
  private static long bufferManagerSupplier(
      final java.util.function.ToLongFunction<BufferManagerImpl> reader) {
    final BufferManager bm = Databases.peekGlobalBufferManager();
    if (bm instanceof BufferManagerImpl bmi) {
      return reader.applyAsLong(bmi);
    }
    return 0L;
  }

  /**
   * Reads {@link MemorySegmentAllocator#getPhysicalMemoryBytes()} from the active
   * allocator when it has been initialized; otherwise 0. Safe to call from any
   * platform — allocators that don't track physical commitment report 0.
   */
  private static long allocatorPhysicalMemoryBytesOrZero() {
    try {
      final MemorySegmentAllocator allocator = Allocators.getInstance();
      if (!allocator.isInitialized()) {
        return 0L;
      }
      return allocator.getPhysicalMemoryBytes();
    } catch (final Throwable t) {
      // Defensive: a metrics scrape must never propagate exceptions. Worst case
      // we report 0 and the operator sees a flat line — preferable to /metrics
      // returning 500.
      return 0L;
    }
  }

  private SirixMetricsRegistry() {
    // Static-only.
  }

  /**
   * Register a gauge for later forwarding. Safe to call from any thread, at any time.
   * If a {@link Bridge} has already been {@linkplain #install installed}, the new gauge
   * is forwarded immediately.
   */
  public static void registerGauge(final String name, final String help,
      final LongSupplier supplier) {
    if (name == null || help == null || supplier == null) {
      throw new NullPointerException("name/help/supplier");
    }
    final GaugeReg reg = new GaugeReg(name, help, supplier);
    final List<Bridge> snapshot;
    synchronized (REGISTRATIONS) {
      REGISTRATIONS.add(reg);
      snapshot = new ArrayList<>(BRIDGES);
    }
    // Forward outside the monitor — bridges shouldn't see registry-internal locking, and
    // a misbehaving bridge can't deadlock against later concurrent registrations.
    for (final Bridge bridge : snapshot) {
      bridge.registerGauge(name, help, supplier);
    }
  }

  /**
   * Install a metrics bridge. Every previously-registered gauge is forwarded
   * immediately; later-registered gauges are forwarded as they are added. Multiple
   * bridges can be installed; each receives the same set of registrations.
   */
  public static void install(final Bridge bridge) {
    if (bridge == null) {
      throw new NullPointerException("bridge");
    }
    final List<GaugeReg> snapshot;
    synchronized (REGISTRATIONS) {
      BRIDGES.add(bridge);
      snapshot = new ArrayList<>(REGISTRATIONS);
    }
    for (final GaugeReg reg : snapshot) {
      bridge.registerGauge(reg.name, reg.help, reg.supplier);
    }
  }

  /** Test-only: snapshot of currently registered gauge names. */
  public static List<String> registeredGaugeNames() {
    synchronized (REGISTRATIONS) {
      final List<String> out = new ArrayList<>(REGISTRATIONS.size());
      for (final GaugeReg reg : REGISTRATIONS) {
        out.add(reg.name);
      }
      return Collections.unmodifiableList(out);
    }
  }
}
