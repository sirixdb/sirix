/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.metrics;

import io.sirix.cache.ShardedPageCache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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

  private static final List<GaugeReg> REGISTRATIONS = new CopyOnWriteArrayList<>();
  private static final List<Bridge> BRIDGES = new CopyOnWriteArrayList<>();

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
    REGISTRATIONS.add(reg);
    for (final Bridge bridge : BRIDGES) {
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
    BRIDGES.add(bridge);
    for (final GaugeReg reg : REGISTRATIONS) {
      bridge.registerGauge(reg.name, reg.help, reg.supplier);
    }
  }

  /** Diagnostic accessor — number of registered gauges (test / debugging). */
  public static int registeredGaugeCount() {
    return REGISTRATIONS.size();
  }

  /** Diagnostic accessor — installed bridge count. */
  public static int installedBridgeCount() {
    return BRIDGES.size();
  }

  /** Test-only: snapshot of currently registered gauge names. */
  public static List<String> registeredGaugeNames() {
    final List<String> out = new ArrayList<>(REGISTRATIONS.size());
    for (final GaugeReg reg : REGISTRATIONS) {
      out.add(reg.name);
    }
    return Collections.unmodifiableList(out);
  }
}
