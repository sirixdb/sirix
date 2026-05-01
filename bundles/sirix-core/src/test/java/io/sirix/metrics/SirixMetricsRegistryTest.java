package io.sirix.metrics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for the {@link SirixMetricsRegistry} SPI. The registry is a
 * process-wide static singleton, so these tests do not isolate the global state — they
 * verify behaviours that hold regardless of what other tests have registered.
 */
final class SirixMetricsRegistryTest {

  /** Captures every gauge a bridge sees for assertion. */
  private static final class CapturingBridge implements SirixMetricsRegistry.Bridge {
    final List<String> namesInOrder = new ArrayList<>();
    final Map<String, LongSupplier> suppliers = new HashMap<>();
    final Map<String, String> helps = new HashMap<>();

    @Override
    public void registerGauge(final String name, final String help, final LongSupplier supplier) {
      namesInOrder.add(name);
      suppliers.put(name, supplier);
      helps.put(name, help);
    }
  }

  @Test
  void builtInGauges_areRegisteredAtStaticInit() {
    final List<String> names = SirixMetricsRegistry.registeredGaugeNames();
    assertTrue(names.contains("sirix_record_page_cache_hits_total"),
        "expected built-in cache-hit gauge; got " + names);
    assertTrue(names.contains("sirix_record_page_cache_misses_total"),
        "expected built-in cache-miss gauge; got " + names);
    assertTrue(names.contains("sirix_record_page_cache_evictions_total"),
        "expected built-in cache-eviction gauge; got " + names);
  }

  @Test
  void install_forwardsAllPreviouslyRegisteredGauges() {
    final CapturingBridge bridge = new CapturingBridge();
    SirixMetricsRegistry.install(bridge);

    assertTrue(bridge.namesInOrder.contains("sirix_record_page_cache_hits_total"));
    assertTrue(bridge.namesInOrder.contains("sirix_record_page_cache_misses_total"));
    assertTrue(bridge.namesInOrder.contains("sirix_record_page_cache_evictions_total"));
  }

  @Test
  void registerGauge_afterInstall_forwardsImmediately() {
    final CapturingBridge bridge = new CapturingBridge();
    SirixMetricsRegistry.install(bridge);
    final int beforeCount = bridge.namesInOrder.size();

    final String name = "sirix_test_late_gauge_" + System.nanoTime();
    final AtomicLong value = new AtomicLong(42L);
    SirixMetricsRegistry.registerGauge(name, "test", value::get);

    assertEquals(beforeCount + 1, bridge.namesInOrder.size(),
        "late registration must be forwarded to the already-installed bridge");
    assertEquals(name, bridge.namesInOrder.get(beforeCount));
    assertEquals(42L, bridge.suppliers.get(name).getAsLong());
    value.set(99L);
    assertEquals(99L, bridge.suppliers.get(name).getAsLong(),
        "supplier must be live — not a snapshot at registration time");
  }

  @Test
  void install_isIdempotent_eachBridgeReceivesAllRegistrations() {
    final CapturingBridge a = new CapturingBridge();
    final CapturingBridge b = new CapturingBridge();
    SirixMetricsRegistry.install(a);
    SirixMetricsRegistry.install(b);

    assertTrue(a.namesInOrder.contains("sirix_record_page_cache_hits_total"));
    assertTrue(b.namesInOrder.contains("sirix_record_page_cache_hits_total"));
  }

  @Test
  void registerGauge_rejectsNullArguments() {
    assertThrows(NullPointerException.class,
        () -> SirixMetricsRegistry.registerGauge(null, "h", () -> 0L));
    assertThrows(NullPointerException.class,
        () -> SirixMetricsRegistry.registerGauge("n", null, () -> 0L));
    assertThrows(NullPointerException.class,
        () -> SirixMetricsRegistry.registerGauge("n", "h", null));
  }

  @Test
  void install_rejectsNullBridge() {
    assertThrows(NullPointerException.class, () -> SirixMetricsRegistry.install(null));
  }
}
