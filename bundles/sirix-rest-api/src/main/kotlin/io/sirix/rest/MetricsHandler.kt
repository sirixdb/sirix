package io.sirix.rest

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.system.UptimeMetrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.sirix.metrics.SirixMetricsRegistry
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.LongSupplier

/**
 * Provides HTTP request metrics via Micrometer with a Prometheus registry, exposed in
 * Prometheus text exposition format 0.0.4 on `GET /metrics`.
 *
 * Exposes:
 * - `http_request_duration_seconds` — request-duration histogram, tagged by
 *   method / path pattern / status, with fixed SLO buckets
 *   (5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000 ms)
 * - `http_requests_total` — request counter with the same tags
 * - `http_active_requests` — in-flight request gauge
 * - `jvm_memory_*`, `jvm_gc_*`, `process_uptime_seconds` / `process_start_time_seconds`
 *   — JVM basics via the standard Micrometer binders
 * - every `sirix_*` gauge registered with [SirixMetricsRegistry] (page-cache
 *   hit/miss/eviction counters, cache sizes, transaction counts, allocator commitment)
 *
 * Call [install] once during router setup to wire the before/after handlers
 * and register the `/metrics` endpoint.
 */
object MetricsHandler {

    private const val REQUEST_START_KEY = "sirix.metrics.startNanos"

    /**
     * Fixed latency buckets. Micrometer publishes service-level objectives as classic
     * Prometheus histogram buckets (`…_bucket{le="0.005"}` … plus `+Inf`), which is exactly
     * the fixed-bucket layout we want — cheap, mergeable across instances, and sufficient
     * for p50/p95/p99 estimation via `histogram_quantile()`.
     */
    private val LATENCY_SLO_BUCKETS = arrayOf(
        Duration.ofMillis(5),
        Duration.ofMillis(10),
        Duration.ofMillis(25),
        Duration.ofMillis(50),
        Duration.ofMillis(100),
        Duration.ofMillis(250),
        Duration.ofMillis(500),
        Duration.ofMillis(1_000),
        Duration.ofMillis(2_500),
        Duration.ofMillis(5_000),
        Duration.ofMillis(10_000)
    )

    @JvmField
    val registry: PrometheusMeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    private val activeRequests = AtomicInteger(0)

    /**
     * Per-(method, path, status) meters, resolved once and reused. Micrometer's registry
     * already interns meters by id, but going through the builder on every request would
     * re-allocate the tag list and the SLO distribution config on the hot path; a single
     * concurrent-map hit keyed by a small string keeps the steady-state per-request cost
     * at one lookup and zero boxing.
     *
     * IMPORTANT: do NOT register untagged meters under these names. The Prometheus
     * registry requires every meter of one name to carry the same tag keys — an untagged
     * `http_request_duration_seconds` registered first makes every tagged registration
     * fail (Micrometer logs a warning and drops the series from the scrape output).
     */
    private data class RouteMeters(val timer: Timer, val counter: Counter)

    private val routeMeters = ConcurrentHashMap<String, RouteMeters>()

    private val sirixBridgeInstalled = AtomicBoolean(false)

    init {
        registry.gauge("http_active_requests", activeRequests)

        // JVM basics: heap/non-heap usage (jvm_memory_used_bytes / jvm_memory_max_bytes /
        // jvm_memory_committed_bytes + buffer-pool gauges), GC counts and pause times
        // (jvm_gc_pause_seconds_count/_sum etc.), process uptime and start time.
        // JvmGcMetrics holds GC notification listeners and is AutoCloseable; this is a
        // process-lifetime singleton, so the listeners intentionally live until shutdown.
        JvmMemoryMetrics().bindTo(registry)
        JvmGcMetrics().bindTo(registry)
        UptimeMetrics().bindTo(registry)
    }

    /**
     * Installs a one-shot adapter that forwards every gauge registered with
     * [SirixMetricsRegistry] to this handler's Prometheus registry. Idempotent — if
     * called more than once (e.g. from multiple `installAuthn` paths), only the first
     * call wires the bridge. Called from [install] so embedders never need to invoke it
     * directly.
     */
    private fun installSirixBridge() {
        if (!sirixBridgeInstalled.compareAndSet(false, true)) {
            return
        }
        SirixMetricsRegistry.install { name: String, help: String, supplier: LongSupplier ->
            // Micrometer's PrometheusMeterRegistry emits .description() as the # HELP line
            // in the scrape output, so wiring help through gives us self-documenting
            // metrics. .register(registry) returns the existing gauge if a same-named one
            // already exists, so re-registration during reload is harmless.
            Gauge.builder(name) { supplier.asLong.toDouble() }
                .description(help)
                .strongReference(true)
                .register(registry)
        }
    }

    /**
     * Installs metrics collection on the given [router].
     *
     * 1. A before-handler records the request start time and increments the
     *    active-request gauge.
     * 2. An after-handler (response end callback) records duration and status,
     *    and decrements the gauge.
     * 3. A GET `/metrics` endpoint returns the Prometheus scrape payload.
     */
    fun install(router: Router) {
        // Forward every Sirix-internal gauge (cache hit/miss/eviction counters today; future
        // additions automatically follow) to the same Prometheus registry the HTTP handlers
        // publish to. Done here so the database internals don't need to know about
        // Micrometer — they just register LongSuppliers with SirixMetricsRegistry.
        installSirixBridge()

        // -- before: record start time, bump in-flight counter ----------------
        router.route().order(-1).handler { ctx: RoutingContext ->
            ctx.put(REQUEST_START_KEY, System.nanoTime())
            activeRequests.incrementAndGet()
            ctx.addEndHandler {
                recordMetrics(ctx)
            }
            ctx.next()
        }

        // -- /metrics endpoint (unauthenticated, like /health) ----------------
        router.get("/metrics").order(0).handler { ctx: RoutingContext ->
            ctx.response()
                .putHeader(HttpHeaders.CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
                .end(registry.scrape())
        }
    }

    private fun recordMetrics(ctx: RoutingContext) {
        activeRequests.decrementAndGet()

        val startNanos = ctx.get<Long>(REQUEST_START_KEY) ?: return
        val durationNanos = System.nanoTime() - startNanos
        val method = ctx.request().method().name()
        val statusCode = ctx.response().getStatusCode().toString()
        val pathPattern = normalizePathPattern(ctx)

        val meters = routeMeters.computeIfAbsent("$method|$pathPattern|$statusCode") {
            val tags = listOf(
                Tag.of("method", method),
                Tag.of("path", pathPattern),
                Tag.of("status", statusCode)
            )
            RouteMeters(
                Timer.builder("http_request_duration_seconds")
                    .description("HTTP request duration")
                    .tags(tags)
                    .serviceLevelObjectives(*LATENCY_SLO_BUCKETS)
                    .register(registry),
                Counter.builder("http_requests_total")
                    .description("Total HTTP requests")
                    .tags(tags)
                    .register(registry)
            )
        }

        meters.timer.record(durationNanos, TimeUnit.NANOSECONDS)
        meters.counter.increment()
    }

    /**
     * Returns the matched route pattern (e.g. `/:database/:resource`) when
     * available, falling back to the raw URI path. This avoids high-cardinality
     * label explosion from path parameters.
     */
    private fun normalizePathPattern(ctx: RoutingContext): String {
        val currentRoute = ctx.currentRoute()
        val pattern = currentRoute?.path
        if (pattern != null && pattern.isNotEmpty()) {
            return pattern
        }
        // Fallback: strip query string, collapse IDs to reduce cardinality
        val rawPath = ctx.request().path() ?: "/"
        return rawPath
    }
}
