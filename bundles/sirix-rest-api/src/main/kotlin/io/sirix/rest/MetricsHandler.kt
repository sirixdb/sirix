package io.sirix.rest

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Provides HTTP request metrics via Micrometer with a Prometheus registry.
 *
 * Tracks:
 * - request duration (timer, tagged by method / path pattern / status)
 * - total request count (counter, tagged by method / path pattern / status)
 * - in-flight requests (gauge)
 *
 * Call [install] once during router setup to wire the before/after handlers
 * and register the `/metrics` endpoint.
 */
object MetricsHandler {

    private const val REQUEST_START_KEY = "sirix.metrics.startNanos"

    @JvmField
    val registry: PrometheusMeterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    private val activeRequests = AtomicInteger(0)

    private val requestTimer: Timer = Timer.builder("http_request_duration_seconds")
        .description("HTTP request duration")
        .register(registry)

    private val requestCounter: Counter = Counter.builder("http_requests_total")
        .description("Total HTTP requests")
        .register(registry)

    init {
        registry.gauge("http_active_requests", activeRequests)
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

        val tags = listOf(
            Tag.of("method", method),
            Tag.of("path", pathPattern),
            Tag.of("status", statusCode)
        )

        Timer.builder("http_request_duration_seconds")
            .tags(tags)
            .register(registry)
            .record(durationNanos, TimeUnit.NANOSECONDS)

        Counter.builder("http_requests_total")
            .tags(tags)
            .register(registry)
            .increment()
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
