package io.sirix.rest

import io.brackit.query.QueryException
import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Handler
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpServerResponse
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.coroutines.cancellation.CancellationException

/**
 * Request-scoped failure plumbing shared by every route of [SirixVerticle] — extracted so the
 * coroutine bridge and the failure-to-HTTP mapping are testable as the exact production units.
 */
private val logger = LoggerFactory.getLogger(SirixVerticle::class.java)

/**
 * Bridges a suspending handler onto a route, routing EVERY failure of the request body — checked,
 * unchecked, and [java.lang.Error] alike — into the router's failure handler so the client gets a
 * clean status code and the worker/event-loop threads are never killed or poisoned.
 *
 * `catch (e: Exception)` used to let an [AssertionError] thrown during a request (e.g. inside a
 * serializer on an executeBlocking worker, rethrown here by `coAwait`) escape to the event-loop
 * thread's uncaught handler: the stack went to stderr and the request hung until the client
 * timeout instead of becoming a 500.
 *
 * [VirtualMachineError]s (OutOfMemoryError & friends) are NOT swallowed: the request is failed
 * best-effort (cheap — it only schedules the failure handler), then the error is rethrown so the
 * platform-level uncaught handling and any crash-on-OOM policy still see it.
 */
internal fun Route.coroutineHandler(scope: CoroutineScope, fn: suspend (RoutingContext) -> Unit): Route {
    return handler { ctx ->
        scope.launch(ctx.vertx().dispatcher()) {
            try {
                fn(ctx)
            } catch (t: Throwable) {
                if (t is VirtualMachineError) {
                    runCatching { ctx.fail(t) }
                    throw t
                }
                ctx.fail(t)
            }
        }
    }
}

/**
 * The router-wide failure handler: logs full details server-side and maps the failure to a safe
 * client response (no internal details). [Throwable]s without a dedicated mapping — including
 * [java.lang.Error]s — become a generic 500.
 */
internal fun routerFailureHandler(): Handler<RoutingContext> = Handler { failureRoutingContext ->
    val statusCode = failureRoutingContext.statusCode()
    val failure = failureRoutingContext.failure()
    val request = failureRoutingContext.request()

    // Log full details server-side (stack traces never sent to client)
    if (failure != null) {
        if (failure is CancellationException) {
            logger.debug(
                "Request cancelled (shutdown): {} {} (statusCode={})",
                request.method(),
                request.uri(),
                statusCode
            )
        } else {
            logger.error(
                "Request failed: {} {} (statusCode={})",
                request.method(),
                request.uri(),
                statusCode,
                failure
            )
        }
    } else {
        logger.error(
            "Request failed: {} {} (statusCode={}, no exception)",
            request.method(),
            request.uri(),
            statusCode
        )
    }

    // Vert.x's RoutingContext.fail(Throwable) stamps statusCode 500 BEFORE this handler runs
    // (only HttpException carries its own code), so a blanket "statusCode > 0 wins" rule made
    // the type-based client-error branches below unreachable and every QueryException /
    // IllegalArgumentException surfaced as a 500. An explicit non-500 code still wins.
    val resolvedStatus = when {
        statusCode > 0 && statusCode != HttpResponseStatus.INTERNAL_SERVER_ERROR.code() -> statusCode
        failure is HttpException -> failure.statusCode
        // A query evaluation/type error (err:XPTY..., err:XQDY..., FO...) is a CLIENT
        // error: the query came from the request. Masking it as a generic 500 hid the
        // actionable message (e.g. "array() expected" for an object-rooted resource).
        failure is QueryException -> HttpResponseStatus.BAD_REQUEST.code()
        // The HandlerPreconditions contract: malformed parameters/preconditions throw
        // IllegalArgumentException — a client error. Only the message branch below
        // existed, so these still surfaced with a 500 status.
        failure is IllegalArgumentException -> HttpResponseStatus.BAD_REQUEST.code()
        statusCode > 0 -> statusCode
        else -> HttpResponseStatus.INTERNAL_SERVER_ERROR.code()
    }

    // Return a safe error message without internal details
    val safeMessage = when {
        failure is HttpException && failure.payload != null -> failure.payload
        failure is QueryException -> failure.message ?: "Query error"
        failure is IllegalArgumentException -> failure.message ?: "Bad request"
        resolvedStatus == 404 -> "Not found"
        resolvedStatus == 401 -> "Unauthorized"
        resolvedStatus == 403 -> "Forbidden"
        resolvedStatus in 400..499 -> failure?.message ?: "Bad request"
        else -> "Internal server error"
    }

    endWithErrorResponse(failureRoutingContext.response(), resolvedStatus, safeMessage)
}

internal fun endWithErrorResponse(response: HttpServerResponse, statusCode: Int, message: String?) {
    if (response.ended() || response.headWritten()) {
        return
    }
    response.setStatusCode(statusCode)
        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    val safeMessage = message ?: "An error occurred"
    val escapedMessage = safeMessage.replace("\\", "\\\\").replace("\"", "\\\"")
    response.end("""{"statusCode":$statusCode,"message":"$escapedMessage"}""")
}
