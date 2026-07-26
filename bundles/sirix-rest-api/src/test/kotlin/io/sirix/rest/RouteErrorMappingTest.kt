package io.sirix.rest

import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.WebClient
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.concurrent.TimeUnit

/**
 * Verifies the request-scoped failure mapping against [java.lang.Error]s using the EXACT
 * production units ([coroutineHandler] + [routerFailureHandler], the ones SirixVerticle's router
 * is built from — no Keycloak/TLS needed for that).
 *
 * An [AssertionError] thrown while serving a request (observed in production from
 * JsonLimitedSerializer during a GET, surfacing via an executeBlocking future awaited on the
 * event loop) used to escape the `catch (e: Exception)` bridge to the event-loop thread's
 * uncaught handler: the client got NO response (hang until timeout) and the failure bypassed the
 * failure handler entirely. It must map to a clean 500 — and the server must keep serving.
 */
@ExtendWith(VertxExtension::class)
@DisplayName("Route error mapping for java.lang.Error")
class RouteErrorMappingTest {

    @Test
    @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
    @DisplayName("AssertionError in a request handler maps to 500 and the event loop keeps serving")
    fun assertionErrorMapsTo500AndServerSurvives(vertx: Vertx, testContext: VertxTestContext) {
        val router = Router.router(vertx)
        val scope = CoroutineScope(vertx.dispatcher())
        router.get("/boom").coroutineHandler(scope) {
            throw AssertionError("injected: serializer invariant violated")
        }
        router.get("/ok").coroutineHandler(scope) { ctx ->
            ctx.response().end("ok")
        }
        router.route().failureHandler(routerFailureHandler())

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                val server = vertx.createHttpServer().requestHandler(router).listen(0).coAwait()
                val client = WebClient.create(vertx)

                val boomResponse = client.get(server.actualPort(), "localhost", "/boom").send().coAwait()
                testContext.verify {
                    assertEquals(500, boomResponse.statusCode(), "an Error must map to a 500, not a hang")
                    val body = boomResponse.bodyAsJsonObject()
                    assertEquals(500, body.getInteger("statusCode"))
                    assertEquals("Internal server error", body.getString("message"))
                }

                // The same event loop must still serve requests — the Error must not have killed
                // or poisoned it.
                val okResponse = client.get(server.actualPort(), "localhost", "/ok").send().coAwait()
                testContext.verify {
                    assertEquals(200, okResponse.statusCode())
                    assertEquals("ok", okResponse.bodyAsString())
                    testContext.completeNow()
                }
            } catch (t: Throwable) {
                testContext.failNow(t)
            }
        }
    }

    @Test
    @Timeout(value = 30, timeUnit = TimeUnit.SECONDS)
    @DisplayName("Exception mapping is unchanged: IllegalArgumentException stays a 400")
    fun illegalArgumentStillMapsTo400(vertx: Vertx, testContext: VertxTestContext) {
        val router = Router.router(vertx)
        val scope = CoroutineScope(vertx.dispatcher())
        router.get("/bad").coroutineHandler(scope) {
            throw IllegalArgumentException("database names must not contain path separators")
        }
        router.route().failureHandler(routerFailureHandler())

        GlobalScope.launch(vertx.dispatcher()) {
            try {
                val server = vertx.createHttpServer().requestHandler(router).listen(0).coAwait()
                val client = WebClient.create(vertx)

                val response = client.get(server.actualPort(), "localhost", "/bad").send().coAwait()
                testContext.verify {
                    assertEquals(400, response.statusCode())
                    assertEquals(
                        "database names must not contain path separators",
                        response.bodyAsJsonObject().getString("message")
                    )
                    testContext.completeNow()
                }
            } catch (t: Throwable) {
                testContext.failNow(t)
            }
        }
    }
}
