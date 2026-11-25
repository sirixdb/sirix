package io.sirix.rest.crud

import io.sirix.rest.SirixVerticle
import io.vertx.core.Handler
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.AdditionalAnswers
import org.mockito.Mockito
import java.nio.file.Files
import java.nio.file.Path

/**
 * FMT-209 / REST-210: whole-resource replacement through PUT draws a fresh incarnation id.
 *
 * Replacing an existing resource creates a new resource history that restarts revision numbering, so
 * the replacement MUST carry a new incarnation id (REST-210); conditional-write tokens issued against
 * the replaced resource can then never be satisfied by an unrelated revision of the same number.
 * The id is read as the `incarnationId` member of the persisted `ressetting.obj` (CONF-102): 16
 * lowercase hexadecimal digits, so the test binds to no accessor of an implementation. The token
 * grammar and its 412 / 428 verdicts belong to REST-204 / REST-205 and are not exercised here.
 *
 * Planned, not enforced (CONV-102): the id is unimplemented, so the class is `@Disabled`. The
 * implementation change removes the annotation and must not weaken an assertion.
 */
@Disabled("FMT-209 / REST-210 planned: PUT replacement does not yet draw an incarnation id")
@Tag("offline")
@Timeout(60)
class PutResourceIncarnationIdTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun putReplacementDrawsAFreshIncarnationIdEachTime(xml: Boolean) = runBlocking {
        val content = if (xml) "<loaded>value</loaded>" else "{\"loaded\":true}"
        withHttp { client, port ->
            val ids = mutableListOf<String>()
            for (attempt in 1..3) {
                val response = client.put(port, "127.0.0.1", "/auth/doc")
                    .putHeader("content-type", if (xml) "application/xml" else "application/json")
                    .sendBuffer(Buffer.buffer(content)).coAwait()
                assertEquals(200, response.statusCode(), response.bodyAsString())
                ids += persistedIncarnationId()
            }
            assertTrue(ids.all { it.matches(Regex("[0-9a-f]{16}")) }, "ids must be 16 lowercase hex digits: $ids")
            assertEquals(3, ids.toSet().size, "each replacement must draw its own id, was $ids")
            assertNotEquals(ids[0], ids[1])
            assertNotEquals(ids[1], ids[2])
        }
    }

    /** The persisted member of the settings document of resource `doc` in database `auth`. */
    private fun persistedIncarnationId(): String {
        val settings = JsonObject(
            Files.readString(directory.resolve("sirix-data/auth/resources/doc/ressetting.obj"))
        )
        assertTrue(settings.containsKey("incarnationId"), "the resource settings lack the incarnationId member")
        return settings.getString("incarnationId")
    }

    private suspend fun withHttp(block: suspend (WebClient, Int) -> Unit) {
        val vertx = Vertx.vertx()
        // The server factory is the transport boundary: use a real ephemeral listener and retain
        // its public actualPort(), without exposing or reflecting on the verticle's private state.
        val listener = vertx.createHttpServer(HttpServerOptions().setSsl(false))
        val server = Mockito.mock(HttpServer::class.java, AdditionalAnswers.delegatesTo<Any>(listener))
        Mockito.`when`(server.requestHandler(Mockito.any<Handler<HttpServerRequest>>())).thenAnswer { call ->
            val router = call.getArgument<Handler<HttpServerRequest>>(0)
            // Keycloak authentication pauses the stream before coroutine dispatch. The offline
            // all-permissions identity skips that step, so supply the same paused transport here.
            listener.requestHandler { request -> request.pause(); router.handle(request) }
            server
        }
        val facade = Mockito.spy(vertx)
        Mockito.doReturn(server).`when`(facade).createHttpServer(Mockito.any(HttpServerOptions::class.java))
        val context = Mockito.spy(vertx.orCreateContext)
        Mockito.doReturn(JsonObject().put("auth.mode", "none").put("use.http", true).put("port", 0))
            .`when`(context).config()
        val previousHome = System.getProperty("user.home")
        val verticle = try {
            System.setProperty("user.home", directory.toString())
            SirixVerticle()
        } finally { System.setProperty("user.home", previousHome) }
        verticle.init(facade, context)
        try {
            val started = Promise.promise<Void>()
            verticle.start(started)
            started.future().coAwait()
            // Bound a missing HTTP response; this is a liveness limit, not a latency assertion.
            val client = WebClient.create(vertx, WebClientOptions().setIdleTimeout(10))
            try { block(client, listener.actualPort()) } finally {
                client.close()
                val stopped = Promise.promise<Void>()
                verticle.stop(stopped)
                stopped.future().coAwait()
            }
        } finally { vertx.close().coAwait() }
    }
}
