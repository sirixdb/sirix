package io.sirix.rest.crud

import io.sirix.api.xml.XmlResourceSession
import io.sirix.api.json.JsonResourceSession
import io.vertx.core.buffer.Buffer
import io.sirix.access.DatabasesInternals
import io.sirix.access.Databases
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.node.BasicXmlDBStore
import io.brackit.query.node.parser.DocumentParser
import io.sirix.rest.SirixVerticle
import io.vertx.core.Handler
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerRequest
import org.mockito.AdditionalAnswers
import io.vertx.core.http.HttpServerOptions
import org.mockito.Mockito
import io.vertx.ext.web.multipart.MultipartForm
import java.nio.file.Files
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.xml.serialize.XmlSerializer
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.ByteArrayOutputStream
import java.nio.file.Path

/** REST-210: the actual streaming PUT handlers commit content and finish the HTTP response. */
@Tag("offline")
@Timeout(60)
class PutResourceLifecycleTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @CsvSource("true,false", "true,true", "false,false", "false,true")
    fun putCreatesAndReplacesWithContentAndCompletesItsResponse(xml: Boolean, large: Boolean) = runBlocking {
        val initial = if (xml) "<original/>" else "{\"original\":true}"
        if (xml) BasicXmlDBStore.newBuilder().location(directory.resolve("sirix-data")).build().use {
            it.create("auth", "keep", DocumentParser(initial))
        } else BasicJsonDBStore.newBuilder().location(directory.resolve("sirix-data")).build().use {
            it.create("auth", "keep", initial)
        }
        val content = if (xml) {
            if (large) "<loaded>" + "<v/>".repeat(5100) + "</loaded>" else "<loaded>value</loaded>"
        } else {
            if (large) "[" + List(5100) { "1" }.joinToString(",") + "]" else "{\"loaded\":true}"
        }
        withHttp { client, port ->
            for (attempt in 1..2) {
                val response = client.put(port, "127.0.0.1", "/auth/doc?commitTimestamp=2024-01-01T00:00:00Z&useDeweyIDs=${!large}")
                    .putHeader("content-type", if (xml) "application/xml" else "application/json")
                    .sendBuffer(Buffer.buffer(content)).coAwait()
                assertEquals(200, response.statusCode(), response.bodyAsString())
                if (large) assertEquals(0, response.body()?.length() ?: 0)
                else {
                    val body = requireNotNull(response.bodyAsString())
                    assertFalse(body.isEmpty())
                    if (xml) assertTrue(body.contains("<loaded"))
                    else assertEquals(content, body)
                }
                assertNoRequestHandles()
                assertPersisted(xml, content, !large)
            }
        }
    }

    @ParameterizedTest
    @CsvSource("true", "false")
    fun malformedPutDoesNotRetainItsNewResource(xml: Boolean) = runBlocking {
        withHttp { client, port ->
            val response = client.put(port, "127.0.0.1", "/auth/doc")
                .putHeader("content-type", if (xml) "application/xml" else "application/json")
                .sendBuffer(Buffer.buffer(if (xml) "<broken" else "{broken")).coAwait()
            assertTrue(response.statusCode() >= 400)
            assertNoRequestHandles()
            val path = directory.resolve("sirix-data/auth")
            val database = if (xml) Databases.openXmlDatabase(path) else Databases.openJsonDatabase(path)
            database.use { assertTrue(it.listResources().isEmpty(), "failed PUT must remove its partial resource") }
        }
    }

    /** VER-201 rejects timestamps outside signed epoch microseconds; REST preserves that client error. */
    @ParameterizedTest
    @CsvSource("true", "false")
    fun commitFailureAfterStreamingRemainsAClientErrorAndReleasesTheTarget(xml: Boolean) = runBlocking {
        withHttp { client, port ->
            val response = client.put(port, "127.0.0.1", "/auth/doc?commitTimestamp=%2B300000-01-01T00:00:00Z")
                .putHeader("content-type", if (xml) "application/xml" else "application/json")
                .sendBuffer(Buffer.buffer(if (xml) "<loaded/>" else "{\"loaded\":true}")).coAwait()
            assertEquals(400, response.statusCode(), response.bodyAsString())
            assertNoRequestHandles()
            val path = directory.resolve("sirix-data/auth")
            val database = if (xml) Databases.openXmlDatabase(path) else Databases.openJsonDatabase(path)
            database.use { assertTrue(it.listResources().isEmpty(), "failed commit must remove the partial target") }
        }
    }

    /** REST-210: database-scoped routes must not recreate a database holding committed history. */
    @ParameterizedTest
    @CsvSource("true", "false")
    fun databaseCreationAndUploadDispatchPreserveExistingHistory(xml: Boolean) = runBlocking {
        val content = if (xml) "<original/>" else "{\"original\":true}"
        if (xml) BasicXmlDBStore.newBuilder().location(directory.resolve("sirix-data")).build().use {
            it.create("auth", "keep", DocumentParser(content))
        } else BasicJsonDBStore.newBuilder().location(directory.resolve("sirix-data")).build().use {
            it.create("auth", "keep", content)
        }
        val contentType = if (xml) "application/xml" else "application/json"
        withHttp { client, port ->
            assertEquals(201, client.put(port, "127.0.0.1", "/auth").putHeader("content-type", contentType)
                .send().coAwait().statusCode())
            val upload = Files.writeString(directory.resolve("upload"), content)
            val form = MultipartForm.create().binaryFileUpload("file", "fresh", upload.toString(), contentType)
            assertEquals(201, client.post(port, "127.0.0.1", "/auth").sendMultipartForm(form).coAwait().statusCode())
        }
        val database = if (xml) Databases.openXmlDatabase(directory.resolve("sirix-data/auth"))
            else Databases.openJsonDatabase(directory.resolve("sirix-data/auth"))
        database.use {
            it.beginResourceSession("keep").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber)
                val output = ByteArrayOutputStream()
                if (session is XmlResourceSession) XmlSerializer.newBuilder(session, output).build().call()
                else JsonSerializer.newBuilder(session as JsonResourceSession, output).build().call()
                assertEquals(content, output.toString(Charsets.UTF_8))
            }
        }
    }

    private fun assertNoRequestHandles() {
        // The documented public snapshot lists live user handles, not temporary deletion backends.
        val databasePath = directory.resolve("sirix-data/auth").toRealPath()
        assertTrue(DatabasesInternals.getOpenDatabases()[databasePath].isNullOrEmpty(),
            "completed PUT must release its request-owned database handle")
    }

    private fun assertPersisted(xml: Boolean, content: String, deweyIDs: Boolean) {
        val path = directory.resolve("sirix-data/auth")
        val database = if (xml) Databases.openXmlDatabase(path) else Databases.openJsonDatabase(path)
        database.use {
            assertEquals(setOf("keep", "doc"), it.listResources().map { path -> path.fileName.toString() }.toSet())
            it.beginResourceSession("doc").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber, "replacement must restart history")
                assertEquals(deweyIDs, session.resourceConfig.areDeweyIDsStored,
                    "explicit resource options must survive creation and replacement")
                val output = ByteArrayOutputStream()
                if (session is XmlResourceSession) XmlSerializer.newBuilder(session, output).build().call()
                else JsonSerializer.newBuilder(session as JsonResourceSession, output).build().call()
                assertEquals(content, output.toString(Charsets.UTF_8))
            }
        }
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
