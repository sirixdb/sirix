package io.sirix.rest

import io.brackit.query.atomic.QNm
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.io.IOStorage
import io.sirix.api.NodeCursor
import io.sirix.api.Database
import io.sirix.api.json.JsonNodeReadOnlyTrx
import io.sirix.api.json.JsonNodeTrx
import io.sirix.api.xml.XmlNodeReadOnlyTrx
import io.sirix.api.xml.XmlNodeTrx
import io.sirix.rest.crud.DiffHandler
import io.sirix.rest.crud.RevertHandler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.Path

class RevertHandlerTest {
    @TempDir
    lateinit var location: Path

    @Test
    fun restoresJsonAndXmlAndPreservesHistoryAfterColdReopen() = runBlocking {
        for (xml in listOf(false, true)) {
            createHistory(xml)
            serve { client, port ->
                val response = client.post(port, "localhost", "/db/res/revert")
                    .sendJsonObject(JsonObject().put("targetRevision", 1).put("expectedHead", 2).put("message", "undo agent edit"))
                    .coAwait()
                assertEquals(201, response.statusCode(), response.bodyAsString())
                assertEquals(JsonObject().put("revision", 3).put("previousHead", 2), response.bodyAsJsonObject())
                if (!xml) {
                    val diff = client.get(port, "localhost", "/db/res/diff?first-revision=2&second-revision=3")
                        .send().coAwait()
                    assertEquals(200, diff.statusCode(), diff.bodyAsString())
                    assertTrue(requireNotNull(diff.bodyAsJsonObject()).getJsonArray("diffs").size() > 0, diff.bodyAsString())
                }
            }
            // Every request's handles are closed; reopen solely from the persisted database.
            openDatabase(xml).use { db ->
                db.beginResourceSession("res").use { session ->
                    assertEquals(3, session.mostRecentRevisionNumber)
                    for (revision in 1..3) {
                        session.beginNodeReadOnlyTrx(revision).use { rtx ->
                            (rtx as NodeCursor).moveToFirstChild()
                            val value = if (xml) (rtx as XmlNodeReadOnlyTrx).name.localName
                                        else (rtx as JsonNodeReadOnlyTrx).value
                            assertEquals(if (revision == 2) "new" else "old", value)
                        }
                    }
                    val commit = session.getHistory(1)[0]
                    assertEquals("undo agent edit", commit.commitMessage.orElseThrow())
                    assertEquals("admin", commit.user.name)
                }
            }
            Databases.removeDatabase(location.resolve("db"))
        }
    }

    @Test
    fun rejectsInvalidRequestsWithoutCommitting() = runBlocking {
        createHistory(false)
        serve { client, port ->
            val bodies = listOf("", "[]", "1", "true", "\"text\"", "null", "{", "{}", "{\"targetRevision\":0}",
                "{\"targetRevision\":-1}", "{\"targetRevision\":1.5}", "{\"targetRevision\":\"1\"}",
                "{\"targetRevision\":2147483648}", "{\"targetRevision\":3}",
                "{\"targetRevision\":1,\"expectedHead\":null}", "{\"targetRevision\":1,\"expectedHead\":0}",
                "{\"targetRevision\":1,\"message\":2}", "{\"targetRevision\":1,\"nodeId\":5}")
            for (body in bodies) {
                val response = client.post(port, "localhost", "/db/res/revert")
                    .putHeader("Content-Type", "application/json").sendBuffer(Buffer.buffer(body)).coAwait()
                assertEquals(400, response.statusCode(), "$body: ${response.bodyAsString()}")
            }
            val subtree = client.post(port, "localhost", "/db/res/revert?nodeId=1")
                .sendJsonObject(JsonObject().put("targetRevision", 1)).coAwait()
            assertEquals(400, subtree.statusCode(), subtree.bodyAsString())
            val stale = client.post(port, "localhost", "/db/res/revert")
                .sendJsonObject(JsonObject().put("targetRevision", 1).put("expectedHead", 1)).coAwait()
            assertEquals(409, stale.statusCode(), stale.bodyAsString())
            for (path in listOf("/missing/res/revert", "/db/missing/revert")) {
                val missing = client.post(port, "localhost", path)
                    .sendJsonObject(JsonObject().put("targetRevision", 1)).coAwait()
                assertEquals(404, missing.statusCode(), missing.bodyAsString())
            }
        }
        assertHead(2)
    }

    @Test
    fun onlyOneConcurrentRequestCanMatchExpectedHead() = runBlocking {
        createHistory(false)
        serve { client, port ->
            val statuses = coroutineScope {
                val first = async { client.post(port, "localhost", "/db/res/revert")
                    .sendJsonObject(JsonObject().put("targetRevision", 1).put("expectedHead", 2)).coAwait().statusCode() }
                val second = async { client.post(port, "localhost", "/db/res/revert")
                    .sendJsonObject(JsonObject().put("targetRevision", 1).put("expectedHead", 2)).coAwait().statusCode() }
                listOf(first.await(), second.await()).sorted()
            }
            assertEquals(listOf(201, 409), statuses)
        }
        assertHead(3)
    }

    @Test
    fun optionalHeadAndMessageAndRevertToHeadAppendARevision() = runBlocking {
        createHistory(false)
        serve { client, port ->
            val response = client.post(port, "localhost", "/db/res/revert")
                .sendJsonObject(JsonObject().put("targetRevision", 2)).coAwait()
            assertEquals(201, response.statusCode(), response.bodyAsString())
            assertEquals(3, requireNotNull(response.bodyAsJsonObject()).getInteger("revision"))
        }
        assertHead(3)
    }

    @Test
    fun viewRoleCannotRevert() = runBlocking {
        createHistory(false)
        val user = User.create(JsonObject())
        user.authorizations().put("test", RoleBasedAuthorization.create("view"))
        serve(user) { client, port ->
            val response = client.post(port, "localhost", "/db/res/revert")
                .sendJsonObject(JsonObject().put("targetRevision", 1)).coAwait()
            assertEquals(403, response.statusCode(), response.bodyAsString())
        }
        assertHead(2)
    }

    @Test
    fun prunedTargetFailsWithoutAppendingOrReplacingTheTarget() = runBlocking {
        createHistory(false)
        val timestamp = openDatabase(false).use { db ->
            db.beginResourceSession("res").use { session ->
                session.beginNodeReadOnlyTrx(1).use { it.revisionTimestamp }
            }
        }
        // A retention tombstone keeps the revision's timestamp while removing its readable root.
        val record = ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE)
        IOStorage.writePrunedRevisionRecord(record, timestamp.toEpochMilli())
        record.flip()
        val revisions = location.resolve("db/resources/res/data").resolve(IOStorage.REVISIONS_FILENAME)
        FileChannel.open(revisions, StandardOpenOption.WRITE).use { channel ->
            var offset = IOStorage.revisionsFileOffset(1)
            while (record.hasRemaining()) offset += channel.write(record, offset)
            channel.force(false)
        }
        Databases.clearGlobalCaches()
        serve { client, port ->
            val response = client.post(port, "localhost", "/db/res/revert")
                .sendJsonObject(JsonObject().put("targetRevision", 1).put("expectedHead", 2)).coAwait()
            assertEquals(410, response.statusCode(), response.bodyAsString())
        }
        assertHead(2)
    }

    private fun assertHead(expected: Int) {
        openDatabase(false).use { db ->
            db.beginResourceSession("res").use { assertEquals(expected, it.mostRecentRevisionNumber) }
        }
    }

    private fun createHistory(xml: Boolean) {
        val config = DatabaseConfiguration(location.resolve("db"))
        if (xml) Databases.createXmlDatabase(config) else Databases.createJsonDatabase(config)
        openDatabase(xml).use { db ->
            db.createResource(ResourceConfiguration.newBuilder("res").useDeweyIDs(true).build())
            db.beginResourceSession("res").use { session ->
                session.beginNodeTrx().use { wtx ->
                    if (xml) (wtx as XmlNodeTrx).insertElementAsFirstChild(QNm("old"))
                    else (wtx as JsonNodeTrx).insertStringValueAsFirstChild("old")
                    wtx.commit("initial")
                    (wtx as NodeCursor).moveToDocumentRoot()
                    (wtx as NodeCursor).moveToFirstChild()
                    if (xml) (wtx as XmlNodeTrx).setName(QNm("new"))
                    else (wtx as JsonNodeTrx).setStringValue("new")
                    wtx.commit("agent edit")
                }
            }
        }
    }

    private fun openDatabase(xml: Boolean): Database<*> =
        if (xml) Databases.openXmlDatabase(location.resolve("db")) else Databases.openJsonDatabase(location.resolve("db"))

    private suspend fun serve(user: User = NoneAuth.createUser(), block: suspend (WebClient, Int) -> Unit) {
        val vertx = Vertx.vertx()
        val client = WebClient.create(vertx)
        try {
            val router = Router.router(vertx)
            router.post("/:database/:resource/revert").handler(BodyHandler.create())
                .handler { ctx -> ctx.put("user", user); ctx.next() }
                .coroutineHandler(CoroutineScope(vertx.dispatcher())) { RevertHandler(location, NoneAuth).handle(it) }
            router.get("/:database/:resource/diff")
                .handler { ctx -> ctx.put("user", user); ctx.next() }
                .coroutineHandler(CoroutineScope(vertx.dispatcher())) { DiffHandler(location, NoneAuth).handle(it) }
            router.route().failureHandler(routerFailureHandler())
            val server = vertx.createHttpServer().requestHandler(router).listen(0).coAwait()
            try { block(client, server.actualPort()) } finally { server.close().coAwait() }
        } finally {
            client.close()
            vertx.close().coAwait()
        }
    }
}
