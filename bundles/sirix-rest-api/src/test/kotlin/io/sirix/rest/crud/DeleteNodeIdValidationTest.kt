package io.sirix.rest.crud

import io.brackit.query.atomic.QNm
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.api.Database
import io.sirix.api.NodeCursor
import io.sirix.api.json.JsonNodeTrx
import io.sirix.api.xml.XmlNodeTrx
import io.sirix.rest.NoneAuth
import io.sirix.rest.coroutineHandler
import io.sirix.rest.routerFailureHandler
import io.sirix.service.json.shredder.JsonShredder
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.nio.file.Path

/**
 * A `nodeId` on DELETE addresses a subtree. A request that carries one must delete exactly that
 * subtree or nothing: it never falls back to deleting the resource, the database or every database.
 *
 * - Not a decimal 64-bit integer (including empty and overflowing values): 400.
 * - Given on a route without a resource (`/` or `/{database}`): 400.
 * - Numeric but not a node of the resource's most recent revision: 404.
 * - An existing node: that subtree is removed and a new revision is committed.
 *
 * Every refusal leaves all databases, resources and revisions unchanged.
 */
@Tag("offline")
class DeleteNodeIdValidationTest {
    @TempDir
    lateinit var location: Path

    enum class Format { JSON, XML }

    @ParameterizedTest
    @EnumSource(Format::class)
    fun malformedNodeIdIsRejectedAndDeletesNothing(format: Format) = runBlocking {
        val itemKey = createResource(format)
        serve { client, port ->
            for (nodeId in listOf("abc", "1.5", "", "9223372036854775808", "%0A", "%0D", "%09", "%00", "%01", "%08", "%0C", "%22", "%5C")) {
                val response = delete(client, port, "/db/res?nodeId=$nodeId")
                assertError(response, 400, "nodeId='$nodeId'")
                assertTrue(response.bodyAsJsonObject().getString("message").contains("nodeId"), response.bodyAsString())
            }
        }
        assertUnchanged(format, itemKey)
    }

    @ParameterizedTest
    @EnumSource(Format::class)
    fun nodeIdWithoutResourceIsRejectedAndDeletesNothing(format: Format) = runBlocking {
        val itemKey = createResource(format)
        serve { client, port ->
            for (path in listOf("/db?nodeId=$itemKey", "/?nodeId=$itemKey", "/db?nodeId=abc", "/?nodeId=abc")) {
                assertError(delete(client, port, path), 400, path)
            }
        }
        assertUnchanged(format, itemKey)
    }

    @ParameterizedTest
    @EnumSource(Format::class)
    fun unknownNodeIdIsNotFoundAndDeletesNothing(format: Format) = runBlocking {
        val itemKey = createResource(format)
        serve { client, port ->
            for (nodeId in listOf("999999", "-5")) {
                assertError(delete(client, port, "/db/res?nodeId=$nodeId"), 404, "nodeId=$nodeId")
            }
        }
        assertUnchanged(format, itemKey)
    }

    @ParameterizedTest
    @EnumSource(Format::class)
    fun existingNodeIdRemovesOnlyThatSubtree(format: Format) = runBlocking {
        val itemKey = createResource(format)
        serve { client, port ->
            val response = delete(client, port, "/db/res?nodeId=$itemKey")
            assertEquals(204, response.statusCode(), response.bodyAsString())
        }
        openDatabase(format).use { db ->
            assertTrue(db.existsResource("res"))
            db.beginResourceSession("res").use { session ->
                assertEquals(2, session.mostRecentRevisionNumber)
                session.beginNodeReadOnlyTrx().use { rtx ->
                    assertFalse((rtx as NodeCursor).moveTo(itemKey))
                }
            }
        }
    }

    private fun assertError(response: HttpResponse<Buffer>, status: Int, what: String) {
        assertEquals(status, response.statusCode(), "$what: ${response.bodyAsString()}")
        val body = response.bodyAsJsonObject()
        assertNotNull(body, what)
        assertEquals(status, body.getInteger("statusCode"), what)
    }

    private suspend fun delete(client: WebClient, port: Int, path: String): HttpResponse<Buffer> =
        client.delete(port, "localhost", path).send().coAwait()

    /** The database, the resource, its single revision and the deletable item all still exist. */
    private fun assertUnchanged(format: Format, itemKey: Long) {
        assertTrue(Databases.existsDatabase(location.resolve("db")))
        openDatabase(format).use { db ->
            assertTrue(db.existsResource("res"))
            db.beginResourceSession("res").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber)
                session.beginNodeReadOnlyTrx().use { rtx ->
                    assertTrue((rtx as NodeCursor).moveTo(itemKey))
                }
            }
        }
    }

    /** Commits one revision holding a root with one deletable child; returns the child's key. */
    private fun createResource(format: Format): Long {
        val config = DatabaseConfiguration(location.resolve("db"))
        if (format == Format.XML) Databases.createXmlDatabase(config) else Databases.createJsonDatabase(config)
        return openDatabase(format).use { db ->
            db.createResource(ResourceConfiguration.newBuilder("res").build())
            db.beginResourceSession("res").use { session ->
                session.beginNodeTrx().use { trx ->
                    val cursor = trx as NodeCursor
                    if (format == Format.XML) {
                        val xtx = trx as XmlNodeTrx
                        xtx.insertElementAsFirstChild(QNm("r"))
                        xtx.insertElementAsFirstChild(QNm("a"))
                    } else {
                        (trx as JsonNodeTrx).insertSubtreeAsFirstChild(
                            JsonShredder.createStringReader("""[{"a":1}]"""),
                            JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.NO
                        )
                        cursor.moveToDocumentRoot()
                        cursor.moveToFirstChild()
                        cursor.moveToFirstChild()
                    }
                    val itemKey = cursor.nodeKey
                    trx.commit()
                    itemKey
                }
            }
        }
    }

    private fun openDatabase(format: Format): Database<*> =
        if (format == Format.XML) Databases.openXmlDatabase(location.resolve("db"))
        else Databases.openJsonDatabase(location.resolve("db"))

    private suspend fun serve(block: suspend (WebClient, Int) -> Unit) {
        val vertx = Vertx.vertx()
        val client = WebClient.create(vertx)
        try {
            val user = NoneAuth.createUser()
            val scope = CoroutineScope(vertx.dispatcher())
            val router = Router.router(vertx)
            for (path in listOf("/", "/:database", "/:database/:resource")) {
                router.delete(path)
                    .handler { ctx -> ctx.put("user", user); ctx.next() }
                    .coroutineHandler(scope) { DeleteHandler(location, NoneAuth).handle(it) }
            }
            router.route().failureHandler(routerFailureHandler())
            val server = vertx.createHttpServer().requestHandler(router).listen(0).coAwait()
            try {
                block(client, server.actualPort())
            } finally {
                server.close().coAwait()
            }
        } finally {
            client.close()
            vertx.close().coAwait()
        }
    }
}
