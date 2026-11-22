package io.sirix.rest.crud

import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.trx.node.HashType
import io.sirix.api.Database
import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.json.JsonNodeTrx
import io.sirix.api.xml.XmlNodeTrx
import io.sirix.rest.NoneAuth
import io.sirix.rest.coroutineHandler
import io.sirix.rest.crud.json.JsonGet
import io.sirix.rest.crud.json.JsonHead
import io.sirix.rest.crud.xml.XmlGet
import io.sirix.rest.crud.xml.XmlHead
import io.sirix.rest.routerFailureHandler
import io.sirix.service.json.shredder.JsonShredder
import io.sirix.service.xml.shredder.XmlShredder
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito
import java.nio.file.Path
import java.util.stream.Stream

/**
 * REST-203 / issue #1387: a read addressed by `nodeId` serializes exactly that node's subtree or
 * fails. It never answers with another cursor position (the whole resource) and a wrong ETag.
 *
 * Contract, for GET (with and without `query`) and HEAD, JSON and XML, hashes on and off:
 * - `nodeId` absent: the whole resource (ETag of the document root's first child).
 * - `nodeId` not a 64-bit integer: 400.
 * - `nodeId` numeric but not a node of every selected revision: 404.
 * - `nodeId` of an existing node: 200, that subtree, and the ETag of that node.
 */
@Tag("offline")
class ReadNodeIdValidationTest {
    @TempDir
    lateinit var location: Path

    /** Node keys of the fixture, read back from the store instead of hard-coded. */
    private data class Fixture(val firstItemKey: Long, val secondItemKey: Long)

    @ParameterizedTest(name = "{0}, hashes {1}")
    @MethodSource("formatsAndHashes")
    fun getWithExistingNodeIdServesThatSubtreeAndItsETag(format: Format, hash: HashType) = runBlocking {
        val fixture = createResource(format, hash)
        serve { client, port ->
            val whole = get(client, port, format, "")
            assertEquals(200, whole.statusCode(), whole.bodyAsString())

            val subtree = get(client, port, format, "?nodeId=${fixture.firstItemKey}")
            assertEquals(200, subtree.statusCode(), subtree.bodyAsString())
            val body = subtree.bodyAsString()
            assertTrue(body.contains(format.firstItemText), body)
            assertFalse(body.contains(format.secondItemText), body)
            assertNotEquals(whole.bodyAsString(), body)

            if (hash == HashType.NONE) {
                assertNull(subtree.getHeader(HttpHeaders.ETAG.toString()))
            } else {
                val expected = nodeHash(format, 2, fixture.firstItemKey)
                assertEquals(expected.toString(), subtree.getHeader(HttpHeaders.ETAG.toString()))
                assertNotEquals(whole.getHeader(HttpHeaders.ETAG.toString()), subtree.getHeader(HttpHeaders.ETAG.toString()))
            }
        }
    }

    @ParameterizedTest(name = "{0}, hashes {1}")
    @MethodSource("formatsAndHashes")
    fun getWithUnknownNodeIdIsNotFound(format: Format, hash: HashType) = runBlocking {
        createResource(format, hash)
        serve { client, port ->
            for (nodeId in listOf("999999", "-5", Long.MAX_VALUE.toString())) {
                val response = get(client, port, format, "?nodeId=$nodeId")
                assertError(response, 404, "nodeId=$nodeId")
            }
        }
    }

    @ParameterizedTest(name = "{0}, hashes {1}")
    @MethodSource("formatsAndHashes")
    fun getWithNonNumericNodeIdIsBadRequest(format: Format, hash: HashType) = runBlocking {
        createResource(format, hash)
        serve { client, port ->
            for (nodeId in listOf("abc", "1.5", "", "9223372036854775808")) {
                val response = get(client, port, format, "?nodeId=$nodeId")
                assertError(response, 400, "nodeId='$nodeId'")
                assertTrue(response.bodyAsJsonObject().getString("message").contains("nodeId"), response.bodyAsString())
            }
        }
    }

    @ParameterizedTest(name = "{0}, hashes {1}")
    @MethodSource("formatsAndHashes")
    fun getWithNodeIdAbsentFromAnySelectedRevisionIsNotFound(format: Format, hash: HashType) = runBlocking {
        val fixture = createResource(format, hash)
        serve { client, port ->
            val inRevisionTwo = get(client, port, format, "?nodeId=${fixture.secondItemKey}&revision=2")
            assertEquals(200, inRevisionTwo.statusCode(), inRevisionTwo.bodyAsString())

            val inRevisionOne = get(client, port, format, "?nodeId=${fixture.secondItemKey}&revision=1")
            assertError(inRevisionOne, 404, "revision 1")

            val range = get(client, port, format, "?nodeId=${fixture.secondItemKey}&start-revision=1&end-revision=2")
            assertError(range, 404, "revision range 1..2")
        }
    }

    @ParameterizedTest(name = "{0}, hashes {1}")
    @MethodSource("formatsAndHashes")
    fun queryWithBadNodeIdFailsInsteadOfEvaluatingElsewhere(format: Format, hash: HashType) = runBlocking {
        val fixture = createResource(format, hash)
        serve { client, port ->
            val ok = get(client, port, format, "?nodeId=${fixture.firstItemKey}&query=1")
            assertEquals(200, ok.statusCode(), ok.bodyAsString())
            assertError(get(client, port, format, "?nodeId=999999&query=1"), 404, "unknown nodeId with query")
            assertError(get(client, port, format, "?nodeId=abc&query=1"), 400, "non-numeric nodeId with query")
        }
    }

    @ParameterizedTest(name = "{0}, hashes {1}")
    @MethodSource("formatsAndHashes")
    fun headBehavesLikeGet(format: Format, hash: HashType) = runBlocking {
        val fixture = createResource(format, hash)
        serve { client, port ->
            val ok = head(client, port, format, "?nodeId=${fixture.firstItemKey}")
            assertEquals(200, ok.statusCode())
            val etag = ok.getHeader(HttpHeaders.ETAG.toString())
            if (hash == HashType.NONE) {
                assertEquals("", etag)
            } else {
                assertEquals(nodeHash(format, 2, fixture.firstItemKey).toString(), etag)
                assertEquals(etag, get(client, port, format, "?nodeId=${fixture.firstItemKey}")
                    .getHeader(HttpHeaders.ETAG.toString()))
            }
            assertEquals(200, head(client, port, format, "").statusCode())
            assertEquals(404, head(client, port, format, "?nodeId=999999").statusCode())
            assertEquals(404, head(client, port, format, "?nodeId=${fixture.secondItemKey}&revision=1").statusCode())
            assertEquals(400, head(client, port, format, "?nodeId=abc").statusCode())
        }
    }

    private fun assertError(response: HttpResponse<Buffer>, status: Int, what: String) {
        assertEquals(status, response.statusCode(), "$what: ${response.bodyAsString()}")
        val body = response.bodyAsJsonObject()
        assertNotNull(body, what)
        assertEquals(status, body.getInteger("statusCode"), what)
        assertNotNull(body.getString("message"), what)
        assertNull(response.getHeader(HttpHeaders.ETAG.toString()), "$what must not carry an ETag")
    }

    private suspend fun get(client: WebClient, port: Int, format: Format, params: String): HttpResponse<Buffer> =
        client.get(port, "localhost", "/${format.prefix}/db/res$params").send().coAwait()

    private suspend fun head(client: WebClient, port: Int, format: Format, params: String): HttpResponse<Buffer> =
        client.head(port, "localhost", "/${format.prefix}/db/res$params").send().coAwait()

    private fun nodeHash(format: Format, revision: Int, nodeKey: Long): Long =
        openDatabase(format).use { db ->
            db.beginResourceSession("res").use { session ->
                session.beginNodeReadOnlyTrx(revision).use { rtx ->
                    assertTrue((rtx as NodeCursor).moveTo(nodeKey))
                    (rtx as NodeReadOnlyTrx).hash
                }
            }
        }

    /**
     * Revision 1 holds one item; revision 2 adds a second item as its right sibling, so the second
     * item's key exists only in revision 2.
     */
    private fun createResource(format: Format, hash: HashType): Fixture {
        val config = DatabaseConfiguration(location.resolve("db"))
        if (format == Format.XML) Databases.createXmlDatabase(config) else Databases.createJsonDatabase(config)
        return openDatabase(format).use { db ->
            db.createResource(ResourceConfiguration.newBuilder("res").hashKind(hash).build())
            db.beginResourceSession("res").use { session ->
                session.beginNodeTrx().use { trx ->
                    val wtx = trx as NodeCursor
                    if (format == Format.XML) {
                        val xtx = trx as XmlNodeTrx
                        xtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader("<r><a>first</a></r>"), XmlNodeTrx.Commit.No)
                        xtx.commit()
                        wtx.moveToDocumentRoot(); wtx.moveToFirstChild(); wtx.moveToFirstChild()
                        val first = wtx.nodeKey
                        xtx.insertSubtreeAsRightSibling(XmlShredder.createStringReader("<b>second</b>"), XmlNodeTrx.Commit.No)
                        val second = wtx.nodeKey
                        xtx.commit()
                        Fixture(first, second)
                    } else {
                        val jtx = trx as JsonNodeTrx
                        jtx.insertSubtreeAsFirstChild(
                            JsonShredder.createStringReader("""[{"a":"first"}]"""),
                            JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.NO
                        )
                        jtx.commit()
                        wtx.moveToDocumentRoot(); wtx.moveToFirstChild(); wtx.moveToFirstChild()
                        val first = wtx.nodeKey
                        jtx.insertSubtreeAsRightSibling(
                            JsonShredder.createStringReader("""{"b":"second"}"""),
                            JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.NO
                        )
                        val second = wtx.nodeKey
                        jtx.commit()
                        Fixture(first, second)
                    }
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
            val keycloak = Mockito.mock(OAuth2Auth::class.java)
            val user = NoneAuth.createUser()
            val scope = CoroutineScope(vertx.dispatcher())
            val router = Router.router(vertx)
            router.get("/json/:database/:resource").handler(BodyHandler.create())
                .handler { ctx -> ctx.put("user", user); ctx.next() }
                .coroutineHandler(scope) { JsonGet(location, keycloak, NoneAuth).handle(it) }
            router.get("/xml/:database/:resource").handler(BodyHandler.create())
                .handler { ctx -> ctx.put("user", user); ctx.next() }
                .coroutineHandler(scope) { XmlGet(location, keycloak, NoneAuth).handle(it) }
            router.head("/json/:database/:resource")
                .coroutineHandler(scope) { JsonHead(location).handle(it) }
            router.head("/xml/:database/:resource")
                .coroutineHandler(scope) { XmlHead(location).handle(it) }
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

    enum class Format(val prefix: String, val firstItemText: String, val secondItemText: String) {
        JSON("json", "first", "second"),
        XML("xml", "first", "second"),
    }

    companion object {
        @JvmStatic
        fun formatsAndHashes(): Stream<Arguments> = Stream.of(
            Arguments.of(Format.JSON, HashType.NONE),
            Arguments.of(Format.JSON, HashType.ROLLING),
            Arguments.of(Format.XML, HashType.NONE),
            Arguments.of(Format.XML, HashType.ROLLING),
        )
    }
}
