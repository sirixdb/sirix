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
import io.vertx.core.json.Json
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
            for (nodeId in listOf("abc", "1.5", "", "9223372036854775808", "%0A", "%0D", "%09", "%00", "%01", "%08", "%0C", "%22", "%5C")) {
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
    fun queryReturnsSelectedContextETagForEveryResultMode(format: Format, hash: HashType) = runBlocking {
        val fixture = createResource(format, hash)
        serve { client, port ->
            for (revision in listOf(1, 2)) {
                for (query in listOf("%24%24", "1")) {
                    for (window in listOf("", "&startResultSeqIndex=0&endResultSeqIndex=0")) {
                        val response = get(client, port, format,
                            "?nodeId=${fixture.firstItemKey}&revision=$revision&query=$query$window")
                        assertEquals(200, response.statusCode(), response.bodyAsString())
                        if (query == "%24%24") {
                            assertTrue(response.bodyAsString().contains("first"), response.bodyAsString())
                            assertFalse(response.bodyAsString().contains("second"), response.bodyAsString())
                        } else {
                            assertTrue(response.bodyAsString().contains("1"), response.bodyAsString())
                        }
                        val expected = if (hash == HashType.NONE) null
                            else nodeHash(format, revision, fixture.firstItemKey).toString()
                        assertEquals(expected, response.getHeader(HttpHeaders.ETAG.toString()))
                    }
                }
            }
            val failed = get(client, port, format, "?nodeId=${fixture.firstItemKey}&query=1%20div%200")
            assertError(failed, 400, "query evaluation failure")
        }
    }

    @ParameterizedTest
    @MethodSource("jsonHashes")
    fun recordPaginationStaysInsideSelectedSubtree(hash: HashType) = runBlocking {
        val fixture = createResource(Format.JSON, hash)
        val fieldKey = childKey(Format.JSON, fixture.firstItemKey)
        serve { client, port ->
            val initial = get(client, port, Format.JSON, "?nodeId=${fixture.firstItemKey}&nextTopLevelNodes=2")
            assertEquals(200, initial.statusCode(), initial.bodyAsString())
            assertEquals(Json.decodeValue("""{"a":"first","c":"continued"}"""), Json.decodeValue(initial.bodyAsString()))
            val limited = get(client, port, Format.JSON, "?nodeId=${fixture.firstItemKey}&nextTopLevelNodes=1")
            assertEquals(200, limited.statusCode(), limited.bodyAsString())
            assertEquals(Json.decodeValue("""{"a":"first"}"""), Json.decodeValue(limited.bodyAsString()))
            val continued = get(client, port, Format.JSON,
                "?nodeId=${fixture.firstItemKey}&nextTopLevelNodes=2&startNodeKey=$fieldKey")
            assertEquals(200, continued.statusCode(), continued.bodyAsString())
            assertEquals(Json.decodeValue("""{"value":[{"c":"continued"}]}"""), Json.decodeValue(continued.bodyAsString()))
            val exhausted = get(client, port, Format.JSON,
                "?nodeId=${fixture.firstItemKey}&nextTopLevelNodes=2&startNodeKey=${fixture.firstItemKey}")
            assertEquals(200, exhausted.statusCode(), exhausted.bodyAsString())
            assertEquals(Json.decodeValue("""{"value":[]}"""), Json.decodeValue(exhausted.bodyAsString()))
            for (response in listOf(initial, limited, continued, exhausted)) {
                val expected = if (hash == HashType.NONE) null else nodeHash(Format.JSON, 2, fixture.firstItemKey).toString()
                assertEquals(expected, response.getHeader(HttpHeaders.ETAG.toString()))
            }
            for (cursor in listOf(fixture.secondItemKey, 0L, -1L, 999999L)) {
                assertError(get(client, port, Format.JSON,
                    "?nodeId=${fixture.firstItemKey}&nextTopLevelNodes=2&startNodeKey=$cursor"), 400, "outside subtree")
            }
            val earlier = get(client, port, Format.JSON,
                "?nodeId=0&nextTopLevelNodes=2&startNodeKey=${fixture.secondItemKey}&start-revision=1&end-revision=2")
            assertError(earlier, 400, "cursor missing in an earlier revision")
            val whole = get(client, port, Format.JSON, "?nextTopLevelNodes=2")
            assertEquals(200, whole.statusCode(), whole.bodyAsString())
            assertEquals(Json.decodeValue("""[{"a":"first","c":"continued"},{"b":"second"}]"""),
                Json.decodeValue(whole.bodyAsString()))
            val documentRoot = get(client, port, Format.JSON, "?nodeId=0&nextTopLevelNodes=2&startNodeKey=0")
            assertEquals(200, documentRoot.statusCode(), documentRoot.bodyAsString())
            assertEquals(Json.decodeValue("""[{"a":"first","c":"continued"},{"b":"second"}]"""),
                Json.decodeValue(documentRoot.bodyAsString()))
            val history = get(client, port, Format.JSON,
                "?nodeId=${fixture.firstItemKey}&nextTopLevelNodes=2&start-revision=1&end-revision=2")
            assertEquals(200, history.statusCode(), history.bodyAsString())
            assertEquals(Json.decodeValue("""{"sirix":[{"revisionNumber":1,"revision":{"a":"first","c":"continued"}},{"revisionNumber":2,"revision":{"a":"first","c":"continued"}}]}"""),
                Json.decodeValue(history.bodyAsString()))
            val metadata = get(client, port, Format.JSON,
                "?nodeId=${fixture.firstItemKey}&nextTopLevelNodes=2&withMetaData=nodeKey")
            assertEquals(200, metadata.statusCode(), metadata.bodyAsString())
            assertEquals(fixture.firstItemKey, metadata.bodyAsJsonObject().getJsonObject("metadata").getLong("nodeKey"))
            assertTrue(metadata.bodyAsString().contains("first"), metadata.bodyAsString())
            assertFalse(metadata.bodyAsString().contains("second"), metadata.bodyAsString())
            val wholeContinuation = get(client, port, Format.JSON,
                "?nextTopLevelNodes=2&startNodeKey=${fixture.firstItemKey}")
            assertEquals(200, wholeContinuation.statusCode(), wholeContinuation.bodyAsString())
            assertEquals(Json.decodeValue("""{"value":[{"b":"second"}]}"""), Json.decodeValue(wholeContinuation.bodyAsString()))
            val field = get(client, port, Format.JSON, "?nodeId=$fieldKey&nextTopLevelNodes=2")
            assertEquals(200, field.statusCode(), field.bodyAsString())
            assertEquals(Json.decodeValue("""{"a":"first"}"""), Json.decodeValue(field.bodyAsString()))
        }
    }

    @ParameterizedTest
    @MethodSource("jsonHashes")
    fun namedRecordRootsRetainFieldNamesAndETags(hash: HashType) = runBlocking {
        val config = DatabaseConfiguration(location.resolve("db"))
        Databases.createJsonDatabase(config)
        val keys = linkedMapOf<String, Long>()
        Databases.openJsonDatabase(location.resolve("db")).use { db ->
            db.createResource(ResourceConfiguration.newBuilder("res").hashKind(hash).build())
            db.beginResourceSession("res").use { session ->
                session.beginNodeTrx().use { trx ->
                    trx.insertSubtreeAsFirstChild(
                        JsonShredder.createStringReader("""{"arr":[1,2,3],"obj":{"a":1,"b":2,"c":3},"scalar":"first","outside":"second"}"""),
                        JsonNodeTrx.Commit.NO
                    )
                    trx.commit()
                }
                session.beginNodeReadOnlyTrx().use { cursor ->
                    assertTrue(cursor.moveToFirstChild())
                    assertTrue(cursor.moveToFirstChild())
                    do {
                        val name = cursor.name.stringValue()
                        if (name != "outside") keys[name] = cursor.nodeKey
                    } while (cursor.moveToRightSibling())
                }
            }
        }
        val expectedPages = mapOf(
            "arr" to """{"arr":[1,2]}""",
            "obj" to """{"obj":{"a":1,"b":2}}""",
            "scalar" to """{"scalar":"first"}"""
        )
        assertEquals(expectedPages.keys, keys.keys)
        serve { client, port ->
            for ((name, key) in keys) {
                val expectedHash = if (hash == HashType.NONE) null else nodeHash(Format.JSON, 1, key).toString()
                val plain = get(client, port, Format.JSON, "?nodeId=$key&nextTopLevelNodes=2")
                assertEquals(200, plain.statusCode(), plain.bodyAsString())
                assertEquals(Json.decodeValue(expectedPages.getValue(name)), Json.decodeValue(plain.bodyAsString()))
                assertEquals(expectedHash, plain.getHeader(HttpHeaders.ETAG.toString()))
                for (mode in listOf("nodeKey", "nodeKeyAndChildCount", "all")) {
                    val response = get(client, port, Format.JSON,
                        "?nodeId=$key&nextTopLevelNodes=2&withMetaData=$mode")
                    assertEquals(200, response.statusCode(), response.bodyAsString())
                    val page = response.bodyAsJsonObject()!!
                    assertEquals(name, page.getString("key"))
                    assertEquals(key, page.getJsonObject("metadata")!!.getLong("nodeKey"))
                    assertEquals(if (name == "scalar") 1 else 2, page.getJsonArray("value")!!.size())
                    assertEquals(expectedHash, response.getHeader(HttpHeaders.ETAG.toString()))
                    if (name != "scalar") {
                        val lastLoaded = childKey(Format.JSON, key)
                        val continued = get(client, port, Format.JSON,
                            "?nodeId=$key&nextTopLevelNodes=2&startNodeKey=$lastLoaded&withMetaData=$mode")
                        assertEquals(200, continued.statusCode(), continued.bodyAsString())
                        assertEquals(name, continued.bodyAsJsonObject()!!.getString("key"))
                        assertEquals(2, continued.bodyAsJsonObject()!!.getJsonArray("value")!!.size())
                        assertEquals(expectedHash, continued.getHeader(HttpHeaders.ETAG.toString()))
                    }
                }
            }
        }
    }

    private fun childKey(format: Format, parentKey: Long): Long = openDatabase(format).use { db ->
        db.beginResourceSession("res").use { session ->
            session.beginNodeReadOnlyTrx().use { trx ->
                val cursor = trx as NodeCursor
                assertTrue(cursor.moveTo(parentKey))
                assertTrue(cursor.moveToFirstChild())
                cursor.nodeKey
            }
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
                assertNull(etag)
            } else {
                assertEquals(nodeHash(format, 2, fixture.firstItemKey).toString(), etag)
                assertEquals(etag, get(client, port, format, "?nodeId=${fixture.firstItemKey}")
                    .getHeader(HttpHeaders.ETAG.toString()))
            }
            // nodeId absent: the ETag names the document root's first child, as on GET (REST-203).
            val whole = head(client, port, format, "")
            assertEquals(200, whole.statusCode())
            if (hash == HashType.NONE) {
                assertNull(whole.getHeader(HttpHeaders.ETAG.toString()))
            } else {
                assertEquals(documentFirstChildHash(format, 2).toString(), whole.getHeader(HttpHeaders.ETAG.toString()))
                assertEquals(whole.getHeader(HttpHeaders.ETAG.toString()),
                    get(client, port, format, "").getHeader(HttpHeaders.ETAG.toString()))
            }
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

    private fun documentFirstChildHash(format: Format, revision: Int): Long =
        openDatabase(format).use { db ->
            db.beginResourceSession("res").use { session ->
                session.beginNodeReadOnlyTrx(revision).use { rtx ->
                    (rtx as NodeCursor).moveToDocumentRoot()
                    assertTrue((rtx as NodeCursor).moveToFirstChild())
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
                            JsonShredder.createStringReader("""[{"a":"first","c":"continued"}]"""),
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
        fun jsonHashes(): Stream<HashType> = Stream.of(HashType.NONE, HashType.ROLLING)

        @JvmStatic
        fun formatsAndHashes(): Stream<Arguments> = Stream.of(
            Arguments.of(Format.JSON, HashType.NONE),
            Arguments.of(Format.JSON, HashType.ROLLING),
            Arguments.of(Format.XML, HashType.NONE),
            Arguments.of(Format.XML, HashType.ROLLING),
        )
    }
}
