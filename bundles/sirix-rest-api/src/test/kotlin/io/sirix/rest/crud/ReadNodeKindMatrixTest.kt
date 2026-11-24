package io.sirix.rest.crud

import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.trx.node.HashType
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
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mockito
import org.w3c.dom.Element
import org.w3c.dom.Node
import org.xml.sax.InputSource
import java.io.StringReader
import java.nio.file.Path
import javax.xml.XMLConstants
import javax.xml.parsers.DocumentBuilderFactory

@Tag("offline")
class ReadNodeKindMatrixTest {
    @TempDir
    lateinit var location: Path

    private data class Target(val key: Long, val expected: String, val page: String = expected)

    @ParameterizedTest
    @EnumSource(value = HashType::class, names = ["NONE", "ROLLING"])
    fun everyJsonNodeKindKeepsItsScopeAndHashInEveryReadBranch(hash: HashType) = runBlocking {
        val obj = """{"obj":{"x":"inside"},"arr":[3,4],"str":"first","num":7,"bool":true,"nil":null}"""
        val array = """[true,6,"raw",null,2147483648,1.5,123456789012345678901234567890]"""
        val document = "[$obj,$array]"
        Databases.createJsonDatabase(DatabaseConfiguration(location.resolve("db")))
        val targets = mutableListOf<Target>()
        Databases.openJsonDatabase(location.resolve("db")).use { db ->
            db.createResource(ResourceConfiguration.newBuilder("res").hashKind(hash).build())
            db.beginResourceSession("res").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document), JsonNodeTrx.Commit.NO)
                    writer.commit()
                }
                session.beginNodeReadOnlyTrx().use { cursor ->
                    targets.add(Target(0, document))
                    assertTrue(cursor.moveToFirstChild())
                    targets.add(Target(cursor.nodeKey, document))
                    assertTrue(cursor.moveToFirstChild())
                    val objectKey = cursor.nodeKey
                    targets.add(Target(objectKey, obj, """{"obj":{"x":"inside"},"arr":[3,4]}"""))
                    assertTrue(cursor.moveToFirstChild())
                    for (expected in listOf("""{"obj":{"x":"inside"}}""", """{"arr":[3,4]}""",
                        """{"str":"first"}""", """{"num":7}""", """{"bool":true}""", """{"nil":null}""")) {
                        targets.add(Target(cursor.nodeKey, expected))
                        cursor.moveToRightSibling()
                    }
                    assertTrue(cursor.moveTo(objectKey))
                    assertTrue(cursor.moveToRightSibling())
                    targets.add(Target(cursor.nodeKey, array, "[true,6]"))
                    assertTrue(cursor.moveToFirstChild())
                    for (expected in listOf("true", "6", "\"raw\"", "null", "2147483648", "1.5", "123456789012345678901234567890")) {
                        targets.add(Target(cursor.nodeKey, expected))
                        cursor.moveToRightSibling()
                    }
                }
            }
        }
        val checks = mutableListOf<Executable>()
        serve { client, port ->
            for (target in targets) {
                val expectedHash = expectedHash(false, hash, target.key)
                val base = "/json/db/res?nodeId=${target.key}"
                for (page in listOf(false, true)) {
                    val response = client.get(port, "localhost", base + if (page) "&nextTopLevelNodes=2" else "").send().coAwait()
                    checks.add {
                        assertSuccess(response, expectedHash, "JSON ${target.key}, record page $page")
                        assertEquals(Json.decodeValue(if (page) target.page else target.expected), Json.decodeValue(response.bodyAsString()),
                            "JSON ${target.key}, record page $page: ${response.bodyAsString()}")
                    }
                }
                for (window in listOf("", "&startResultSeqIndex=0&endResultSeqIndex=0", "&startResultSeqIndex=1&endResultSeqIndex=1")) {
                    for (query in listOf("%24%24", "1", "sdb:nodekey(%24%24)")) {
                        val response = client.get(port, "localhost", "$base&query=$query$window").send().coAwait()
                        checks.add {
                            assertSuccess(response, expectedHash, "JSON ${target.key}, query $query$window")
                            val results = response.bodyAsJsonObject()!!.getJsonArray("rest")!!
                            val emptyWindow = window.startsWith("&startResultSeqIndex=1")
                            assertEquals(if (emptyWindow) 0 else 1, results.size())
                            if (!emptyWindow) when (query) {
                                "%24%24" -> assertEquals(Json.decodeValue(target.expected), results.getJsonObject(0).getValue("revision"), "JSON ${target.key}, query $query$window: ${response.bodyAsString()}")
                                "1" -> assertEquals(1L, results.getLong(0))
                                else -> assertEquals(target.key, results.getLong(0))
                            }
                        }
                    }
                }
                val head = client.head(port, "localhost", base).send().coAwait()
                checks.add {
                    assertSuccess(head, expectedHash, "JSON HEAD ${target.key}")
                    assertEquals(0, head.body()?.length() ?: 0)
                }
            }
            assertAll(checks)
        }
    }

    @ParameterizedTest
    @EnumSource(value = HashType::class, names = ["NONE", "ROLLING"])
    fun everyXmlNodeKindKeepsItsScopeAndHashInEveryReadBranch(hash: HashType) = runBlocking {
        val document = """<r xmlns="urn:root" xmlns:p="urn:test" a="v&amp;&quot;" p:n="prefixed"><p:e>first</p:e><!--note--><?target data?><b>second</b></r>"""
        Databases.createXmlDatabase(DatabaseConfiguration(location.resolve("db")))
        val targets = mutableListOf<Target>()
        Databases.openXmlDatabase(location.resolve("db")).use { db ->
            db.createResource(ResourceConfiguration.newBuilder("res").hashKind(hash).build())
            db.beginResourceSession("res").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(XmlShredder.createStringReader(document), XmlNodeTrx.Commit.No)
                    writer.commit()
                }
                session.beginNodeReadOnlyTrx().use { cursor ->
                    targets.add(Target(0, document))
                    assertTrue(cursor.moveToFirstChild())
                    val root = cursor.nodeKey
                    targets.add(Target(root, document))
                    for (index in 0 until cursor.attributeCount) {
                        assertTrue(cursor.moveToAttribute(index))
                        val expected = if (cursor.name.prefix.isEmpty())
                            """<rest:attribute xmlns:rest="https://sirix.io/rest" name="a" namespace="" value="v&amp;&quot;"/>"""
                        else """<rest:attribute xmlns:rest="https://sirix.io/rest" name="p:n" namespace="urn:test" value="prefixed"/>"""
                        targets.add(Target(cursor.nodeKey, expected))
                        assertTrue(cursor.moveTo(root))
                    }
                    for (index in 0 until cursor.namespaceCount) {
                        assertTrue(cursor.moveToNamespace(index))
                        val expected = if (cursor.name.prefix.isEmpty())
                            """<rest:namespace xmlns:rest="https://sirix.io/rest" prefix="" uri="urn:root"/>"""
                        else """<rest:namespace xmlns:rest="https://sirix.io/rest" prefix="p" uri="urn:test"/>"""
                        targets.add(Target(cursor.nodeKey, expected))
                        assertTrue(cursor.moveTo(root))
                    }
                    assertTrue(cursor.moveTo(root))
                    assertTrue(cursor.moveToFirstChild())
                    val element = cursor.nodeKey
                    targets.add(Target(element, """<p:e xmlns:p="urn:test">first</p:e>"""))
                    assertTrue(cursor.moveToFirstChild())
                    targets.add(Target(cursor.nodeKey, "first"))
                    assertTrue(cursor.moveTo(element))
                    assertTrue(cursor.moveToRightSibling())
                    targets.add(Target(cursor.nodeKey, "<!--note-->"))
                    assertTrue(cursor.moveToRightSibling())
                    targets.add(Target(cursor.nodeKey, "<?target data?>"))
                }
            }
        }
        val checks = mutableListOf<Executable>()
        serve { client, port ->
            for (target in targets) {
                val expectedHash = expectedHash(true, hash, target.key)
                val base = "/xml/db/res?nodeId=${target.key}"
                val plain = client.get(port, "localhost", base).send().coAwait()
                checks.add {
                    assertSuccess(plain, expectedHash, "XML ${target.key}")
                    assertXmlSubtree(target.expected, plain.bodyAsString()!!)
                }
                for (window in listOf("", "&startResultSeqIndex=0&endResultSeqIndex=0", "&startResultSeqIndex=1&endResultSeqIndex=1")) {
                    for (query in listOf("%24%24", "1", "sdb:nodekey(%24%24)")) {
                        val response = client.get(port, "localhost", "$base&query=$query$window").send().coAwait()
                        checks.add {
                            assertSuccess(response, expectedHash, "XML ${target.key}, query $query$window")
                            if (window.startsWith("&startResultSeqIndex=1")) {
                                val envelope = parseXml(response.bodyAsString()!!)
                                assertEquals(0, envelope.getElementsByTagName("*").length)
                                assertTrue(envelope.textContent.isBlank())
                            } else if (query == "%24%24") {
                                assertXmlSubtree(target.expected, response.bodyAsString()!!)
                            } else {
                                val expected = if (query == "1") "1" else target.key.toString()
                                assertEquals(expected, parseXml(response.bodyAsString()!!).textContent.trim())
                            }
                        }
                    }
                }
                val head = client.head(port, "localhost", base).send().coAwait()
                checks.add {
                    assertSuccess(head, expectedHash, "XML HEAD ${target.key}")
                    assertEquals(0, head.body()?.length() ?: 0)
                }
            }
            assertAll(checks)
        }
    }

    private fun assertXmlSubtree(expected: String, actual: String) {
        val document = parseXml(actual)
        val items = document.getElementsByTagNameNS("https://sirix.io/rest", "item")
        assertEquals(1, items.length, actual)
        val item = items.item(0)
        if (expected == "first") {
            assertEquals("first", item.textContent.trim())
            assertEquals(0, (item as Element).getElementsByTagName("*").length)
        } else {
            val expectedRoot = parseXml("<check>$expected</check>")
            assertEquals(xmlChildren(expectedRoot), xmlChildren(item), actual)
        }
    }

    private fun xmlChildren(node: Node): List<Any> = (0 until node.childNodes.length).map { node.childNodes.item(it) }
        .filter { it.nodeType != Node.TEXT_NODE || it.nodeValue.isNotBlank() }
        .map { child ->
            if (child is Element) {
                val attrs = (0 until child.attributes.length).map { child.attributes.item(it) }
                    .filter { it.namespaceURI != XMLConstants.XMLNS_ATTRIBUTE_NS_URI && it.namespaceURI != "https://sirix.io/rest" }
                    .associate { "{${it.namespaceURI ?: ""}}${it.localName ?: it.nodeName}" to it.nodeValue }
                listOf(child.namespaceURI ?: "", child.localName ?: child.nodeName, attrs, xmlChildren(child))
            } else {
                listOf(child.nodeType, child.nodeName, child.nodeValue)
            }
        }

    private fun parseXml(xml: String): Element {
        val factory = DocumentBuilderFactory.newInstance()
        factory.isNamespaceAware = true
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
        return factory.newDocumentBuilder().parse(InputSource(StringReader(xml))).documentElement
    }

    private fun expectedHash(xml: Boolean, hash: HashType, key: Long): String? {
        if (hash == HashType.NONE) return null
        val db = if (xml) Databases.openXmlDatabase(location.resolve("db")) else Databases.openJsonDatabase(location.resolve("db"))
        return db.use {
            it.beginResourceSession("res").use { session ->
                session.beginNodeReadOnlyTrx(1).use { trx ->
                    assertTrue((trx as NodeCursor).moveTo(key))
                    (trx as NodeReadOnlyTrx).hash.toString()
                }
            }
        }
    }

    private fun assertSuccess(response: HttpResponse<Buffer>, hash: String?, what: String) {
        assertEquals(200, response.statusCode(), "$what: ${response.bodyAsString()}")
        assertEquals(hash, response.getHeader(HttpHeaders.ETAG.toString()), what)
    }

    private suspend fun serve(block: suspend (WebClient, Int) -> Unit) {
        val vertx = Vertx.vertx()
        val client = WebClient.create(vertx)
        try {
            val keycloak = Mockito.mock(OAuth2Auth::class.java)
            val scope = CoroutineScope(vertx.dispatcher())
            val router = Router.router(vertx)
            for (format in listOf("json", "xml")) {
                router.get("/$format/:database/:resource").handler(BodyHandler.create())
                    .handler { ctx -> ctx.put("user", NoneAuth.createUser()); ctx.next() }
                    .coroutineHandler(scope) {
                        if (format == "json") JsonGet(location, keycloak, NoneAuth).handle(it)
                        else XmlGet(location, keycloak, NoneAuth).handle(it)
                    }
                router.head("/$format/:database/:resource").coroutineHandler(scope) {
                    if (format == "json") JsonHead(location).handle(it) else XmlHead(location).handle(it)
                }
            }
            router.route().failureHandler(routerFailureHandler())
            val server = vertx.createHttpServer().requestHandler(router).listen(0).coAwait()
            try { block(client, server.actualPort()) } finally { server.close().coAwait() }
        } finally {
            client.close()
            vertx.close().coAwait()
        }
    }
}
