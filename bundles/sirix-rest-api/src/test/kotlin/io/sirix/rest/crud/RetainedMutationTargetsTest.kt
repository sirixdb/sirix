package io.sirix.rest.crud

import com.google.gson.stream.JsonReader
import com.sun.net.httpserver.HttpServer
import io.brackit.query.Query
import io.brackit.query.jsonitem.`object`.ArrayObject
import io.brackit.query.jdm.Stream
import io.brackit.query.jdm.Type
import io.brackit.query.node.parser.DocumentParser
import io.brackit.query.node.parser.NodeSubtreeHandler
import io.brackit.query.node.parser.NodeSubtreeParser
import io.brackit.query.util.path.PathParser
import io.brackit.query.util.path.Path as QueryPath
import io.brackit.query.util.serialize.StringSerializer
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.trx.node.json.JsonIndexController
import io.sirix.api.json.JsonNodeTrx
import io.sirix.api.json.JsonResourceSession
import io.sirix.index.IndexDef
import io.sirix.index.IndexDefs
import io.sirix.index.aggregate.TemporalAggregateDeclarations
import io.sirix.index.projection.ProjectionIndexHOTStorage
import io.sirix.io.StorageType
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.rest.crud.xml.XmlSessionDBStore
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.json.shredder.JsonShredder
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.io.PrintWriter
import java.io.StringReader
import java.io.StringWriter
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@Tag("offline")
class RetainedMutationTargetsTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @ValueSource(strings = ["delete", "put"])
    fun loadRetainsLaterTargetsWhileTheFirstSourceIsBlocked(action: String) {
        createProtectedDatabase()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val secondOpened = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        Executors.newCachedThreadPool().use { handlers ->
            server.executor = handlers
            server.createContext("/first") { exchange ->
                exchange.sendResponseHeaders(200, 2)
                exchange.responseBody.use { output ->
                    entered.countDown()
                    check(release.await(30, TimeUnit.SECONDS))
                    output.write("{}".toByteArray())
                }
            }
            server.createContext("/second") { exchange ->
                secondOpened.incrementAndGet()
                exchange.sendResponseHeaders(200, 2)
                exchange.responseBody.use { it.write("{}".toByteArray()) }
            }
            server.start()
            try {
                Databases.openJsonDatabase(directory.resolve("auth")).use { observer ->
                    observer.beginResourceSession("keep").use { source ->
                        val before = witness(source)
                        Executors.newSingleThreadExecutor().use { executor ->
                            val request = executor.submit {
                                BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { delegate ->
                                    val guarded = JsonSessionDBStore(context(), delegate, user("view", "create"), authz())
                                    SirixQueryContext.createWithJsonStore(guarded).use { ctx ->
                                        SirixCompileChain.createWithJsonStore(guarded).use { chain ->
                                            val port = server.address.port
                                            val query = "jn:load('auth','unused',('http://127.0.0.1:$port/first','http://127.0.0.1:$port/second'),false())"
                                            StringSerializer(PrintWriter(StringWriter())).use { it.serialize(Query(chain, query).execute(ctx)) }
                                        }
                                    }
                                }
                            }
                            try {
                                check(entered.await(30, TimeUnit.SECONDS))
                                assertEquals(0, secondOpened.get(), "The second URI must stay unopened while the first input blocks")
                                if (action == "put") {
                                    assertFalse(observer.createResource(ResourceConfiguration.newBuilder("resource2").build()))
                                }
                                assertThrows(IllegalStateException::class.java) { observer.removeResource("resource2") }
                                unchanged(before, witness(source))
                            } finally {
                                release.countDown()
                            }
                            request.get(30, TimeUnit.SECONDS)
                        }
                        assertEquals(1, secondOpened.get())
                        for (name in listOf("resource1", "resource2")) {
                            observer.beginResourceSession(name).use { session ->
                                assertEquals(1, session.mostRecentRevisionNumber)
                                val json = StringBuilder()
                                JsonSerializer.Builder(session, json).build().call()
                                assertEquals("{}", json.toString())
                            }
                        }
                        unchanged(before, witness(source))
                    }
                }
            } finally {
                release.countDown()
                server.stop(0)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["delete", "put"])
    fun xmlBatchRetainsLaterTargets(action: String) {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        BasicXmlDBStore.newBuilder().location(directory).build().use { delegate ->
            delegate.create("auth")
            val guarded = XmlSessionDBStore(context(), delegate, user("view", "create"), authz())
            val first = object : NodeSubtreeParser {
                override fun parse(handler: NodeSubtreeHandler) {
                    entered.countDown()
                    check(release.await(30, TimeUnit.SECONDS))
                    DocumentParser("<first/>").parse(handler)
                }
            }
            Executors.newSingleThreadExecutor().use { executor ->
                val request = executor.submit { guarded.lookup("auth")!!.addAll(listOf(first, DocumentParser("<second/>"))) }
                try {
                    check(entered.await(30, TimeUnit.SECONDS))
                    Databases.openXmlDatabase(directory.resolve("auth")).use { observer ->
                        if (action == "put") {
                            assertFalse(observer.createResource(ResourceConfiguration.newBuilder("resource2").build()))
                        }
                        assertThrows(IllegalStateException::class.java) { observer.removeResource("resource2") }
                    }
                } finally {
                    release.countDown()
                }
                request.get(30, TimeUnit.SECONDS)
            }
            delegate.lookup("auth")!!.database.beginResourceSession("resource2").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber)
            }
        }
    }

    @Test
    fun xmlParserStreamClosesAfterItsParsersFinish() {
        val closed = AtomicBoolean()
        val parsers = object : Stream<NodeSubtreeParser> {
            private var consumed = false
            override fun next(): NodeSubtreeParser? {
                if (consumed) return null
                consumed = true
                return object : NodeSubtreeParser {
                    override fun parse(handler: NodeSubtreeHandler) {
                        assertFalse(closed.get(), "Parser input must stay alive until its write commits")
                        DocumentParser("<kept/>").parse(handler)
                    }
                }
            }
            override fun close() {
                closed.set(true)
            }
        }
        BasicXmlDBStore.newBuilder().location(directory).build().use { delegate ->
            XmlSessionDBStore(context(), delegate, user("create"), authz()).create("auth", parsers)
            assertEquals(true, closed.get())
            delegate.lookup("auth")!!.database.beginResourceSession("resource1").use { session ->
                assertEquals(1, session.mostRecentRevisionNumber)
            }
        }
    }

    @Test
    fun batchSourcesOpenAndCloseOneAtATime() {
        val active = AtomicInteger()
        val opened = AtomicInteger()
        val closed = AtomicInteger()
        val sources = (0 until 16).map {
            Callable {
                assertEquals(0, active.get(), "The previous source must close before the next one opens")
                opened.incrementAndGet()
                active.incrementAndGet()
                JsonReader(object : StringReader("{}") {
                    private var open = true
                    override fun close() {
                        if (open) {
                            open = false
                            active.decrementAndGet()
                            closed.incrementAndGet()
                        }
                        super.close()
                    }
                })
            }
        }
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { delegate ->
            delegate.create("auth")
            val guarded = JsonSessionDBStore(context(), delegate, user("view", "create"), authz())
            guarded.lookup("auth")!!.addAllSources(sources, ArrayObject(emptyArray(), emptyArray()))
            assertEquals(16, opened.get())
            assertEquals(16, closed.get())
            assertEquals(0, active.get())
            assertEquals(16, delegate.lookup("auth")!!.database.listResources().size)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun anotherCreateOnlyRequestIsRefusedDuringTheFirstCommit(xml: Boolean) {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        Executors.newSingleThreadExecutor().use { executor ->
            val first = executor.submit {
                if (xml) {
                    BasicXmlDBStore.newBuilder().location(directory).build().use { delegate ->
                        val parser = object : NodeSubtreeParser {
                            override fun parse(handler: NodeSubtreeHandler) {
                                entered.countDown()
                                check(release.await(30, TimeUnit.SECONDS))
                                DocumentParser("<first/>").parse(handler)
                            }
                        }
                        XmlSessionDBStore(context(), delegate, user("create"), authz()).create("auth", parser)
                    }
                } else {
                    BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { delegate ->
                        val source = object : StringReader("{}") {
                            override fun read(buffer: CharArray, offset: Int, length: Int): Int {
                                entered.countDown()
                                check(release.await(30, TimeUnit.SECONDS))
                                return super.read(buffer, offset, length)
                            }
                        }
                        JsonSessionDBStore(context(), delegate, user("create"), authz()).create("auth", "records", JsonReader(source))
                    }
                }
            }
            try {
                check(entered.await(30, TimeUnit.SECONDS))
                val refused = assertThrows(HttpException::class.java) {
                    if (xml) {
                        BasicXmlDBStore.newBuilder().location(directory).build().use {
                            XmlSessionDBStore(context(), it, user("create"), authz()).create("auth", DocumentParser("<second/>"))
                        }
                    } else {
                        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use {
                            JsonSessionDBStore(context(), it, user("create"), authz()).create("auth", "other", "{}")
                        }
                    }
                }
                assertEquals(403, refused.statusCode)
            } finally {
                release.countDown()
            }
            first.get(30, TimeUnit.SECONDS)
        }
        if (xml) {
            Databases.openXmlDatabase(directory.resolve("auth")).use { database ->
                assertEquals(setOf("resource1"), database.listResources().map { it.fileName.toString() }.toSet())
                database.beginResourceSession("resource1").use { session -> assertEquals(1, session.mostRecentRevisionNumber) }
            }
        } else {
            Databases.openJsonDatabase(directory.resolve("auth")).use { database ->
                assertEquals(setOf("records"), database.listResources().map { it.fileName.toString() }.toSet())
                database.beginResourceSession("records").use { session -> assertEquals(1, session.mostRecentRevisionNumber) }
            }
        }
    }

    private data class Witness(val revision: Int, val catalogue: String, val projection: ByteArray, val aggregate: ByteArray)

    private fun witness(session: JsonResourceSession): Witness {
        val revision = session.mostRecentRevisionNumber
        val catalogue = StringWriter()
        StringSerializer(PrintWriter(catalogue)).use { it.serialize(session.getRtxIndexController<JsonIndexController>(revision).indexes.materialize()) }
        val aggregate = DerivedAggregateWitness.header(session, revision)
        session.beginNodeReadOnlyTrx(revision).use { reader ->
            val projection = ProjectionIndexHOTStorage.readMetadataBlob(reader.storageEngineReader, 0)
            assertNotNull(projection)
            return Witness(revision, catalogue.toString(), projection!!, aggregate)
        }
    }

    private fun unchanged(before: Witness, after: Witness) {
        assertEquals(before.revision, after.revision)
        assertEquals(before.catalogue, after.catalogue)
        assertArrayEquals(before.projection, after.projection)
        assertArrayEquals(before.aggregate, after.aggregate)
    }

    private fun createProtectedDatabase() {
        val path = directory.resolve("auth")
        Databases.createJsonDatabase(DatabaseConfiguration(path))
        Databases.openJsonDatabase(path).use { database ->
            database.createResource(ResourceConfiguration.newBuilder("keep").storageType(StorageType.FILE_CHANNEL).validTimePaths("lo", "hi").build())
            database.beginResourceSession("keep").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"g\":1,\"n\":1,\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2025-01-01T00:00:00Z\"}]"), JsonNodeTrx.Commit.NO)
                    val fields = listOf("g", "n", "lo", "hi")
                    val paths = fields.map { QueryPath.parse("/[]/$it", PathParser.Type.JSON) }
                    val projection = IndexDefs.createProjectionIdxDef(QueryPath.parse("/[]", PathParser.Type.JSON), paths,
                        listOf(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON)
                    val controller: JsonIndexController = session.getWtxIndexController(writer.revisionNumber)
                    controller.createIndexes(setOf(projection), writer)
                    TemporalAggregateDeclarations.declare(controller, writer, "/[]", listOf("g"), listOf("sum(n)"))
                    writer.commit()
                }
            }
        }
    }

    private fun context(): RoutingContext = Mockito.mock(RoutingContext::class.java)
    private fun authz(): AuthorizationProvider = Mockito.mock(AuthorizationProvider::class.java)
    private fun user(vararg roles: String): User = User.fromName("test-user").also { user ->
        user.authorizations().put("test", roles.map { RoleBasedAuthorization.create(it) }.toSet())
    }
}
