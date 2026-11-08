package io.sirix.rest.crud

import io.brackit.query.atomic.QNm
import io.brackit.query.node.parser.DocumentParser
import io.brackit.query.node.parser.NodeSubtreeParser
import io.brackit.query.node.stream.ArrayStream
import io.brackit.query.util.serialize.StringSerializer
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.trx.node.xml.XmlIndexController
import io.sirix.io.StorageType
import io.sirix.index.IndexDef
import io.sirix.index.IndexDefs
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.rest.crud.xml.XmlSessionDBStore
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.xml.serialize.XmlSerializer
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import org.mockito.stubbing.Answer
import java.io.ByteArrayOutputStream
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Tag("offline")
class StoreMutationBoundaryTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun xmlCreationCollisionRequiresModify(streaming: Boolean) {
        val putCreated = AtomicBoolean()
        lateinit var before: XmlWitness
        BasicXmlDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { delegate ->
            val store = XmlSessionDBStore(context(), delegate, user("view", "create"), authorizations())
            val putAtCreation = Answer<Any?> { invocation ->
                if (invocation.method.name == "newBuilder" && invocation.getArgument<String>(0) == "resource1"
                    && putCreated.compareAndSet(false, true)) {
                    Databases.openXmlDatabase(directory.resolve("auth")).use { database ->
                        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource1")
                            .storageType(StorageType.FILE_CHANNEL).build()))
                        database.beginResourceSession("resource1").use { session ->
                            session.beginNodeTrx().use { writer ->
                                writer.insertElementAsFirstChild(QNm("root"))
                                writer.insertTextAsFirstChild("preserved")
                                session.getWtxIndexController<XmlIndexController>(writer.revisionNumber)
                                    .createIndexes(setOf(IndexDefs.createNameIdxDef(0, IndexDef.DbType.XML)), writer)
                                writer.commit()
                            }
                        }
                    }
                    before = xmlWitness(delegate)
                }
                invocation.callRealMethod()
            }
            Mockito.mockStatic(ResourceConfiguration::class.java, putAtCreation).use {
                val failure = runCatching {
                    val parser = DocumentParser("<root>replacement</root>")
                    if (streaming) store.create("auth", ArrayStream(arrayOf<NodeSubtreeParser>(parser)))
                    else store.create("auth", parser)
                }.exceptionOrNull()
                assertTrue(putCreated.get(), "The authorized PUT must create the resolved target before store creation")
                assertEquals(before, xmlWitness(delegate), "A collision must preserve the resource created by PUT")
                assertEquals(403, assertThrows(HttpException::class.java) { failure?.let { throw it } }.statusCode)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 4, 5])
    fun xmlReplacementRequiresDeleteAtEveryParserEntryPoint(overload: Int) {
        BasicXmlDBStore.newBuilder().location(directory).build().use { delegate ->
            delegate.create("auth", DocumentParser("<root>preserved</root>"))
            val before = xmlWitness(delegate)
            val refused = XmlSessionDBStore(context(), delegate, user("view", "create"), authorizations())
            assertEquals(403, assertThrows(HttpException::class.java) { replaceXml(refused, overload) }.statusCode)
            assertEquals(before, xmlWitness(delegate))
            val allowed = XmlSessionDBStore(context(), delegate, user("view", "create", "delete"), authorizations())
            replaceXml(allowed, overload)
            val after = xmlWitness(delegate)
            assertEquals(setOf("resource1"), after.resources)
            assertEquals(1, after.revisions)
            assertEquals("<root>replacement</root>", after.document)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 4])
    fun xmlAddsRequireModifyForExistingTargets(overload: Int) {
        BasicXmlDBStore.newBuilder().location(directory).build().use { delegate ->
            delegate.create("auth").add("resource2", DocumentParser("<root>preserved</root>")).trx.close()
            val before = xmlWitness(delegate, "resource2")
            val refused = XmlSessionDBStore(context(), delegate, user("view", "create"), authorizations())
            assertEquals(403, assertThrows(HttpException::class.java) { appendXml(refused, overload) }.statusCode)
            assertEquals(before, xmlWitness(delegate, "resource2"))
            val allowed = XmlSessionDBStore(context(), delegate, user("view", "create", "modify"), authorizations())
            appendXml(allowed, overload)
            assertEquals(before.revisions + if (overload == 4) 2 else 1, xmlWitness(delegate, "resource2").revisions)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun concurrentCreatorsCannotReplaceEachOthersDatabase(xml: Boolean) {
        val ready = CountDownLatch(2)
        val start = CountDownLatch(1)
        val results = Executors.newFixedThreadPool(2).use { executor ->
            val calls = (0..1).map { id ->
                executor.submit<Pair<Int, Int>> {
                    ready.countDown()
                    check(start.await(30, TimeUnit.SECONDS))
                    val status = try {
                        if (xml) {
                            BasicXmlDBStore.newBuilder().location(directory).build().use { delegate ->
                                XmlSessionDBStore(context(), delegate, user("create"), authorizations())
                                    .create("auth", DocumentParser("<root>$id</root>"))
                            }
                        } else {
                            BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { delegate ->
                                JsonSessionDBStore(context(), delegate, user("create"), authorizations())
                                    .create("auth", "records", "{\"winner\":$id}")
                            }
                        }
                        200
                    } catch (failure: HttpException) {
                        failure.statusCode
                    }
                    id to status
                }
            }
            check(ready.await(30, TimeUnit.SECONDS))
            start.countDown()
            calls.map { it.get(30, TimeUnit.SECONDS) }
        }
        assertEquals(listOf(200, 403), results.map { it.second }.sorted())
        val winner = results.single { it.second == 200 }.first
        Databases.clearGlobalCaches()
        if (xml) {
            BasicXmlDBStore.newBuilder().location(directory).build().use { delegate ->
                val state = xmlWitness(delegate)
                assertEquals(setOf("resource1"), state.resources)
                assertEquals(1, state.revisions)
                assertEquals("<root>$winner</root>", state.document)
            }
        } else {
            Databases.openJsonDatabase(directory.resolve("auth")).use { database ->
                assertEquals(setOf("records"), database.listResources().map { it.fileName.toString() }.toSet())
                database.beginResourceSession("records").use { session ->
                    assertEquals(1, session.mostRecentRevisionNumber)
                    val output = StringBuilder()
                    JsonSerializer.Builder(session, output).build().call()
                    assertEquals("{\"winner\":$winner}", output.toString())
                }
            }
        }
    }

    private fun replaceXml(store: XmlSessionDBStore, overload: Int) {
        val parser = DocumentParser("<root>replacement</root>")
        when (overload) {
            1 -> store.create("auth", parser)
            2 -> store.create("auth", parser, "replacement", null)
            3 -> store.create("auth", ArrayStream(arrayOf<NodeSubtreeParser>(parser)))
            4 -> store.create("auth", "fresh", parser)
            5 -> store.create("auth", "fresh", parser, "replacement", null)
        }
    }

    private fun appendXml(store: XmlSessionDBStore, overload: Int) {
        val collection = store.lookup("auth")!!
        val parser = DocumentParser("<added/>")
        when (overload) {
            1 -> collection.add("resource2", parser).trx.close()
            2 -> collection.add("resource2", parser, "append", null).trx.close()
            3 -> collection.add(parser).trx.close()
            4 -> collection.addAll(listOf(parser, DocumentParser("<second/>")))
        }
    }

    private data class XmlWitness(
        val resources: Set<String>,
        val revisions: Int,
        val catalogue: String,
        val document: String
    )

    private fun xmlWitness(store: BasicXmlDBStore, resourceName: String = "resource1"): XmlWitness {
        val database = store.lookup("auth")!!.database
        database.beginResourceSession(resourceName).use { session ->
            val revision = session.mostRecentRevisionNumber
            val catalogue = StringWriter()
            StringSerializer(PrintWriter(catalogue)).use {
                it.serialize(session.getRtxIndexController<XmlIndexController>(revision).indexes.materialize())
            }
            val document = ByteArrayOutputStream()
            XmlSerializer.newBuilder(session, document, revision).build().call()
            return XmlWitness(database.listResources().map { it.fileName.toString() }.toSet(), revision, catalogue.toString(), document.toString(Charsets.UTF_8))
        }
    }

    private fun user(vararg roles: String): User {
        val user = User.fromName("test-user")
        user.authorizations().put("test", roles.map { RoleBasedAuthorization.create(it) }.toSet())
        return user
    }

    private fun context(): RoutingContext = Mockito.mock(RoutingContext::class.java)

    private fun authorizations(): AuthorizationProvider = Mockito.mock(AuthorizationProvider::class.java)
}
