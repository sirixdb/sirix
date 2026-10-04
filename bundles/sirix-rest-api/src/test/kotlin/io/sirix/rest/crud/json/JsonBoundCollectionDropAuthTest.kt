package io.sirix.rest.crud.json

import io.brackit.query.Query
import io.sirix.access.Databases
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.compiler.optimizer.stats.Histogram
import io.sirix.query.compiler.optimizer.stats.HistogramCollector
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.json.JsonDBCollection
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.Authorization
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledOnOs
import org.junit.jupiter.api.condition.OS
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.nio.file.Files
import java.nio.file.Path
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn

@Tag("offline")
class JsonBoundCollectionDropAuthTest {
    @TempDir
    lateinit var directory: Path

    private val revisions = listOf(1, 2, StatisticsCatalog.LATEST_REVISION)
    private val fields = listOf("price", "quantity")

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun resourceBoundDropChecksLogicalRoleAndInvalidatesOnlyTargetStatistics(canDelete: Boolean) {
        val storePath = directory.resolve("store")
        val otherPath = directory.resolve("other")
        val targetPath = storePath.resolve("orders")
        val unrelatedPath = otherPath.resolve("orders")
        val affectedNames = listOf("orders", "./orders", "../store/orders")
        val unrelatedName = "../other/orders"
        val catalog = StatisticsCatalog.getInstance()
        try {
            BasicJsonDBStore.newBuilder().location(storePath).buildPathSummary(true).build().use { store ->
                BasicJsonDBStore.newBuilder().location(otherPath).buildPathSummary(true).build().use { other ->
                    val target = store.create("orders", "resource1", "{\"price\":[10,11],\"quantity\":[3,4]}")
                    advanceToRevisionTwo(target, 20)
                    target.close()
                    val unrelated = other.create("orders", "resource1", "{\"price\":[100,101],\"quantity\":[7,8]}")
                    advanceToRevisionTwo(unrelated, 200)
                    unrelated.close()
                    val affectedHistograms = affectedNames.associateWith { name ->
                        collectHistograms(store, name).also { store.lookup(name).close() }
                    }
                    val unrelatedHistograms = collectHistograms(store, unrelatedName)
                    val roles = if (canDelete) arrayOf("orders-view", "orders-delete") else arrayOf("orders-view")
                    withResourceBoundCollection(store, "orders", targetPath, userWith(*roles)) { session, context, _ ->
                        if (canDelete) {
                            val wrongName = assertThrows(HttpException::class.java) {
                                session.drop(targetPath.toRealPath().toString())
                            }
                            assertEquals(403, wrongName.statusCode)
                            assertTrue(Files.exists(targetPath))
                            executeDrop(session, context, "orders")
                        } else {
                            val exception = assertThrows(HttpException::class.java) {
                                executeDrop(session, context, "orders")
                            }
                            assertEquals(403, exception.statusCode)
                        }
                    }
                    assertEquals(!canDelete, Files.exists(targetPath))
                    for (name in affectedNames) {
                        for (index in revisions.indices) {
                            for (fieldIndex in fields.indices) {
                                val histogram = catalog.get(name, "resource1", fields[fieldIndex], revisions[index])
                                if (canDelete) {
                                    assertNull(histogram)
                                } else {
                                    assertSame(affectedHistograms.getValue(name)[index * fields.size + fieldIndex], histogram)
                                }
                            }
                        }
                    }
                    assertTrue(Files.exists(unrelatedPath))
                    for (index in revisions.indices) {
                        for (fieldIndex in fields.indices) {
                            assertSame(unrelatedHistograms[index * fields.size + fieldIndex],
                                catalog.get(unrelatedName, "resource1", fields[fieldIndex], revisions[index]))
                        }
                    }
                    val histogram = requireNotNull(HistogramCollector(store).collect(unrelatedName, "resource1", "price",
                        HistogramCollector.DEFAULT_SAMPLE_SIZE, Histogram.DEFAULT_BUCKET_COUNT, 2))
                    assertEquals(200.0, histogram.minValue())
                    assertEquals(201.0, histogram.maxValue())
                }
            }
        } finally {
            (affectedNames + unrelatedName).forEach { catalog.invalidateDatabase(it) }
            Databases.removeDatabase(targetPath)
            Databases.removeDatabase(unrelatedPath)
        }
    }

    @ParameterizedTest
    @CsvSource(value = ["[1]|false", "{\"price\":[1,2]}|false", "[true]|true", "[\"value\"]|true",
        "[null]|true", "[12]|true"], delimiter = '|')
    fun authorizedDropWorksForEveryResourceBoundJsonItem(document: String, bindChild: Boolean) {
        val databasePath = directory.resolve("orders")
        try {
            BasicJsonDBStore.newBuilder().location(directory).build().use { store ->
                store.create("orders", "resource1", document).close()
                withResourceBoundCollection(store, "orders", databasePath, userWith("orders-view", "orders-delete"), bindChild) {
                    session, context, _ -> executeDrop(session, context, "orders")
                }
                assertFalse(Files.exists(databasePath))
                assertNull(store.lookup("orders"))
            }
        } finally {
            StatisticsCatalog.getInstance().invalidateDatabase("orders")
            Databases.removeDatabase(databasePath)
        }
    }

    @Test
    @EnabledOnOs(OS.LINUX, OS.MAC)
    fun boundAliasDeletionKeepsItsOriginalTargetAfterAliasChanges() {
        val storePath = directory.resolve("store")
        val targetPath = storePath.resolve("orders")
        val otherPath = directory.resolve("other")
        val unrelatedPath = otherPath.resolve("orders")
        val aliasPath = storePath.resolve("alias")
        val user = userWith("alias-view", "alias-delete")
        try {
            BasicJsonDBStore.newBuilder().location(storePath).build().use { store ->
                BasicJsonDBStore.newBuilder().location(otherPath).build().use { other ->
                    store.create("orders", "resource1", "[\"target\"]").close()
                    other.create("orders", "resource1", "[\"unrelated\"]").close()
                    Files.createSymbolicLink(aliasPath, targetPath.toRealPath())
                    withResourceBoundCollection(store, "alias", targetPath, user) { _, _, collection ->
                        Files.delete(aliasPath)
                        Files.createSymbolicLink(aliasPath, unrelatedPath.toRealPath())
                        AuthCheckingJsonDBCollection(collection, user, Mockito.mock(AuthorizationProvider::class.java)).delete()
                    }
                    assertFalse(Files.exists(targetPath))
                    assertTrue(Files.exists(unrelatedPath))
                    store.lookup("alias").database.beginResourceSession("resource1").use { session ->
                        session.beginNodeReadOnlyTrx().use { reader ->
                            assertTrue(reader.moveToFirstChild())
                            assertTrue(reader.moveToFirstChild())
                            assertEquals("unrelated", reader.value)
                        }
                    }
                }
            }
        } finally {
            StatisticsCatalog.getInstance().invalidateDatabase("orders")
            StatisticsCatalog.getInstance().invalidateDatabase("alias")
            Databases.removeDatabase(targetPath)
            Databases.removeDatabase(unrelatedPath)
        }
    }

    private fun userWith(vararg roles: String): User {
        val user = User.fromName("test-user")
        val authorizations: Set<Authorization> = roles.map { RoleBasedAuthorization.create(it) }.toSet()
        user.authorizations().put("test-provider", authorizations)
        return user
    }

    private fun withResourceBoundCollection(store: BasicJsonDBStore, name: String, target: Path, user: User,
        bindChild: Boolean = false,
        action: (JsonSessionDBStore, SirixQueryContext, JsonDBCollection) -> Unit) {
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val sessionStore = JsonSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
        val handler = JsonGet(store.location, Mockito.mock(OAuth2Auth::class.java), authz)
        Databases.openJsonDatabase(target).use { database ->
            val collection = runBlocking {
                suspendCoroutineUninterceptedOrReturn<JsonDBCollection> { continuation ->
                    JsonGetTestBinding.collection(handler, name, database, continuation)
                }
            }
            SirixQueryContext.createWithJsonStore(sessionStore).use { context ->
                database.beginResourceSession("resource1").use { session ->
                    session.beginNodeReadOnlyTrx().use { reader ->
                        assertTrue(reader.moveToFirstChild())
                        if (bindChild) {
                            assertTrue(reader.moveToFirstChild())
                        }
                        JsonGetTestBinding.bind(handler, reader, collection, context, sessionStore)
                        assertSame(database, sessionStore.lookup(name).database)
                        assertTrue(context.contextItem != null)
                        action(sessionStore, context, collection)
                    }
                }
            }
        }
    }

    private fun executeDrop(store: JsonSessionDBStore, context: SirixQueryContext, name: String) {
        SirixCompileChain.createWithJsonStore(store).use { chain ->
            Query(chain, "jn:drop-database('$name')").evaluate(context)
        }
    }

    private fun advanceToRevisionTwo(collection: JsonDBCollection, price: Int) {
        collection.database.beginResourceSession("resource1").use { session ->
            session.beginNodeTrx().use { writer ->
                assertEquals(1, session.mostRecentRevisionNumber)
                assertTrue(writer.moveToFirstChild())
                assertTrue(writer.moveToFirstChild())
                assertEquals("price", writer.name?.localName)
                assertTrue(writer.moveToFirstChild())
                writer.setNumberValue(price)
                assertTrue(writer.moveToRightSibling())
                writer.setNumberValue(price + 1)
                writer.commit()
                assertEquals(2, session.mostRecentRevisionNumber)
            }
        }
    }

    private fun collectHistograms(store: BasicJsonDBStore, name: String): List<Histogram> {
        val collector = HistogramCollector(store)
        val catalog = StatisticsCatalog.getInstance()
        return revisions.flatMap { revision ->
            fields.map { field ->
                assertTrue(collector.collectAndRegister(name, "resource1", field, revision))
                requireNotNull(catalog.get(name, "resource1", field, revision)).also {
                    assertEquals(2L, it.totalCount())
                }
            }
        }
    }
}
