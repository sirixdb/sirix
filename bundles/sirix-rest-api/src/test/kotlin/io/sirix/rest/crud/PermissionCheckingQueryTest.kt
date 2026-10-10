package io.sirix.rest.crud

import io.brackit.query.atomic.QNm
import io.brackit.query.jdm.Type
import io.brackit.query.util.path.Path as QueryPath
import io.brackit.query.util.path.PathParser
import io.brackit.query.util.serialize.StringSerializer
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.trx.node.json.JsonIndexController
import io.sirix.api.json.JsonNodeTrx
import io.sirix.index.IndexDef
import io.sirix.index.IndexDefs
import io.sirix.index.IndexType
import io.sirix.index.aggregate.TemporalAggregateDeclarations
import io.sirix.index.aggregate.TemporalAggregateIndexes
import io.sirix.index.aggregate.TemporalAggregateStorage
import io.sirix.io.StorageType
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.service.json.shredder.JsonShredder
import io.vertx.core.Future
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.Authorization
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Path

@Tag("offline")
class PermissionCheckingQueryTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @CsvSource(
        "create,execute,false", "create,evaluate,false", "create,serialize,false", "create,paginate,false",
        "drop,execute,false", "drop,evaluate,false", "drop,serialize,false", "drop,paginate,false",
        "create,execute,true", "create,evaluate,true", "create,serialize,true", "create,paginate,true",
        "drop,execute,true", "drop,evaluate,true", "drop,serialize,true", "drop,paginate,true"
    )
    fun aggregateDdlAuthorizesBeforeMutatingTheCallerWriter(action: String, mode: String, modify: Boolean) {
        val first = createDatabase(action == "drop")
        val user = userWith(*if (modify) arrayOf("auth-view", "modify") else arrayOf("auth-view"))
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        Mockito.`when`(authz.getAuthorizations(user)).thenReturn(Future.succeededFuture())
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { store ->
            val guarded = JsonSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithJsonStore(guarded).use { context ->
                SirixCompileChain.createWithJsonStore(guarded).use { chain ->
                    store.lookup("auth").database.beginResourceSession("records").use { session ->
                        session.beginNodeTrx().use { writer ->
                            numberField(writer, first)
                            writer.setNumberValue(99)
                            val represented = writer.storageEngineWriter.revisionToRepresent
                            val revision = session.mostRecentRevisionNumber
                            val before = aggregateHeader(writer)
                            val query = if (action == "create") {
                                "jn:create-temporal-aggregate-index(jn:doc('auth','records'),'/[]','g','sum(n)')"
                            } else {
                                "jn:drop-temporal-aggregate-index(jn:doc('auth','records'),0)"
                            }
                            if (modify) {
                                run(mode, chain, context, query, keycloak, authz, user)
                            } else {
                                val failure = assertThrows(IllegalStateException::class.java) {
                                    run(mode, chain, context, query, keycloak, authz, user)
                                }
                                assertTrue(failure.message!!.contains("not allowed to modify"))
                                assertArrayEquals(before, aggregateHeader(writer))
                            }
                            val expected = if (modify) {
                                if (action == "create") 1 else 0
                            } else {
                                if (action == "drop") 1 else 0
                            }
                            assertEquals(expected, session.getWtxIndexController<JsonIndexController>(writer.revisionNumber).indexes
                                .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE))
                            assertSame(writer, session.nodeTrx.orElseThrow())
                            assertEquals(represented, writer.storageEngineWriter.revisionToRepresent)
                            assertEquals(revision, session.mostRecentRevisionNumber)
                            numberField(writer, first)
                            assertEquals(99, writer.numberValue.toInt())
                            writer.storageEngineWriter.assertTransactionWritable()
                            Mockito.verify(authz).getAuthorizations(user)
                            writer.rollback()
                        }
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["execute", "evaluate", "serialize", "paginate"])
    fun viewOnlyReadQueriesStillExecute(mode: String) {
        createDatabase(false)
        val user = userWith("auth-view")
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { store ->
            val guarded = JsonSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithJsonStore(guarded).use { context ->
                SirixCompileChain.createWithJsonStore(guarded).use { chain ->
                    assertEquals("1", run(mode, chain, context, "sum(jn:doc('auth','records')[].n)", keycloak, authz, user))
                    Mockito.verifyNoInteractions(authz)
                    store.lookup("auth").database.beginResourceSession("records").use { session ->
                        assertTrue(session.nodeTrx.isEmpty)
                        assertEquals(1, session.mostRecentRevisionNumber)
                    }
                }
            }
        }
    }

    private fun createDatabase(aggregate: Boolean): Long {
        val path = directory.resolve("auth")
        Databases.createJsonDatabase(DatabaseConfiguration(path))
        Databases.openJsonDatabase(path).use { database ->
            database.createResource(ResourceConfiguration.newBuilder("records").validTimePaths("lo", "hi")
                .storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build())
            database.beginResourceSession("records").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"g\":1,\"n\":1}]"), JsonNodeTrx.Commit.NO)
                    writer.moveToDocumentRoot()
                    writer.moveToFirstChild()
                    writer.moveToFirstChild()
                    val first = writer.nodeKey
                    val fields = listOf("g", "n", "lo", "hi")
                    val paths: List<QueryPath<QNm>> = fields.map { QueryPath.parse("/[]/$it", PathParser.Type.JSON) }
                    val projection = IndexDefs.createProjectionIdxDef(QueryPath.parse("/[]", PathParser.Type.JSON),
                        paths,
                        listOf(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON)
                    val controller: JsonIndexController = session.getWtxIndexController(writer.revisionNumber)
                    controller.createIndexes(setOf(projection), writer)
                    if (aggregate) {
                        TemporalAggregateDeclarations.declare(controller, writer, "/[]", listOf("g"), listOf("sum(n)"))
                    }
                    writer.commit()
                    return first
                }
            }
        }
    }

    private fun aggregateHeader(writer: JsonNodeTrx): ByteArray? =
        TemporalAggregateStorage.Reader(writer.storageEngineWriter, 0).use { it.get(TemporalAggregateIndexes.headerKey()) }

    private fun numberField(writer: JsonNodeTrx, record: Long) {
        writer.moveTo(record)
        writer.moveToFirstChild()
        writer.moveToRightSibling()
        writer.moveToFirstChild()
    }

    private fun userWith(vararg roles: String): User {
        val user = User.fromName("test-user")
        val authorizations: Set<Authorization> = roles.map { RoleBasedAuthorization.create(it) }.toSet()
        user.authorizations().put("test-provider", authorizations)
        return user
    }

    private fun run(mode: String, chain: SirixCompileChain, context: SirixQueryContext, query: String,
                    keycloak: OAuth2Auth, authz: AuthorizationProvider, user: User): String {
        val output = StringWriter()
        val writer = PrintWriter(output)
        val checked = PermissionCheckingQuery(chain, query, keycloak, user, authz)
        when (mode) {
            "execute", "evaluate" -> StringSerializer(writer).use {
                it.serialize(if (mode == "execute") checked.execute(context) else checked.evaluate(context))
            }
            "serialize" -> checked.serialize(context, writer)
            "paginate" -> QuerySerializer.serializePaginated(chain, query, context, 0, null, keycloak, authz, user,
                StringSerializer(writer)) { serializer, item -> serializer.serialize(item) }
            else -> error(mode)
        }
        writer.flush()
        return output.toString().trim()
    }
}
