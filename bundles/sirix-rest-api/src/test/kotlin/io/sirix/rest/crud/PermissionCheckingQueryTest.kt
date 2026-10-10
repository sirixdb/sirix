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
import io.sirix.index.projection.ProjectionIndexHOTStorage
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
import java.time.Instant

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

    @ParameterizedTest
    @CsvSource(
        "execute,false,0,false", "execute,true,0,false", "execute,false,1,false", "execute,true,1,false",
        "execute,false,2,false", "execute,true,2,false", "execute,false,3,false", "execute,true,3,false",
        "paginate,false,0,false", "paginate,true,0,false", "paginate,false,1,false", "paginate,true,1,false",
        "paginate,false,2,false", "paginate,true,2,false", "paginate,false,3,false", "paginate,true,3,false",
        "execute,false,0,true", "execute,true,0,true", "execute,false,1,true", "execute,true,1,true",
        "execute,false,2,true", "execute,true,2,true", "execute,false,3,true", "execute,true,3,true",
        "paginate,false,0,true", "paginate,true,0,true", "paginate,false,1,true", "paginate,true,1,true",
        "paginate,false,2,true", "paginate,true,2,true", "paginate,false,3,true", "paginate,true,3,true"
    )
    fun projectionDropAuthorizesBothSignaturesAndCommitWrappers(
        mode: String, withId: Boolean, commitArguments: Int, modify: Boolean
    ) {
        createDatabase(true)
        val user = userWith(*if (modify) arrayOf("auth-view", "modify") else arrayOf("auth-view"))
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        Mockito.`when`(authz.getAuthorizations(user)).thenReturn(Future.succeededFuture())
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        val beforeProjection: ByteArray
        val beforeAggregate: ByteArray
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { store ->
            val guarded = JsonSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithJsonStore(guarded).use { context ->
                SirixCompileChain.createWithJsonStore(guarded).use { chain ->
                    store.lookup("auth").database.beginResourceSession("records").use { session ->
                        session.beginNodeReadOnlyTrx().use { reader ->
                            beforeProjection = ProjectionIndexHOTStorage.readMetadataBlob(reader.storageEngineReader, 0)!!
                            beforeAggregate = TemporalAggregateStorage.Reader(reader.storageEngineReader, 0).use {
                                it.get(TemporalAggregateIndexes.headerKey())!!
                            }
                        }
                        val drop = "jn:drop-projection-index(jn:doc('auth','records')${if (withId) ",0" else ""})"
                        val wrapped = commitArguments != 0
                        val query = when (commitArguments) {
                            0 -> drop
                            1 -> "sdb:commit($drop)"
                            2 -> "sdb:commit($drop,'drop projection')"
                            3 -> "sdb:commit($drop,'drop projection',xs:dateTime('2024-02-15T00:00:00Z'))"
                            else -> error(commitArguments)
                        }
                        if (modify) {
                            run(mode, chain, context, query, keycloak, authz, user)
                            val writer = session.nodeTrx.orElseThrow()
                            val catalogue = session.getWtxIndexController<JsonIndexController>(writer.revisionNumber).indexes
                            assertEquals(0, catalogue.getNrOfIndexDefsWithType(IndexType.PROJECTION))
                            assertEquals(0, catalogue.getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE))
                            assertEquals(if (wrapped) 2 else 1, session.mostRecentRevisionNumber)
                            if (!wrapped) writer.commit()
                            writer.close()
                        } else {
                            val failure = assertThrows(IllegalStateException::class.java) {
                                run(mode, chain, context, query, keycloak, authz, user)
                            }
                            assertTrue(failure.message!!.contains("not allowed to modify"))
                            assertTrue(session.nodeTrx.isEmpty)
                            assertEquals(1, session.mostRecentRevisionNumber)
                        }
                        Mockito.verify(authz).getAuthorizations(user)
                        assertEquals("1", run(mode, chain, context, "sum(jn:doc('auth','records')[].n)", keycloak, authz, user))
                        Mockito.verifyNoMoreInteractions(authz)
                    }
                }
            }
        }
        Databases.clearGlobalCaches()
        Databases.openJsonDatabase(directory.resolve("auth")).use { database ->
            database.beginResourceSession("records").use { session ->
                assertEquals(if (modify) 2 else 1, session.mostRecentRevisionNumber)
                for (revision in setOf(1, session.mostRecentRevisionNumber)) {
                    val catalogue = session.getRtxIndexController<JsonIndexController>(revision).indexes
                    val expected = if (modify && revision == 2) 0 else 1
                    assertEquals(expected, catalogue.getNrOfIndexDefsWithType(IndexType.PROJECTION))
                    assertEquals(expected, catalogue.getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE))
                    session.beginNodeReadOnlyTrx(revision).use { reader ->
                        assertArrayEquals(beforeProjection,
                            ProjectionIndexHOTStorage.readMetadataBlob(reader.storageEngineReader, 0))
                        TemporalAggregateStorage.Reader(reader.storageEngineReader, 0).use {
                            assertArrayEquals(beforeAggregate, it.get(TemporalAggregateIndexes.headerKey()))
                        }
                        reader.moveToDocumentRoot()
                        reader.moveToFirstChild()
                        reader.moveToFirstChild()
                        reader.moveToFirstChild()
                        reader.moveToRightSibling()
                        reader.moveToFirstChild()
                        assertEquals(1, reader.numberValue.toInt())
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
                .storageType(StorageType.FILE_CHANNEL).customCommitTimestamps(true).storeDiffs(false).build())
            database.beginResourceSession("records").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
                        "[{\"g\":1,\"n\":1,\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2025-01-01T00:00:00Z\"}]"), JsonNodeTrx.Commit.NO)
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
                    writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"))
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
