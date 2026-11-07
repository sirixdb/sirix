package io.sirix.rest.crud

import io.brackit.query.Query
import io.brackit.query.atomic.QNm
import io.brackit.query.jdm.Type
import io.brackit.query.node.parser.DocumentParser
import io.brackit.query.util.path.Path as QueryPath
import io.brackit.query.util.path.PathParser
import io.brackit.query.util.serialize.StringSerializer
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.trx.node.json.JsonIndexController
import io.sirix.api.json.JsonNodeTrx
import io.sirix.api.json.JsonResourceSession
import io.sirix.index.IndexDef
import io.sirix.index.IndexDefs
import io.sirix.index.IndexType
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
import io.vertx.core.Future
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.Authorization
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant

/**
 * A query mutates through more than XQuery Update expressions. Index DDL, `sdb:commit` and
 * `sdb:rollback` are ordinary function calls, so `module.body.isUpdating` is false for them and
 * [PermissionCheckingQuery]'s updating-expression check never fires. Each one then reaches a
 * resource session through a looked-up document and begins its own write transaction, which also
 * leaves the authorization-checking collection decorator behind.
 *
 * Every case below therefore submits one such mutation as a VIEW-only user and requires both that
 * it is refused and that nothing moved: the revision count, the index catalogue, the projection
 * index's metadata blob and the temporal aggregate header in the hidden derived resource must all
 * be byte-for-byte what they were. The MODIFY arm proves the same calls still work when allowed, so
 * a refusal cannot be mistaken for the function being broken.
 */
@Tag("offline")
class QueryMutationAuthorizationTest {
    @TempDir
    lateinit var directory: Path

    /**
     * Mutating functions that carry no updating flag. Each is spelled with the arity its
     * `execute` accepts, so a refusal cannot be confused with an arity complaint.
     */
    @ParameterizedTest
    @CsvSource(
        delimiter = '|', quoteCharacter = '"', value = [
            "jn:create-name-index(jn:doc('auth','records'),'g')|NAME|1",
            "jn:create-path-index(jn:doc('auth','records'),'/[]/g')|PATH|1",
            "jn:create-cas-index(jn:doc('auth','records'),'xs:integer','/[]/g')|CAS|1",
            "jn:create-projection-index(jn:doc('auth','records'),'/[]',('/[]/n'))|PROJECTION|1",
            "jn:create-temporal-aggregate-index(jn:doc('auth','records'),'/[]','g','max(n)')|TEMPORAL_AGGREGATE|1",
            "jn:drop-projection-index(jn:doc('auth','records'),0)|PROJECTION|-1",
            "jn:drop-temporal-aggregate-index(jn:doc('auth','records'),0)|TEMPORAL_AGGREGATE|-1"
        ]
    )
    fun viewOnlyUserCannotMutateThroughQueryFunctions(query: String, indexType: IndexType, delta: Int) {
        createDatabase()
        val keycloak = Mockito.mock(OAuth2Auth::class.java)

        withStore { fixture ->
            fixture.store.lookup("auth")!!.database.beginResourceSession("records").use { session ->
                val before = witness(session)
                assertRefused(query) { run(fixture, query, keycloak) }
                assertTrue(session.nodeTrx.isEmpty, "A refused mutation must leave no write transaction open")
                assertWitnessUnchanged(before, witness(session), query)
            }
        }

        Databases.clearGlobalCaches()
        withStore(modify = true) { fixture ->
            fixture.store.lookup("auth")!!.database.beginResourceSession("records").use { session ->
                val before = session.getRtxIndexController<JsonIndexController>(session.mostRecentRevisionNumber).indexes
                val expected = IndexType.entries.associateWith {
                    before.getNrOfIndexDefsWithType(it) + if (it == indexType) delta else 0
                }.toMutableMap()
                if (indexType == IndexType.PROJECTION && delta == -1) {
                    expected[IndexType.TEMPORAL_AGGREGATE] = 0
                }
                run(fixture, query, keycloak)
                session.nodeTrx.orElseThrow().use { writer ->
                    val after = session.getWtxIndexController<JsonIndexController>(writer.revisionNumber).indexes
                    for (type in IndexType.entries) {
                        assertEquals(expected[type], after.getNrOfIndexDefsWithType(type), "$type catalogue: $query")
                    }
                    writer.rollback()
                }
            }
        }
    }

    /**
     * `sdb:commit` and `sdb:rollback` need a write transaction with pending work to be refused for
     * the right reason: without one they fail on the transaction's absence, which would prove
     * nothing about authorization.
     */
    @ParameterizedTest
    @ValueSource(strings = ["sdb:commit(jn:doc('auth','records'))", "sdb:rollback(jn:doc('auth','records'))"])
    fun viewOnlyUserCannotCommitOrRollBackSomeoneElsesWriter(query: String) {
        createDatabase()
        val keycloak = Mockito.mock(OAuth2Auth::class.java)

        withStore { fixture ->
            fixture.store.lookup("auth")!!.database.beginResourceSession("records").use { session ->
                val before = witness(session)
                session.beginNodeTrx().use { writer ->
                    numberField(writer)
                    writer.setNumberValue(4242)

                    assertRefused(query) { run(fixture, query, keycloak) }

                    // Neither published nor discarded: the pending edit is still on the writer.
                    numberField(writer)
                    assertEquals(4242, writer.numberValue.toInt())
                    writer.rollback()
                }
                assertWitnessUnchanged(before, witness(session), query)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "sdb:commit(jn:doc('tenant-b/orders','records'))",
        "sdb:rollback(jn:doc('tenant-b/orders','records'))",
        "jn:create-name-index(jn:doc('tenant-b/orders','records'),'g')"
    ])
    fun basenameModifyRoleCannotMutateNestedDatabase(query: String) {
        createDatabase("orders")
        createDatabase("tenant-b/orders")
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        withStore(modify = true, databaseName = "tenant-b/orders", modifyRole = "orders-modify") { fixture ->
            fixture.store.lookup("tenant-b/orders")!!.database.beginResourceSession("records").use { session ->
                val before = witness(session)
                session.beginNodeTrx().use { writer ->
                    numberField(writer)
                    writer.setNumberValue(4242)
                    assertRefused(query) { run(fixture, query, keycloak) }
                    numberField(writer)
                    assertEquals(4242, writer.numberValue.toInt())
                    writer.rollback()
                }
                assertWitnessUnchanged(before, witness(session), query)
            }
        }
    }

    @Test
    fun nestedDatabaseModifyRoleCanPublishRevision() {
        createDatabase("tenant-b/orders")
        withStore(modify = true, databaseName = "tenant-b/orders", modifyRole = "tenant-b/orders-modify") { fixture ->
            fixture.store.lookup("tenant-b/orders")!!.database.beginResourceSession("records").use { session ->
                val before = session.mostRecentRevisionNumber
                run(fixture, "sdb:commit(jn:doc('tenant-b/orders','records'))", Mockito.mock(OAuth2Auth::class.java))
                assertEquals(before + 1, session.mostRecentRevisionNumber)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "sdb:commit(xn:doc('tenant-b/orders','resource1'))",
        "sdb:rollback(xn:doc('tenant-b/orders','resource1'))",
        "xn:create-name-index(xn:doc('tenant-b/orders','resource1'),())",
        "xn:create-path-index(xn:doc('tenant-b/orders','resource1'),'/root')",
        "xn:create-cas-index(xn:doc('tenant-b/orders','resource1'),'xs:string','/root')"
    ])
    fun basenameModifyRoleCannotMutateNestedXmlDatabase(query: String) {
        val user = userWith("view", "orders-modify")
        val authz = authorizations()
        BasicXmlDBStore.newBuilder().location(directory).build().use { store ->
            store.create("orders", DocumentParser("<root>1</root>"))
            store.create("tenant-b/orders", DocumentParser("<root>1</root>"))
            val guarded = XmlSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithNodeStore(guarded).use { context ->
                SirixCompileChain.createWithNodeStore(guarded).use { chain ->
                    guarded.lookup("tenant-b/orders")!!.database.beginResourceSession("resource1").use { session ->
                        val before = session.mostRecentRevisionNumber
                        session.beginNodeTrx().use { writer ->
                            writer.moveToDocumentRoot()
                            writer.moveToFirstChild()
                            writer.moveToFirstChild()
                            writer.setValue("pending")
                            assertRefused(query) {
                                StringSerializer(PrintWriter(StringWriter())).use {
                                    it.serialize(Query(chain, query).execute(context))
                                }
                            }
                            assertEquals("pending", writer.value)
                            assertEquals(before, session.mostRecentRevisionNumber)
                            writer.rollback()
                        }
                    }
                }
            }
        }
    }

    @Test
    fun restModifyRoleCannotImportServerFile() {
        val source = Files.writeString(directory.resolve("server.xml"), "<root>private</root>")
        val user = userWith("view", "modify")
        val authz = authorizations()
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        BasicXmlDBStore.newBuilder().location(directory).build().use { store ->
            store.create("auth", DocumentParser("<root>1</root>"))
            val guarded = XmlSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithNodeStore(guarded).use { context ->
                SirixCompileChain.createWithNodeStore(guarded).use { chain ->
                    guarded.lookup("auth")!!.database.beginResourceSession("resource1").use { session ->
                        val before = session.mostRecentRevisionNumber
                        val output = StringWriter()
                        val checked = PermissionCheckingQuery(
                            chain, "xn:import('auth','resource1','$source')", keycloak, user, authz
                        )
                        assertThrows(ClassCastException::class.java) {
                            checked.serialize(context, PrintWriter(output))
                        }
                        assertEquals("", output.toString())
                        assertEquals(before, session.mostRecentRevisionNumber)
                        assertTrue(session.nodeTrx.isEmpty)
                        session.beginNodeReadOnlyTrx().use { reader ->
                            reader.moveToFirstChild()
                            reader.moveToFirstChild()
                            assertEquals("1", reader.value)
                        }
                    }
                }
            }
        }
    }

    @Test
    fun nestedXmlDatabaseModifyRoleCanPublishRevision() {
        val user = userWith("view", "tenant-b/orders-modify")
        val authz = authorizations()
        BasicXmlDBStore.newBuilder().location(directory).build().use { store ->
            store.create("tenant-b/orders", DocumentParser("<root>1</root>"))
            val guarded = XmlSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithNodeStore(guarded).use { context ->
                SirixCompileChain.createWithNodeStore(guarded).use { chain ->
                    guarded.lookup("tenant-b/orders")!!.database.beginResourceSession("resource1").use { session ->
                        val before = session.mostRecentRevisionNumber
                        val query = "sdb:commit(xn:doc('tenant-b/orders','resource1'))"
                        StringSerializer(PrintWriter(StringWriter())).use {
                            it.serialize(Query(chain, query).execute(context))
                        }
                        assertEquals(before + 1, session.mostRecentRevisionNumber)
                    }
                }
            }
        }
    }

    /**
     * Both the store decorator and [PermissionCheckingQuery]'s updating-expression check refuse
     * mutations with [HttpException] 403. A query validation error is not an authorization refusal.
     */
    private fun refusalOrNull(throwable: Throwable): String? {
        var cause: Throwable? = throwable
        while (cause != null) {
            if (cause is HttpException && cause.statusCode == 403) return "HttpException 403"
            cause = cause.cause
        }
        return null
    }

    private fun assertRefused(query: String, body: () -> Unit) {
        val thrown = assertThrows(RuntimeException::class.java, body)
        assertNotNull(
            refusalOrNull(thrown),
            "A VIEW-only caller must be refused, but got ${thrown::class.java.name}: ${thrown.message}; query: $query"
        )
    }

    /** Everything a refused mutation could have touched, captured as comparable bytes. */
    private class Witness(
        val revisions: Int,
        val indexCounts: Map<IndexType, Int>,
        val projectionMetadata: ByteArray?,
        val aggregateHeader: ByteArray?,
        val catalogue: String,
        val document: String
    )

    /**
     * The temporal aggregate header is read from the hidden derived resource, which is where
     * aggregate columns are maintained; the source resource holds none. `awaitRevision` makes that
     * read deterministic — maintenance is asynchronous, and an absent snapshot would silently
     * weaken the witness to a null-against-null comparison, so it is required to be present.
     */
    private fun witness(session: JsonResourceSession): Witness {
        val revisions = session.mostRecentRevisionNumber
        val catalogue = session.getRtxIndexController<JsonIndexController>(revisions).indexes
        val counts = IndexType.entries.associateWith { catalogue.getNrOfIndexDefsWithType(it) }
        val header = DerivedAggregateWitness.header(session, revisions)
        session.beginNodeReadOnlyTrx(revisions).use { reader ->
            val projection = ProjectionIndexHOTStorage.readMetadataBlob(reader.storageEngineReader, 0)
            val catalogueOutput = StringWriter()
            StringSerializer(PrintWriter(catalogueOutput)).use { it.serialize(catalogue.materialize()) }
            val document = StringBuilder()
            JsonSerializer.Builder(session, document, revisions).build().call()
            return Witness(revisions, counts, projection, header, catalogueOutput.toString(), document.toString())
        }
    }

    private fun assertWitnessUnchanged(before: Witness, after: Witness, query: String) {
        assertEquals(before.revisions, after.revisions, "A refused mutation published a revision: $query")
        assertEquals(before.indexCounts, after.indexCounts, "A refused mutation changed the catalogue: $query")
        assertEquals(before.catalogue, after.catalogue, "A refused mutation changed an index definition: $query")
        assertEquals(before.document, after.document, "A refused mutation changed the document: $query")
        assertArrayEquals(
            before.projectionMetadata, after.projectionMetadata,
            "A refused mutation changed the projection index metadata: $query"
        )
        assertArrayEquals(
            before.aggregateHeader, after.aggregateHeader,
            "A refused mutation changed the derived temporal aggregate header: $query"
        )
    }

    /**
     * One projection index (id 0) plus one temporal aggregate (id 0) so that every function under
     * test has something to create or drop, and so both the projection metadata blob and the
     * derived aggregate header exist as witnesses.
     */
    private fun createDatabase(databaseName: String = "auth") {
        val path = directory.resolve(databaseName)
        Databases.createJsonDatabase(DatabaseConfiguration(path))
        Databases.openJsonDatabase(path).use { database ->
            database.createResource(
                ResourceConfiguration.newBuilder("records").validTimePaths("lo", "hi")
                    .storageType(StorageType.FILE_CHANNEL).customCommitTimestamps(true).storeDiffs(false).build()
            )
            database.beginResourceSession("records").use { session ->
                session.beginNodeTrx().use { writer ->
                    writer.insertSubtreeAsFirstChild(
                        JsonShredder.createStringReader(
                            "[{\"g\":1,\"n\":1,\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2025-01-01T00:00:00Z\"}]"
                        ),
                        JsonNodeTrx.Commit.NO
                    )
                    val fields = listOf("g", "n", "lo", "hi")
                    val paths: List<QueryPath<QNm>> = fields.map { QueryPath.parse("/[]/$it", PathParser.Type.JSON) }
                    val projection = IndexDefs.createProjectionIdxDef(
                        QueryPath.parse("/[]", PathParser.Type.JSON), paths,
                        listOf(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON
                    )
                    val controller: JsonIndexController = session.getWtxIndexController(writer.revisionNumber)
                    controller.createIndexes(setOf(projection), writer)
                    TemporalAggregateDeclarations.declare(controller, writer, "/[]", listOf("g"), listOf("sum(n)"))
                    writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"))
                }
            }
        }
    }

    private fun authorizations(): AuthorizationProvider {
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        Mockito.`when`(authz.getAuthorizations(Mockito.any())).thenReturn(Future.succeededFuture())
        return authz
    }

    /**
     * The query context and compile chain outlive every query a case runs: closing them also closes
     * the resource sessions they registered, which would close the session the assertions read.
     */
    /**
     * Writing into a resource that already exists is a modification, so CREATE alone must not reach
     * it. Both functions previously ended at the store's and collection's CREATE-checked entry
     * points only, which let a caller holding CREATE but not MODIFY overwrite existing data — the
     * create-new flag does not decide this, because the create path shreds into a resource that is
     * already there instead of refusing it.
     */
    @ParameterizedTest
    @CsvSource("store,true", "store,false", "load,true", "load,false")
    fun createRoleAloneCannotOverwriteAnExistingResource(function: String, createNew: Boolean) {
        createDatabase()
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        val query = storeOrLoad(function, "records", createNew)
        withRoles("auth-view", "create") { fixture ->
            fixture.store.lookup("auth")!!.database.beginResourceSession("records").use { session ->
                val before = witness(session)
                assertRefused(query) { run(fixture, query, keycloak) }
                assertTrue(session.nodeTrx.isEmpty, "A refused store/load must leave no write transaction open")
                assertWitnessUnchanged(before, witness(session), query)
            }
        }
    }

    @ParameterizedTest
    @CsvSource(
        "store,named", "store,existing", "store,generated", "store,multiple",
        "load,named", "load,existing", "load,generated", "load,multiple"
    )
    fun createRoleCannotReplaceAnExistingDatabase(function: String, target: String) {
        createDatabase()
        val before = databaseWitness()
        val query = replacementQuery(function, target)
        withRoles("auth-view", "create") { fixture ->
            assertRefused(query) { run(fixture, query, Mockito.mock(OAuth2Auth::class.java)) }
        }
        val after = databaseWitness()
        assertEquals(before.first, after.first, "A refused replacement changed the database resources")
        assertWitnessUnchanged(before.second, after.second, query)
    }

    @ParameterizedTest
    @CsvSource(
        "store,named", "store,existing", "store,generated", "store,multiple",
        "load,named", "load,existing", "load,generated", "load,multiple"
    )
    fun createAndDeleteRolesCanReplaceAnExistingDatabase(function: String, target: String) {
        createDatabase()
        withRoles("auth-view", "create", "delete") { fixture ->
            run(fixture, replacementQuery(function, target), Mockito.mock(OAuth2Auth::class.java))
        }
        Databases.clearGlobalCaches()
        Databases.openJsonDatabase(directory.resolve("auth")).use { database ->
            val expected = when (target) {
                "named" -> setOf("fresh")
                "existing" -> setOf("records")
                "generated" -> setOf("resource")
                "multiple" -> setOf("resource1", "resource2")
                else -> error(target)
            }
            assertEquals(expected, database.listResources().map { it.fileName.toString() }.toSet())
        }
    }

    private fun replacementQuery(function: String, target: String): String {
        val resource = when (target) {
            "generated" -> "()"
            "existing" -> "'records'"
            else -> "'fresh'"
        }
        val input = when (function) {
            "store" -> "'{}'"
            "load" -> {
                val source = Files.writeString(directory.resolve("replacement.json"), "{}")
                "'${source.toUri()}'"
            }
            else -> error(function)
        }
        val inputs = if (target == "multiple") "($input,$input)" else input
        return "jn:$function('auth',$resource,$inputs)"
    }

    private fun databaseWitness(): Pair<Set<String>, Witness> {
        Databases.clearGlobalCaches()
        Databases.openJsonDatabase(directory.resolve("auth")).use { database ->
            database.beginResourceSession("records").use { session ->
                val state = witness(session)
                assertNotNull(state.projectionMetadata, "The fixture must contain projection metadata")
                return database.listResources().map { it.fileName.toString() }.toSet() to state
            }
        }
    }

    /** Creating a resource needs CREATE, so MODIFY alone must not bring one into existence. */
    @ParameterizedTest
    @CsvSource("store,true", "store,false", "load,true", "load,false")
    fun modifyRoleAloneCannotCreateAResource(function: String, createNew: Boolean) {
        createDatabase()
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        val query = storeOrLoad(function, "fresh", createNew)
        withRoles("auth-view", "modify") { fixture ->
            assertRefused(query) { run(fixture, query, keycloak) }
        }
        assertFalse(resourceExists("fresh"), "A refused store/load must not create the resource: $query")
    }

    /**
     * The allowed arms, so a refusal above cannot be mistaken for a broken function: the same calls
     * write when the caller holds the permission the operation needs.
     */
    @ParameterizedTest
    @CsvSource("store,true", "store,false", "load,true", "load,false")
    fun authorizedStoreAndLoadStillWrite(function: String, createNew: Boolean) {
        createDatabase()
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        withRoles("auth-view", "create", "modify", "delete") { fixture ->
            run(fixture, storeOrLoad(function, "fresh", createNew), keycloak)
        }
        assertTrue(resourceExists("fresh"), "An authorized store/load must create the resource")
    }

    /** `jn:store` takes its fragment inline; `jn:load` reads it from a URI the test writes. */
    private fun storeOrLoad(function: String, resourceName: String, createNew: Boolean): String {
        val flag = if (createNew) ",true()" else ",false()"
        return when (function) {
            "store" -> "jn:store('auth','$resourceName','[{\"n\":1}]'$flag)"
            "load" -> {
                val source = directory.resolve("$resourceName-source.json")
                if (!Files.exists(source)) Files.writeString(source, "[{\"n\":1}]")
                "jn:load('auth','$resourceName','${source.toUri()}'$flag)"
            }
            else -> error(function)
        }
    }

    private fun resourceExists(resourceName: String): Boolean {
        Databases.clearGlobalCaches()
        Databases.openJsonDatabase(directory.resolve("auth")).use { database ->
            return database.existsResource(resourceName)
        }
    }

    /** A fixture whose caller holds exactly [roles], for asserting one permission at a time. */
    private fun withRoles(vararg roles: String, body: (Fixture) -> Unit) {
        val user = userWith(*roles)
        val authz = authorizations()
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { store ->
            val guarded = JsonSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithJsonStore(guarded).use { context ->
                SirixCompileChain.createWithJsonStore(guarded).use { chain ->
                    body(Fixture(guarded, user, authz, context, chain))
                }
            }
        }
    }

    private fun withStore(
        modify: Boolean = false,
        databaseName: String = "auth",
        modifyRole: String = "modify",
        body: (Fixture) -> Unit
    ) {
        val roles = if (modify) arrayOf("$databaseName-view", modifyRole) else arrayOf("$databaseName-view")
        val user = userWith(*roles)
        val authz = authorizations()
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { store ->
            val guarded = JsonSessionDBStore(Mockito.mock(RoutingContext::class.java), store, user, authz)
            SirixQueryContext.createWithJsonStore(guarded).use { context ->
                SirixCompileChain.createWithJsonStore(guarded).use { chain ->
                    body(Fixture(guarded, user, authz, context, chain))
                }
            }
        }
    }

    private class Fixture(
        val store: JsonSessionDBStore,
        val user: User,
        val authz: AuthorizationProvider,
        val context: SirixQueryContext,
        val chain: SirixCompileChain
    )

    private fun userWith(vararg roles: String): User {
        val user = User.fromName("test-user")
        val authorizations: Set<Authorization> = roles.map { RoleBasedAuthorization.create(it) }.toSet()
        user.authorizations().put("test-provider", authorizations)
        return user
    }

    private fun numberField(writer: JsonNodeTrx) {
        writer.moveToDocumentRoot()
        writer.moveToFirstChild()
        writer.moveToFirstChild()
        writer.moveToFirstChild()
        writer.moveToRightSibling()
        writer.moveToFirstChild()
    }

    private fun run(fixture: Fixture, query: String, keycloak: OAuth2Auth): String {
        val output = StringWriter()
        val checked = PermissionCheckingQuery(fixture.chain, query, keycloak, fixture.user, fixture.authz)
        checked.serialize(fixture.context, PrintWriter(output))
        return output.toString().trim()
    }
}
