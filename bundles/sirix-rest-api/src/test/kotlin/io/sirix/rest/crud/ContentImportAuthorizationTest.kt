package io.sirix.rest.crud

import com.sun.net.httpserver.HttpServer
import io.brackit.query.node.parser.DocumentParser
import io.sirix.io.StorageType
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.rest.crud.xml.XmlSessionDBStore
import io.sirix.rest.routerFailureHandler
import io.vertx.core.Future
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.mockito.Mockito
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

@Tag("offline")
class ContentImportAuthorizationTest {
    @TempDir
    lateinit var directory: Path

    @ParameterizedTest
    @ValueSource(strings = ["jn:import", "jn:load", "xn:import", "xn:load"])
    fun serverFilesAndUrlsAreRejectedBeforeAnyMutation(function: String) {
        val json = function.startsWith("jn:")
        val secret = if (json) "{\"secret\":true}" else "<secret>private</secret>"
        val file = Files.writeString(directory.resolve("server-secret"), secret)
        val requests = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/secret") { exchange ->
            requests.incrementAndGet()
            val bytes = secret.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        server.start()
        try {
            withStore(json, arrayOf("view", "create", "modify", "delete")) { fixture ->
                val sources = listOf(file.toString(), file.toUri().toString(), "http://127.0.0.1:${server.address.port}/secret")
                for (source in sources) {
                    val query = call(function, "doc", source)
                    assertStatus(400) { fixture.run(query) }
                    assertEquals(1, fixture.revision())
                    assertEquals(setOf("doc"), fixture.resources())
                }
                assertEquals(0, requests.get(), "No server-side HTTP request may be attempted")
            }
        } finally {
            server.stop(0)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["jn:load", "xn:load"])
    fun invalidLaterInputCannotPartiallyLoadAContentSequence(function: String) {
        val json = function.startsWith("jn:")
        val first = if (json) "{}" else "<root/>"
        val source = directory.resolve("server-source").toUri().toString()
        withStore(json, arrayOf("view", "create", "modify", "delete")) { fixture ->
            assertStatus(400) { fixture.run("$function('auth',(),('$first','$source'),false())") }
            assertEquals(setOf("doc"), fixture.resources())
            assertEquals(1, fixture.revision())
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["xn:import", "xn:load"])
    fun xmlContentCannotResolveExternalEntities(function: String) {
        val secret = Files.writeString(directory.resolve("entity-secret"), "private")
        val xml = "<!DOCTYPE root [<!ENTITY secret SYSTEM \"${secret.toUri()}\">]><root>&secret;</root>"
        withStore(false, arrayOf("view", "create", "modify", "delete")) { fixture ->
            assertStatus(400) { fixture.run(call(function, "doc", xml)) }
            assertEquals(1, fixture.revision())
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["jn:import", "xn:import"])
    fun importRequiresModifyAndAnExistingResource(function: String) {
        val json = function.startsWith("jn:")
        val content = if (json) "{\"stable\":1,\"value\":2}" else "<root><stable>1</stable><value>2</value></root>"
        withStore(json, arrayOf("auth-view")) { fixture ->
            assertStatus(403) { fixture.run(call(function, "doc", content)) }
            assertEquals(1, fixture.revision())
            assertFalse(fixture.hasWriter())
        }
        withStore(json, arrayOf("auth-view", "auth-modify"), initialize = false) { fixture ->
            fixture.run(call(function, "doc", content))
            assertEquals(2, fixture.revision())
            assertStatus(400) { fixture.run(call(function, "fresh", content)) }
            assertEquals(setOf("doc"), fixture.resources())
        }
        withStore(json, arrayOf("auth-view", "auth-create"), initialize = false) { fixture ->
            assertStatus(403) { fixture.run(call(function, "doc", content)) }
            assertStatus(400) { fixture.run(call(function, "fresh", content)) }
            assertEquals(setOf("doc"), fixture.resources())
            assertFalse(fixture.hasWriter())
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["jn:load", "xn:load"])
    fun loadsAcceptInlineContentAndRequireCreate(function: String) {
        val json = function.startsWith("jn:")
        val content = if (json) "{\"value\":2}" else "<root>2</root>"
        withStore(json, arrayOf("view")) { fixture ->
            assertStatus(403) { fixture.run(call(function, "fresh", content)) }
            assertFalse(fixture.resources().contains("fresh"))
        }
        withStore(json, arrayOf("view", "create"), initialize = false) { fixture ->
            fixture.run(call(function, "fresh", content))
            assertTrue(fixture.resources().contains("fresh"))
            assertEquals(1, fixture.revision("fresh"))
        }
    }

    @Test
    fun xmlLoadIntoExistingResourceRequiresModify() {
        val query = "xn:load('auth','doc','<added/>',false())"
        withStore(false, arrayOf("view", "create")) { fixture ->
            assertStatus(403) { fixture.run(query) }
            assertEquals(1, fixture.revision())
            assertFalse(fixture.hasWriter())
        }
        withStore(false, arrayOf("view", "create", "modify"), initialize = false) { fixture ->
            fixture.run(query)
            assertEquals(2, fixture.revision())
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["jn:load", "xn:load"])
    fun replacingDatabaseRequiresDelete(function: String) {
        val json = function.startsWith("jn:")
        val content = if (json) "{}" else "<root/>"
        withStore(json, arrayOf("view", "create", "modify")) { fixture ->
            assertStatus(403) { fixture.run("$function('auth','fresh','$content',true())") }
            assertEquals(setOf("doc"), fixture.resources())
            assertEquals(1, fixture.revision())
        }
    }

    private fun call(function: String, resource: String, content: String): String {
        val flag = if (function.endsWith(":load")) ",false()" else ""
        return "$function('auth','$resource','${content.replace("'", "''")}'$flag)"
    }

    private class Fixture(
        val context: SirixQueryContext,
        val chain: SirixCompileChain,
        val user: User,
        val authz: AuthorizationProvider,
        val revision: (String) -> Int,
        val resources: () -> Set<String>,
        val writer: () -> Boolean
    ) {
        fun run(query: String) = PermissionCheckingQuery(chain, query, Mockito.mock(OAuth2Auth::class.java), user, authz).evaluate(context)
        fun revision(resource: String = "doc") = revision.invoke(resource)
        fun hasWriter() = writer()
    }

    private fun withStore(json: Boolean, roles: Array<String>, initialize: Boolean = true, body: (Fixture) -> Unit) {
        val user = User.fromName("import-test")
        user.authorizations().put("test", roles.map { RoleBasedAuthorization.create(it) }.toSet())
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        Mockito.`when`(authz.getAuthorizations(Mockito.any())).thenReturn(Future.succeededFuture())
        val routing = Mockito.mock(RoutingContext::class.java)
        if (json) {
            BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { store ->
                if (initialize) store.create("auth", "doc", "{\"stable\":1,\"value\":1}")
                val guarded = JsonSessionDBStore(routing, store, user, authz)
                SirixQueryContext.createWithJsonStore(guarded).use { context ->
                    SirixCompileChain.createWithJsonStore(guarded).use { chain ->
                        val collection = store.lookup("auth")!!
                        body(Fixture(context, chain, user, authz,
                            { name -> collection.database.beginResourceSession(name).mostRecentRevisionNumber },
                            { collection.database.listResources().map { it.fileName.toString() }.toSet() },
                            { collection.database.beginResourceSession("doc").nodeTrx.isPresent }))
                    }
                }
            }
        } else {
            BasicXmlDBStore.newBuilder().location(directory).build().use { store ->
                if (initialize) store.create("auth", "doc", DocumentParser("<root><stable>1</stable><value>1</value></root>"))
                val guarded = XmlSessionDBStore(routing, store, user, authz)
                SirixQueryContext.createWithNodeStore(guarded).use { context ->
                    SirixCompileChain.createWithNodeStore(guarded).use { chain ->
                        val collection = store.lookup("auth")!!
                        body(Fixture(context, chain, user, authz,
                            { name -> collection.database.beginResourceSession(name).mostRecentRevisionNumber },
                            { collection.database.listResources().map { it.fileName.toString() }.toSet() },
                            { collection.database.beginResourceSession("doc").nodeTrx.isPresent }))
                    }
                }
            }
        }
    }

    private fun assertStatus(status: Int, action: () -> Unit) {
        val failure = assertThrows(RuntimeException::class.java, action)
        val routing = Mockito.mock(RoutingContext::class.java)
        val response = Mockito.mock(HttpServerResponse::class.java, Mockito.RETURNS_SELF)
        Mockito.`when`(routing.statusCode()).thenReturn(500)
        Mockito.`when`(routing.failure()).thenReturn(failure)
        Mockito.`when`(routing.request()).thenReturn(Mockito.mock(HttpServerRequest::class.java))
        Mockito.`when`(routing.response()).thenReturn(response)
        routerFailureHandler().handle(routing)
        Mockito.verify(response).setStatusCode(status)
    }
}
