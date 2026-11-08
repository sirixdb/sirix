package io.sirix.rest.crud

import io.sirix.io.StorageType
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.rest.coroutineHandler
import io.sirix.rest.routerFailureHandler
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import org.mockito.Mockito
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/** Exercises the production query route and error mapping without Keycloak or Docker. */
@Tag("offline")
class QueryJsonSerializationTest {
    @TempDir
    lateinit var directory: Path

    @Test
    @Timeout(30)
    fun modifyUserGetsClientErrorForXmlIndexDefinitionInJsonResponse() = runBlocking {
        BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build().use { store ->
            store.create("auth", "records", "[{\"g\":1}]")
        }
        val user = User.fromName("modify-user")
        user.authorizations().put("test-provider", setOf(
            RoleBasedAuthorization.create("auth-view"), RoleBasedAuthorization.create("modify")
        ))
        val authz = Mockito.mock(AuthorizationProvider::class.java)
        Mockito.`when`(authz.getAuthorizations(Mockito.any())).thenReturn(Future.succeededFuture())
        val keycloak = Mockito.mock(OAuth2Auth::class.java)
        val vertx = Vertx.vertx()
        val client = WebClient.create(vertx)
        try {
            val router = Router.router(vertx)
            val handler = GetHandler(directory, keycloak, authz)
            router.get("/").handler { it.put("user", user).next() }
                .coroutineHandler(CoroutineScope(vertx.dispatcher())) { handler.handle(it) }
            router.route().failureHandler(routerFailureHandler())
            val server = vertx.createHttpServer().requestHandler(router).listen(0, "127.0.0.1").coAwait()
            val response = client.get(server.actualPort(), "127.0.0.1", "/")
                .putHeader("Accept", "application/json")
                .addQueryParam("query", "jn:create-name-index(jn:doc('auth','records'),'g')")
                // A non-advancing serializer must fail this test instead of hanging the suite.
                .timeout(5000).send().coAwait()
            assertEquals(400, response.statusCode())
            assertEquals("application/json", response.getHeader("Content-Type"))
            val error = requireNotNull(response.bodyAsJsonObject())
            assertEquals(400, error.getInteger("statusCode"))
            assertTrue(requireNotNull(error.getString("message")).contains("Cannot serialize"))

            val control = client.get(server.actualPort(), "127.0.0.1", "/")
                .putHeader("Accept", "application/xml")
                .addQueryParam("query", "jn:create-path-index(jn:doc('auth','records'),'/[]/g')")
                .timeout(5000).send().coAwait()
            assertEquals(200, control.statusCode())
            assertTrue(requireNotNull(control.bodyAsString()).contains("index"))
        } finally {
            client.close()
            // Bound cleanup too: the original spinning worker ignored server shutdown.
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)
        }
    }
}
