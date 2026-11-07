package io.sirix.rest

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpVersion
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authentication.Credentials
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito

@Tag("offline")
class AuthBodyHandlerTest {
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun consumedPostBodyStillAuthenticatesAndChecksPermissions(allowed: Boolean) = runBlocking {
        val vertx = Vertx.vertx()
        try {
            val user = User.fromName("post-query-user")
            if (allowed) {
                user.authorizations().put("test-provider", setOf(RoleBasedAuthorization.create("view")))
            }
            // Only the identity-provider boundary is mocked; the HTTP request, BodyHandler and
            // production Auth handler must execute, including permission refusal after body read.
            val keycloak = Mockito.mock(OAuth2Auth::class.java)
            Mockito.`when`(keycloak.authenticate(Mockito.any(Credentials::class.java)))
                .thenReturn(Future.succeededFuture(user))
            val authz = Mockito.mock(AuthorizationProvider::class.java)
            Mockito.`when`(authz.getAuthorizations(user)).thenReturn(Future.succeededFuture())
            val router = Router.router(vertx)
            val scope = CoroutineScope(vertx.dispatcher())
            router.post("/").handler(BodyHandler.create()).coroutineHandler(scope) { ctx ->
                Auth(keycloak, authz, AuthRole.VIEW).handle(ctx)
                if (!ctx.failed()) ctx.response().end(ctx.body().asJsonObject().getString("query"))
            }
            router.route().failureHandler(routerFailureHandler())
            val server = vertx.createHttpServer().requestHandler(router).listen(0, "127.0.0.1").coAwait()
            val client = WebClient.create(
                vertx, WebClientOptions().setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false)
            )
            try {
                val response = client.post(server.actualPort(), "127.0.0.1", "/")
                    .putHeader("Authorization", "Bearer synthetic-test-token")
                    .sendJsonObject(JsonObject().put("query", "1 + 1")).coAwait()
                assertEquals(HttpVersion.HTTP_2, response.version())
                assertEquals(if (allowed) 200 else 403, response.statusCode())
                if (allowed) assertEquals("1 + 1", response.bodyAsString())
            } finally {
                client.close()
                server.close().coAwait()
            }
        } finally {
            vertx.close().coAwait()
        }
    }
}
