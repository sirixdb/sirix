package org.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.net.PemKeyCertOptions
import io.vertx.ext.auth.oauth2.OAuth2ClientOptions
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.*
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.ext.web.sstore.LocalSessionStore
import io.vertx.kotlin.core.http.HttpServerOptions
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.auth.oauth2.providers.KeycloakAuth
import kotlinx.coroutines.launch
import org.sirix.rest.crud.Create
import org.sirix.rest.crud.Delete
import org.sirix.rest.crud.Get
import org.sirix.rest.crud.Update
import java.nio.file.Paths

class SirixVerticle: CoroutineVerticle() {
    /** User home directory. */
    private val userHome = System.getProperty("user.home")

    /** Storage for databases: Sirix data in home directory. */
    private val location = Paths.get(userHome, "sirix-data")

    override suspend fun start() {
        val router = createRouter()

        // Start an HTTP/2 server
        val server = vertx.createHttpServer(HttpServerOptions()
                .setSsl(true)
                .setUseAlpn(true)
                .setPemKeyCertOptions(PemKeyCertOptions().setKeyPath(location.resolve("key.pem").toString()).setCertPath(location.resolve("cert.pem").toString())))

        server.requestHandler { router.handle(it) }
                .listenAwait(config.getInteger("https.port", 9443))
    }

    private suspend fun createRouter() = Router.router(vertx).apply {
        route().handler(CookieHandler.create())
        route().handler(BodyHandler.create())
        route().handler(SessionHandler.create(LocalSessionStore.create(vertx)))

        val oauth2 = KeycloakAuth.discoverAwait(
            vertx,
            OAuth2ClientOptions()
                    //.setPubSecKeys(listOf(PubSecKeyOptions().setPublicKey("MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAq94aUofW9yTCvdSZ4GiKm73OZpj5YFUTYokkyqNIzUCHQ6jQehurNmVK92Td/uD+97KdqFrGHyw/0u/QULwzFLQ91tqRioYMYAzx+2Ey2sC40yIBvUCwc+5mDXo2g7RXdT2dNTMOWdY2gXheu/Hg6XVdpu/MnU9CLVEiCtWlq9t1VFczFW2rwmX5P2FpZZWilWPVLrXqBRlClZnDrnOvBkjfqu0R8S/JWEOxlC8SjRzXM6/kag+YQ8lNhJiUp4d48c4ZPsdK9fCn3hqZFWgaWHWwQDdO+X4xMv6SL936goRxy8y9EVlVk3tUvRMKi4GDzjhTjhUje8Nuda1p5+uZKwIDAQAB")))
                    .setSite("http://localhost:8080/auth/realms/master")
                    .setClientID("sirix")
                    .setClientSecret("c8b9b4ed-67bb-47d9-bd73-a3babc470b2c"))

        route().handler(UserSessionHandler.create(oauth2))

        val oauth2Handler = OAuth2AuthHandler.create(oauth2)

        oauth2Handler.setupCallback(get("/callback"))

        // Create.
        put("/:database").handler(oauth2Handler).coroutineHandler { Create(location).handle(it) }
        put("/:database/:resource").handler(oauth2Handler).coroutineHandler { Create(location).handle(it) }

        // Update.
        post("/:database/:resource").handler(oauth2Handler).coroutineHandler { Update(location).handle(it) }

        // Get.
        get("/:database/:resource").handler(oauth2Handler).coroutineHandler { Get(location).handle(it) }
        get("/:database").handler(oauth2Handler).coroutineHandler { Get(location).handle(it) }

        // Delete.
        delete("/:database/:resource").handler(oauth2Handler).coroutineHandler { Delete(location).handle(it) }
        delete("/:database").handler(oauth2Handler).coroutineHandler { Delete(location).handle(it) }

        // Exception with status code
        route().handler { ctx ->
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code()))
        }

        route().failureHandler { failureRoutingContext ->
            val statusCode = failureRoutingContext.statusCode()
            val failure = failureRoutingContext.failure()

            if (statusCode == -1) {
                if (failure is HttpStatusException)
                    response(failureRoutingContext.response(), failure.statusCode, failure.message)
                else
                    response(failureRoutingContext.response(), HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), failure.message)
            } else {
                response(failureRoutingContext.response(), statusCode, failure?.message)
            }
        }
    }

    private fun response(response: HttpServerResponse, statusCode: Int, failureMessage: String?) {
        response.setStatusCode(statusCode).end("Failure calling the RESTful API: $failureMessage")
    }

    /**
     * An extension method for simplifying coroutines usage with Vert.x Web routers.
     */
    private fun Route.coroutineHandler(fn: suspend (RoutingContext) -> Unit) {
        handler { ctx ->
            launch(ctx.vertx().dispatcher()) {
                try {
                    fn(ctx)
                } catch (e: Exception) {
                    ctx.fail(e)
                }
            }
        }
    }
}