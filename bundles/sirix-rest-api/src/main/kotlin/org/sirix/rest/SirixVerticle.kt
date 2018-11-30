package org.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.net.SelfSignedCertificate
import io.vertx.ext.auth.shiro.ShiroAuth
import io.vertx.ext.auth.shiro.ShiroAuthRealmType
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.*
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.ext.web.sstore.LocalSessionStore
import io.vertx.kotlin.core.http.HttpServerOptions
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.auth.shiro.ShiroAuthOptions
import kotlinx.coroutines.launch
import org.sirix.rest.crud.Create
import org.sirix.rest.crud.Delete
import org.sirix.rest.crud.Get
import org.sirix.rest.crud.Update
import java.nio.file.Paths


class SirixVerticle : CoroutineVerticle() {

    /** User home directory. */
    private val userHome = System.getProperty("user.home")

    /** Storage for databases: Sirix data in home directory. */
    private val location = Paths.get(userHome, "sirix-data")

    override suspend fun start() {
        val router = createRouter()

        // Generate the certificate for https
        val cert = SelfSignedCertificate.create()

        // Start an HTTP/2 server
        val server = vertx.createHttpServer(HttpServerOptions()
                .setSsl(true)
                .setUseAlpn(true)
                .setKeyCertOptions(cert.keyCertOptions()))

        server.requestHandler { router.handle(it) }
                .listenAwait(config.getInteger("https.port", 8443))
    }

    private fun createRouter() = Router.router(vertx).apply {
        route().handler(CookieHandler.create())
        route().handler(BodyHandler.create())
        route().handler(SessionHandler.create(LocalSessionStore.create(vertx)))

        val config = json {
            obj("properties_path" to "classpath:test-auth.properties")
        }

        val authProvider = ShiroAuth.create(vertx, ShiroAuthOptions().setType(ShiroAuthRealmType.PROPERTIES).setConfig(config))

        route().handler(UserSessionHandler.create(authProvider))
        route().handler(BasicAuthHandler.create(authProvider))

           // Create.
        put("/:database").coroutineHandler { Create(location).handle(it) }
        put("/:database/:resource").coroutineHandler { Create(location).handle(it) }

        // Update.
        post("/:database/:resource").coroutineHandler { Update(location).handle(it) }

        // Get.
        get("/:database/:resource").coroutineHandler { Get(location).handle(it) }
        get("/:database").coroutineHandler { Get(location).handle(it) }

        // Delete.
        delete("/:database/:resource").coroutineHandler { Delete(location).handle(it) }
        delete("/:database").coroutineHandler { Delete(location).handle(it) }

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
                response(failureRoutingContext.response(), statusCode, failure.message)
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