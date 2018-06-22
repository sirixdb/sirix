package org.sirix.rest

import io.vertx.ext.auth.shiro.ShiroAuth

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.core.net.SelfSignedCertificate
import io.vertx.ext.auth.shiro.ShiroAuthRealmType
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.*
import io.vertx.ext.web.sstore.LocalSessionStore
import io.vertx.kotlin.core.http.HttpServerOptions
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.ext.auth.shiro.ShiroAuthOptions

import org.sirix.rest.crud.Create
import org.sirix.rest.crud.Delete
import org.sirix.rest.crud.Get
import org.sirix.rest.crud.Update

import java.nio.file.Paths

@Suppress("unused")
class SirixVerticle : AbstractVerticle() {

    /** User home directory. */
    private val userHome = System.getProperty("user.home")

    /** Storage for databases: Sirix data in home directory. */
    private val location = Paths.get(userHome, "sirix-data")

    override fun start(startFuture: Future<Void>) {
        val router = createRouter()

        // Generate the certificate for https
        val cert = SelfSignedCertificate.create()

        // Start an HTTP/2 server
        val server = vertx.createHttpServer(HttpServerOptions()
                .setSsl(true)
                .setUseAlpn(true)
                .setKeyCertOptions(cert.keyCertOptions()))

        server.requestHandler { router.accept(it) }
                .listen(config().getInteger("https.port", 8443)) { result ->
                    if (result.succeeded()) {
                        startFuture.complete()
                    } else {
                        startFuture.fail(result.cause())
                    }
                }
    }

    private fun createRouter() = Router.router(vertx).apply {
        route().handler(CookieHandler.create())
        route().handler(BodyHandler.create())
        route().handler(SessionHandler.create(LocalSessionStore.create(vertx)))

        var config = json {
            obj("properties_path" to "classpath:test-auth.properties")
        }

        var authProvider = ShiroAuth.create(vertx, ShiroAuthOptions().setType(ShiroAuthRealmType.PROPERTIES).setConfig(config))

        route().handler(UserSessionHandler.create(authProvider))
        route().pathRegex("/.*").handler(BasicAuthHandler.create(authProvider));

        // Create.
        post("/:database/:resource").blockingHandler(Create(location))
        post("/:database").blockingHandler(Create(location))

        // Update.
        put("/:database/:resource").handler(Update(location))

        // Get.
        get("/:database/:resource").handler(Get(location))
        get("/:database").handler(Get(location))

        // Delete.
        delete("/:database/:resource").handler(Delete(location))
        delete("/:database").handler(Delete(location))

        route().failureHandler({ failureRoutingContext ->
            val statusCode = failureRoutingContext.statusCode()

            if (statusCode == -1) {
                response(failureRoutingContext, 510)
            } else {
                response(failureRoutingContext, statusCode)
            }
        })
    }

    private fun response(failureRoutingContext: RoutingContext, statusCode: Int) {
        val failure = failureRoutingContext.failure()
        val message = failure.localizedMessage
        val response = failureRoutingContext.response()
        response.setStatusCode(statusCode).end("Failure calling the RESTful API: ${message}")
    }
}