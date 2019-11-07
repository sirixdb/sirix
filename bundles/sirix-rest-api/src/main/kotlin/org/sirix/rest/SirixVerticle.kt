package org.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.core.net.PemKeyCertOptions
import io.vertx.ext.auth.oauth2.OAuth2FlowType
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.kotlin.core.http.httpServerOptionsOf
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.auth.authenticateAwait
import io.vertx.kotlin.ext.auth.oauth2.oAuth2ClientOptionsOf
import io.vertx.kotlin.ext.auth.oauth2.providers.KeycloakAuth
import kotlinx.coroutines.launch
import org.apache.http.HttpStatus
import org.sirix.rest.crud.CreateMultipleResources
import org.sirix.rest.crud.Delete
import org.sirix.rest.crud.json.*
import org.sirix.rest.crud.xml.*
import java.nio.file.Paths
import java.util.*

class SirixVerticle : CoroutineVerticle() {
    /** User home directory. */
    private val userHome = System.getProperty("user.home")

    /** Storage for databases: Sirix data in home directory. */
    private val location = Paths.get(userHome, "sirix-data")

    override suspend fun start() {
        val router = createRouter()

        // Start an HTTP/2 server
        val server = vertx.createHttpServer(
            httpServerOptionsOf()
                .setSsl(true)
                .setUseAlpn(true)
                .setPemKeyCertOptions(
                    PemKeyCertOptions().setKeyPath(location.resolve("key.pem").toString())
                        .setCertPath(
                            location.resolve("cert.pem").toString()
                        )
                )
        )

        server.requestHandler { router.handle(it) }
            .listenAwait(config.getInteger("https.port", 9443))
    }

    private suspend fun createRouter() = Router.router(vertx).apply {

        val oauth2Config = oAuth2ClientOptionsOf()
            .setFlow(OAuth2FlowType.valueOf(config.getString("oAuthFlowType", "PASSWORD")))
            .setSite(config.getString("keycloak.url"))
            .setClientID("sirix")
            .setClientSecret(config.getString("client.secret"))
            .setTokenPath(config.getString("token.path", "/token"))
            .setAuthorizationPath(config.getString("auth.path", "/user/authorize"))

        val keycloak = KeycloakAuth.discoverAwait(
            vertx, oauth2Config
        )

        if (oauth2Config.flow == OAuth2FlowType.AUTH_CODE) {
            val allowedHeaders = HashSet<String>()
            allowedHeaders.add("x-requested-with")
            allowedHeaders.add("Access-Control-Allow-Origin")
            allowedHeaders.add("origin")
            allowedHeaders.add("Content-Type")
            allowedHeaders.add("accept")
            allowedHeaders.add("X-PINGARUNER")

            val allowedMethods = HashSet<HttpMethod>()
            allowedMethods.add(HttpMethod.GET)
            allowedMethods.add(HttpMethod.POST)
            allowedMethods.add(HttpMethod.OPTIONS)

            allowedMethods.add(HttpMethod.DELETE)
            allowedMethods.add(HttpMethod.PATCH)
            allowedMethods.add(HttpMethod.PUT)

            this.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods))
        }

        get("/user/authorize").coroutineHandler { rc ->
            if (oauth2Config.flow != OAuth2FlowType.AUTH_CODE) {
                rc.response().statusCode = HttpStatus.SC_BAD_REQUEST
            } else {
                val authorizationUri = keycloak.authorizeURL(
                    JsonObject()
                        .put("redirect_uri", config.getString("redirect.uri"))
                        .put("state", java.util.UUID.randomUUID().toString())
                )
                rc.response().putHeader("Location", authorizationUri)
                    .setStatusCode(HttpStatus.SC_MOVED_TEMPORARILY)
                    .end()
            }
        }

        post("/token").handler(BodyHandler.create()).coroutineHandler { rc ->
            val userJson = rc.bodyAsJson
            val user = keycloak.authenticateAwait(userJson)
            rc.response().end(user.principal().toString())
        }

        // Create Databases
        post("/")
            .consumes("application/xml")
            .produces("application/xml")
            .coroutineHandler {
                Auth(keycloak, AuthRole.VIEW).handle(it)
                it.next()
            }.handler(BodyHandler.create()).coroutineHandler {
                XmlGet(location).handle(it)
            }
        post("/")
            .consumes("application/json")
            .produces("application/json")
            .coroutineHandler {
                Auth(keycloak, AuthRole.VIEW).handle(it)
                it.next()
            }.handler(BodyHandler.create()).coroutineHandler {
                JsonGet(location).handle(it)
            }

        // Read Databases
        get("/").produces("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            XmlGet(location).handle(it)
        }
        get("/").produces("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            JsonGet(location).handle(it)
        }

        // Delete Databases
        delete("/").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            Delete(location).handle(it)
        }

        // Create Database
        post("/:database").consumes("multipart/form-data").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            CreateMultipleResources(location).handle(it)
        }

        // Read Database
        get("/:database").produces("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            XmlGet(location).handle(it)
        }
        get("/:database").produces("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            JsonGet(location).handle(it)
        }

        // Update Database
        put("/:database").consumes("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            XmlCreate(location, false).handle(it)
        }
        put("/:database").consumes("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            JsonCreate(location, false).handle(it)
        }

        // Delete Database
        delete("/:database").consumes("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            XmlDelete(location).handle(it)
        }
        delete("/:database").consumes("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            JsonDelete(location).handle(it)
        }

        // Create Resource
        post("/:database/:resource")
            .consumes("application/xml")
            .produces("application/xml")
            .coroutineHandler {
                Auth(keycloak, AuthRole.VIEW).handle(it)
                it.next()
            }.handler(BodyHandler.create()).coroutineHandler {
                XmlGet(location).handle(it)
            }
        post("/:database/:resource")
            .consumes("application/json")
            .produces("application/json")
            .coroutineHandler {
                Auth(keycloak, AuthRole.VIEW).handle(it)
                it.next()
            }.handler(BodyHandler.create()).coroutineHandler {
                JsonGet(location).handle(it)
            }

        // Read Resource
        get("/:database/:resource").produces("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            XmlGet(location).handle(it)
        }
        get("/:database/:resource").produces("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            JsonGet(location).handle(it)
        }

        // Update Resource
        put("/:database/:resource").consumes("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            XmlCreate(location, false).handle(it)
        }
        put("/:database/:resource").consumes("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            JsonCreate(location, false).handle(it)
        }

        // Delete Resource
        delete("/:database/:resource").consumes("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            XmlDelete(location).handle(it)
        }
        delete("/:database/:resource").consumes("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            JsonDelete(location).handle(it)
        }

        // Check Resource Status
        head("/:database/:resource").produces("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            XmlHead(location).handle(it)
        }
        head("/:database/:resource").produces("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            JsonHead(location).handle(it)
        }

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
                    response(
                        failureRoutingContext.response(), HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        failure.message
                    )
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
    private fun Route.coroutineHandler(fn: suspend (RoutingContext) -> Unit): Route {
        return handler { ctx ->
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
}
