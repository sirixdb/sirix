package org.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonObject
import io.vertx.core.net.PemKeyCertOptions
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.auth.oauth2.OAuth2FlowType
import io.vertx.ext.auth.oauth2.impl.OAuth2TokenImpl
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
import io.vertx.kotlin.ext.auth.oauth2.logoutAwait
import io.vertx.kotlin.ext.auth.oauth2.oAuth2ClientOptionsOf
import io.vertx.kotlin.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.kotlin.ext.auth.oauth2.refreshAwait
import kotlinx.coroutines.launch
import org.apache.http.HttpStatus
import org.sirix.rest.crud.*
import org.sirix.rest.crud.json.JsonCreate
import org.sirix.rest.crud.json.JsonHead
import org.sirix.rest.crud.json.JsonUpdate
import org.sirix.rest.crud.xml.XmlCreate
import org.sirix.rest.crud.xml.XmlHead
import org.sirix.rest.crud.xml.XmlUpdate
import java.io.ByteArrayOutputStream
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
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
        if (config.getBoolean("use.http", false)) {
            val server = vertx.createHttpServer(
                httpServerOptionsOf()
                    .setSsl(false)
            )

            listen(server, router, 8000)
        } else {
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

            listen(server, router, 9443)
        }
    }

    private suspend fun listen(server: HttpServer, router: Router, port: Int) {
        server.requestHandler { router.handle(it) }
            .listenAwait(config.getInteger("port", port))
    }

    private suspend fun createRouter() = Router.router(vertx).apply {

        val oauth2Config = oAuth2ClientOptionsOf()
            .setFlow(OAuth2FlowType.valueOf(config.getString("oAuthFlowType", "PASSWORD")))
            .setSite(config.getString("keycloak.url"))
            .setClientID("sirix")
            .setClientSecret(config.getString("client.secret"))
            .setTokenPath(config.getString("token.path"))
            .setAuthorizationPath(config.getString("auth.path"))

        val keycloak = KeycloakAuth.discoverAwait(
            vertx, oauth2Config
        )

        val allowedHeaders = HashSet<String>()
        allowedHeaders.add("x-requested-with")
        allowedHeaders.add("Access-Control-Allow-Origin")
        allowedHeaders.add("origin")
        allowedHeaders.add("Content-Type")
        allowedHeaders.add("accept")
        allowedHeaders.add("X-PINGARUNER")
        allowedHeaders.add("Authorization")
        allowedHeaders.add("ETag")

        val allowedMethods = HashSet<HttpMethod>()
        allowedMethods.add(HttpMethod.GET)
        allowedMethods.add(HttpMethod.HEAD)
        allowedMethods.add(HttpMethod.POST)
        allowedMethods.add(HttpMethod.OPTIONS)

        allowedMethods.add(HttpMethod.DELETE)
        allowedMethods.add(HttpMethod.PATCH)
        allowedMethods.add(HttpMethod.PUT)

        this.route().handler(
            CorsHandler.create(
                config.getString(
                    "cors.allowedOriginPattern",
                    "*"
                )
            ).allowedHeaders(allowedHeaders).allowedMethods(allowedMethods).allowCredentials(
                config.getBoolean("cors.allowCredentials", false)
            )
        )

        get("/user/authorize").coroutineHandler { rc ->
            if (oauth2Config.flow != OAuth2FlowType.AUTH_CODE) {
                rc.response().statusCode = HttpStatus.SC_BAD_REQUEST
            } else {
                val redirectUri =
                    rc.queryParam("redirect_uri").getOrElse(0) { config.getString("redirect.uri") }
                val state = rc.queryParam("state").getOrElse(0) { UUID.randomUUID().toString() }

                val authorizationUri = keycloak.authorizeURL(
                    JsonObject()
                        .put("redirect_uri", redirectUri)
                        .put("state", state)
                )
                rc.response().putHeader("Location", authorizationUri)
                    .setStatusCode(HttpStatus.SC_MOVED_TEMPORARILY)
                    .end()
            }
        }

        post("/token").handler(BodyHandler.create()).coroutineHandler { rc ->
            try {
                val dataToAuthenticate: JsonObject =
                    when (rc.request().getHeader(HttpHeaders.CONTENT_TYPE)) {
                        "application/json" -> rc.bodyAsJson
                        "application/x-www-form-urlencoded" -> formToJson(rc)
                        else -> rc.bodyAsJson
                    }

                when {
                    dataToAuthenticate.containsKey("refresh_token") -> refreshToken(keycloak, dataToAuthenticate, rc)
                    rc.queryParam("refresh_token") != null && rc.queryParam("refresh_token").isNotEmpty() -> {
                        val json = JsonObject()
                            .put("refresh_token", rc.queryParam("refresh_token"))
                        refreshToken(keycloak, json, rc)
                    }
                    else -> getToken(keycloak, dataToAuthenticate, rc)
                }
            } catch (e: DecodeException) {
                rc.fail(
                    HttpStatusException(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        "\"application/json\" and \"application/x-www-form-urlencoded\" are supported Content-Types." +
                                "If none is specified it's tried to parse as JSON"
                    )
                )
            }
        }

        post("/logout").handler(BodyHandler.create()).coroutineHandler { rc ->
            val token = OAuth2TokenImpl(keycloak, rc.bodyAsJson)
            token.logoutAwait()
            rc.response().end()
        }

        // "/"
        post("/").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            GetHandler(location, keycloak).handle(it)
        }

        get("/").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            GetHandler(location, keycloak).handle(it)
        }

        delete("/").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            DeleteHandler(location).handle(it)
        }

        // "/:database"
        post("/:database").consumes("multipart/form-data").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            CreateMultipleResources(location).handle(it)
        }

        get("/:database").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            GetHandler(location, keycloak).handle(it)
        }

        put("/:database").consumes("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.handler(BodyHandler.create()).coroutineHandler {
            XmlCreate(location, false).handle(it)
        }
        put("/:database").consumes("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.coroutineHandler {
            JsonCreate(location, true).handle(it)
        }

        delete("/:database").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            DeleteHandler(location).handle(it)
        }

        // "/:database/:resource"
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

        post("/:database/:resource")
            .consumes("application/xml")
            .produces("application/xml")
            .coroutineHandler {
                Auth(keycloak, AuthRole.MODIFY).handle(it)
                it.next()
            }.handler(BodyHandler.create()).coroutineHandler {
                XmlUpdate(location).handle(it)
            }
        post("/:database/:resource")
            .consumes("application/json")
            .produces("application/json")
            .coroutineHandler {
                Auth(keycloak, AuthRole.MODIFY).handle(it)
                it.next()
            }.handler(BodyHandler.create()).coroutineHandler {
                JsonUpdate(location).handle(it)
            }

        post("/:database/:resource")
            .coroutineHandler {
                Auth(keycloak, AuthRole.VIEW).handle(it)
                it.next()
            }.handler(BodyHandler.create()).coroutineHandler {
                GetHandler(location, keycloak).handle(it)
            }

        get("/:database/:resource").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            GetHandler(location, keycloak).handle(it)
        }

        put("/:database/:resource").consumes("application/xml").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.coroutineHandler {
            XmlCreate(location, false).handle(it)
        }
        put("/:database/:resource").consumes("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.CREATE).handle(it)
            it.next()
        }.coroutineHandler {
            JsonCreate(location, false).handle(it)
        }

        delete("/:database/:resource").coroutineHandler {
            Auth(keycloak, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler {
            DeleteHandler(location).handle(it)
        }

        // "/:database/:resource/subroutes"
        get("/:database/:resource/history").produces("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            HistoryHandler(location).handle(it)
        }
        get("/:database/:resource/diff").produces("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            DiffHandler(location).handle(it)
        }
        get("/:database/:resource/pathSummary").produces("application/json").coroutineHandler {
            Auth(keycloak, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler {
            PathSummaryHandler(location).handle(it)
        }

        // Exception with status code
        route().handler { ctx ->
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code()))
        }

        route().failureHandler { failureRoutingContext ->
            val statusCode = failureRoutingContext.statusCode()
            val failure = failureRoutingContext.failure()

            val out = ByteArrayOutputStream()
            val printWriter = PrintWriter(out)

            printWriter.use {
                failure.printStackTrace(printWriter)
                printWriter.flush()

                if (statusCode == -1) {
                    if (failure is HttpStatusException) {
                        response(
                            failureRoutingContext.response(),
                            failure.statusCode,
                            out.toString(StandardCharsets.UTF_8)
                        )
                    } else {
                        response(
                            failureRoutingContext.response(), HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            out.toString(StandardCharsets.UTF_8)
                        )
                    }
                } else {
                    response(failureRoutingContext.response(), statusCode, out.toString(StandardCharsets.UTF_8))
                }
            }
        }
    }

    private suspend fun getToken(
        keycloak: OAuth2Auth,
        dataToAuthenticate: JsonObject,
        rc: RoutingContext
    ) {
        val user = keycloak.authenticateAwait(dataToAuthenticate)
        rc.response().end(user.principal().toString())
    }

    private suspend fun refreshToken(
        keycloak: OAuth2Auth,
        dataToAuthenticate: JsonObject,
        rc: RoutingContext
    ) {
        val token = OAuth2TokenImpl(keycloak, dataToAuthenticate)
        token.refreshAwait()
        rc.response().end(token.principal().toString())
    }

    private fun formToJson(rc: RoutingContext): JsonObject {
        val formAttributes = rc.request().formAttributes()
        val refreshToken: String? =
            formAttributes.get("refresh_token")
        if (refreshToken == null) {
            val code =
                formAttributes.get("code")
            val redirectUri =
                formAttributes.get("redirect_uri")
            val responseType =
                formAttributes.get("response_type")

            return JsonObject()
                .put("code", code)
                .put("redirect_uri", redirectUri)
                .put("response_type", responseType)
        } else {
            return JsonObject()
                .put("refresh_token", refreshToken)
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
