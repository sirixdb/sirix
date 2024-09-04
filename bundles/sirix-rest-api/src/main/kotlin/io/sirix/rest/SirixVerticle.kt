package io.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.HttpMethod
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.DecodeException
import io.vertx.core.json.JsonObject
import io.vertx.core.net.PemKeyCertOptions
import io.vertx.ext.auth.JWTOptions
import io.vertx.ext.auth.impl.UserConverter
import io.vertx.ext.auth.impl.UserImpl
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.auth.oauth2.OAuth2AuthorizationURL
import io.vertx.ext.auth.oauth2.OAuth2FlowType
import io.vertx.ext.auth.oauth2.Oauth2Credentials
import io.vertx.ext.auth.oauth2.authorization.KeycloakAuthorization
import io.vertx.ext.auth.oauth2.providers.KeycloakAuth
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import io.vertx.ext.web.handler.HttpException
import io.vertx.kotlin.core.http.httpServerOptionsOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.kotlin.ext.auth.oauth2.oAuth2OptionsOf
import kotlinx.coroutines.launch
import org.apache.http.HttpStatus
import io.sirix.rest.crud.*
import io.sirix.rest.crud.json.JsonCreate
import io.sirix.rest.crud.json.JsonHead
import io.sirix.rest.crud.json.JsonUpdate
import io.sirix.rest.crud.xml.XmlCreate
import io.sirix.rest.crud.xml.XmlHead
import io.sirix.rest.crud.xml.XmlUpdate
import java.io.ByteArrayOutputStream
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.UUID

class SirixVerticle : CoroutineVerticle() {
    /** User home directory. */
    private val userHome = System.getProperty("user.home")

    /** Storage for databases: Sirix data in home directory. */
    private val location = Paths.get(userHome, "sirix-data")

    //    Start an HTTP/2 server
    override suspend fun start() {
        val router = createRouter()
        val server = createServer()
        listen(server, router)
    }

    private fun createServer(): HttpServer {
        val serverOptions = httpServerOptionsOf()
            .setSsl(!config.getBoolean("use.http", false))
            .setUseAlpn(true)
        if (!config.getBoolean("use.http", false)) {
            serverOptions.setPemKeyCertOptions(
                PemKeyCertOptions()
                    .setKeyPath(location.resolve("key.pem").toString())
                    .setCertPath(location.resolve("cert.pem").toString())
            )
        }
        return vertx.createHttpServer(serverOptions)
    }


    private suspend fun listen(server: HttpServer, router: Router) {
        server.requestHandler(router).listen(config.getInteger("port", 9443)).await()
    }

    private suspend fun createRouter() = Router.router(vertx).apply {
        val oAuth2FlowType = OAuth2FlowType.valueOf(config.getString("oAuthFlowType", "PASSWORD"))
        val keycloak = createKeycloakAuth()
        val authz = KeycloakAuthorization.create()

        setupCors()
        setupRoutes(keycloak, authz, oAuth2FlowType)
        setupErrorHandling()
    }

    private suspend fun createKeycloakAuth(): OAuth2Auth {
        val oauth2Config = oAuth2OptionsOf()
            .setSite(config.getString("keycloak.url"))
            .setClientId("sirix")
            .setClientSecret(config.getString("client.secret"))
            .setTokenPath(config.getString("token.path"))
            .setAuthorizationPath(config.getString("auth.path"))
            .setJWTOptions(JWTOptions().setAudience(mutableListOf("account")))

        return KeycloakAuth.discover(vertx, oauth2Config).await()
    }

    private fun Router.setupCors() {
        val allowedHeaders = setOf(
            "x-requested-with",
            "Access-Control-Allow-Origin",
            "origin",
            "Content-Type",
            "accept",
            "X-PINGARUNER",
            "Authorization",
            "ETag"
        )
        val allowedMethods = setOf(
            HttpMethod.GET,
            HttpMethod.HEAD,
            HttpMethod.POST,
            HttpMethod.OPTIONS,
            HttpMethod.DELETE,
            HttpMethod.PATCH,
            HttpMethod.PUT
        )
        var allowedOriginPattern = config.getString("cors.allowedOriginPattern", ".*")
        if ("*" == allowedOriginPattern) {
            allowedOriginPattern = ".*"
        }

        route().handler(
            CorsHandler.create()
                .addRelativeOrigin(allowedOriginPattern)
                .allowedHeaders(allowedHeaders)
                .allowedMethods(allowedMethods)
                .allowCredentials(config.getBoolean("cors.allowCredentials", false))
        )
    }

    private fun Router.setupRoutes(keycloak: OAuth2Auth, authz: KeycloakAuthorization, oAuth2FlowType: OAuth2FlowType) {
        // User authorization route
        get("/user/authorize").coroutineHandler { rc -> handleUserAuthorize(rc, keycloak, oAuth2FlowType) }

        // Token routes
        post("/token").handler(BodyHandler.create())
            .coroutineHandler { rc -> handleToken(rc, keycloak, oAuth2FlowType) }
        post("/logout").handler(BodyHandler.create()).coroutineHandler { rc -> handleLogout(rc, keycloak) }

        // Root routes
        this.setupRootRoutes(keycloak, authz)

        // Database routes
        this.setupDatabaseRoutes(keycloak, authz)

        // Resource routes
        this.setupResourceRoutes(keycloak, authz)
    }

    private fun Router.setupRootRoutes(keycloak: OAuth2Auth, authz: KeycloakAuthorization) {
        post("/").handler(BodyHandler.create()).coroutineHandler {
            Auth(keycloak, authz, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler { GetHandler(location, keycloak, authz).handle(it) }

        get("/").coroutineHandler {
            Auth(keycloak, authz, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler { GetHandler(location, keycloak, authz).handle(it) }

        delete("/").coroutineHandler {
            Auth(keycloak, authz, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler { DeleteHandler(location, authz).handle(it) }
    }

    private fun Router.setupDatabaseRoutes(keycloak: OAuth2Auth, authz: KeycloakAuthorization) {
        post("/:database").consumes("multipart/form-data").handler(BodyHandler.create())
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.CREATE).handle(it)
                it.next()
            }.coroutineHandler { CreateMultipleResources(location).handle(it) }

        get("/:database").coroutineHandler {
            Auth(keycloak, authz, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler { GetHandler(location, keycloak, authz).handle(it) }

        put("/:database").consumes("application/xml").handler(BodyHandler.create())
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.CREATE).handle(it)
                it.next()
            }.coroutineHandler { XmlCreate(location, false).handle(it) }

        put("/:database").consumes("application/json")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.CREATE).handle(it)
                it.next()
            }.coroutineHandler { JsonCreate(location, true).handle(it) }

        delete("/:database").coroutineHandler {
            Auth(keycloak, authz, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler { DeleteHandler(location, authz).handle(it) }
    }

    private fun Router.setupResourceRoutes(keycloak: OAuth2Auth, authz: KeycloakAuthorization) {

        head("/:database/:resource").produces("application/xml")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.VIEW).handle(it)
                it.next()
            }.coroutineHandler {
                XmlHead(location).handle(it)
            }
        head("/:database/:resource").produces("application/json")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.VIEW).handle(it)
                it.next()
            }.coroutineHandler { JsonHead(location).handle(it) }

        post("/:database/:resource").consumes("application/xml").produces("application/xml")
            .handler(BodyHandler.create())
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.MODIFY).handle(it)
                it.next()
            }.coroutineHandler { XmlUpdate(location).handle(it) }
        post("/:database/:resource").consumes("application/json").produces("application/json")
            .handler(BodyHandler.create())
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.MODIFY).handle(it)
                it.next()
            }.coroutineHandler { JsonUpdate(location).handle(it) }
        post("/:database/:resource").handler(BodyHandler.create())
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.VIEW).handle(it)
                it.next()
            }.coroutineHandler { GetHandler(location, keycloak, authz).handle(it) }

        get("/:database/:resource").coroutineHandler {
            Auth(keycloak, authz, AuthRole.VIEW).handle(it)
            it.next()
        }.coroutineHandler { GetHandler(location, keycloak, authz).handle(it) }

        put("/:database/:resource").consumes("application/xml")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.CREATE).handle(it)
                it.next()
            }.coroutineHandler { XmlCreate(location, false).handle(it) }
        put("/:database/:resource").consumes("application/json")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.CREATE).handle(it)
                it.next()
            }.coroutineHandler { JsonCreate(location, false).handle(it) }

        delete("/:database/:resource").coroutineHandler {
            Auth(keycloak, authz, AuthRole.DELETE).handle(it)
            it.next()
        }.coroutineHandler { DeleteHandler(location, authz).handle(it) }

//         "/:database/:resource/subroutes"

        get("/:database/:resource/history").produces("application/json")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.VIEW).handle(it)
                it.next()
            }.coroutineHandler { HistoryHandler(location).handle(it) }

        get("/:database/:resource/diff").produces("application/json")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.VIEW).handle(it)
                it.next()
            }.coroutineHandler { DiffHandler(location).handle(it) }

        get("/:database/:resource/pathSummary").produces("application/json")
            .coroutineHandler {
                Auth(keycloak, authz, AuthRole.VIEW).handle(it)
                it.next()
            }.coroutineHandler { PathSummaryHandler(location).handle(it) }
    }

    private fun Router.setupErrorHandling() {
        route().handler { ctx -> ctx.fail(HttpResponseStatus.NOT_FOUND.code()) }
        route().failureHandler { handleFailures(it) }
    }

    private fun handleUserAuthorize(
        rc: RoutingContext,
        keycloak: OAuth2Auth,
        oAuth2FlowType: OAuth2FlowType
    ) {
        if (oAuth2FlowType != OAuth2FlowType.AUTH_CODE) {
            rc.response().setStatusCode(HttpStatus.SC_BAD_REQUEST).end()
            return
        }

        val redirectUri = rc.queryParam("redirect_uri").getOrElse(0) { config.getString("redirect.uri") }
        val state = rc.queryParam("state").getOrElse(0) { UUID.randomUUID().toString() }

        val authorizationUri = keycloak.authorizeURL(
            OAuth2AuthorizationURL()
                .setRedirectUri(redirectUri)
                .setState(state)
        )

        rc.response()
            .putHeader("Location", authorizationUri)
            .setStatusCode(HttpStatus.SC_MOVED_TEMPORARILY)
            .end()
    }

    private suspend fun handleToken(rc: RoutingContext, keycloak: OAuth2Auth, oAuth2FlowType: OAuth2FlowType) {
        try {
            val dataToAuthenticate: JsonObject = when (rc.request().getHeader(HttpHeaders.CONTENT_TYPE)) {
                "application/json" -> rc.body().asJsonObject()
                "application/x-www-form-urlencoded" -> formToJson(rc)
                else -> rc.body().asJsonObject()
            }

            when {
                dataToAuthenticate.containsKey("refresh_token") -> refreshToken(keycloak, dataToAuthenticate, rc)
                rc.queryParam("refresh_token") != null && rc.queryParam("refresh_token").isNotEmpty() -> {
                    val json = JsonObject().put("refresh_token", rc.queryParam("refresh_token").first())
                    refreshToken(keycloak, json, rc)
                }
                else -> getToken(keycloak, dataToAuthenticate, rc, oAuth2FlowType)
            }
        } catch (e: DecodeException) {
            rc.fail(
                HttpException(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "\"application/json\" and \"application/x-www-form-urlencoded\" are supported Content-Types." +
                            "If none is specified it's tried to parse as JSON"
                )
            )
        }
    }

    private fun handleLogout(rc: RoutingContext, keycloak: OAuth2Auth) {
        val user = UserConverter.decode(rc.body().asJsonObject())
        keycloak.revoke(user, "access-token").onSuccess {
            keycloak.revoke(user, "refresh-token").onSuccess {
                rc.response().end()
            }
        }
    }

    private suspend fun getToken(
        keycloak: OAuth2Auth,
        dataToAuthenticate: JsonObject,
        rc: RoutingContext,
        oAuth2FlowType: OAuth2FlowType
    ) {
        val credentials = Oauth2Credentials(dataToAuthenticate)
            .setFlow(oAuth2FlowType)
        val user = keycloak.authenticate(credentials).await()
        rc.response().end(user.principal().toString())
    }

    private fun refreshToken(
        keycloak: OAuth2Auth,
        dataToAuthenticate: JsonObject,
        rc: RoutingContext
    ) {
        val user = UserImpl(dataToAuthenticate, JsonObject())
        keycloak.refresh(user) {
            if (it.succeeded()) {
                rc.response().end(it.result().principal().toString())
            }
        }
    }

    private fun formToJson(rc: RoutingContext): JsonObject {
        val formAttributes = rc.request().formAttributes()
        return when (val refreshToken: String? = formAttributes["refresh_token"]) {
            null -> JsonObject()
                .put("code", formAttributes["code"])
                .put("redirect_uri", formAttributes["redirect_uri"])
                .put("response_type", formAttributes["response_type"])

            else -> JsonObject().put("refresh_token", refreshToken)
        }
    }

    private fun handleFailures(failureRoutingContext: RoutingContext) {
        val statusCode = failureRoutingContext.statusCode()
        val failure = failureRoutingContext.failure()

        val out = ByteArrayOutputStream()
        val printWriter = PrintWriter(out)

        printWriter.use {
            failure?.printStackTrace(printWriter)
            printWriter.flush()

            if (statusCode == -1) {
                if (failure is HttpException) {
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

    private fun response(response: HttpServerResponse, statusCode: Int, failureMessage: String?) {
        response.setStatusCode(statusCode)
        if (failureMessage.isNullOrEmpty())
            response.end("Exception occured (statusCode: $statusCode)")
        else
            response.end("Exception occured (statusCode: $statusCode) -- $failureMessage")
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