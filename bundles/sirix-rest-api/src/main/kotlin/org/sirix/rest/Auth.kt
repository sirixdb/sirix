package org.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.ext.auth.authenticateAwait
import io.vertx.kotlin.ext.auth.isAuthorizedAwait

class Auth(private val keycloak: OAuth2Auth, private val role: String) {
    suspend fun handle(ctx: RoutingContext): Route {
        val token = ctx.request().getHeader(HttpHeaders.AUTHORIZATION.toString())

        val tokenToAuthenticate = json {
            obj("access_token" to token.substring(7),
                    "token_type" to "Bearer")
        }

        val user = keycloak.authenticateAwait(tokenToAuthenticate)

        val isAuthorized = user.isAuthorizedAwait(role)

        if (!isAuthorized) {
            ctx.fail(HttpResponseStatus.UNAUTHORIZED.code())
            ctx.response().end()
        }

        ctx.put("user", user)

        return ctx.currentRoute()
    }
}