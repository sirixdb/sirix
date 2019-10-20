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

/**
 * Authentication.
 */
class Auth(private val keycloak: OAuth2Auth, private val role: AuthRole) {
    suspend fun handle(ctx: RoutingContext): Route {
        val token = ctx.request().getHeader(HttpHeaders.AUTHORIZATION.toString())

        val tokenToAuthenticate = json {
            obj(
                "access_token" to token.substring(7),
                "token_type" to "Bearer"
            )
        }
        val database = ctx.pathParam("database")
        val user = keycloak.authenticateAwait(tokenToAuthenticate)

        val isAuthorized =
            if (database == null) {
                false
            } else {
                user.isAuthorizedAwait(role.databaseRole(database))
            }

        if (!isAuthorized && !user.isAuthorizedAwait(role.keycloakRole())) {
            ctx.fail(HttpResponseStatus.UNAUTHORIZED.code())
            ctx.response().end()
        }

        ctx.put("user", user)

        return ctx.currentRoute()
    }
}