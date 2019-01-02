package org.sirix.rest

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.ext.auth.authenticateAwait

class Auth(private val keycloak: OAuth2Auth) {
    suspend fun authenticateUser(ctx: RoutingContext): User {
        val token = ctx.request().getHeader(HttpHeaders.AUTHORIZATION.toString())

        val tokenToAuthenticate = json {
            obj("access_token" to token.substring(7),
                    "token_type" to "Bearer")
        }

        return keycloak.authenticateAwait(tokenToAuthenticate)
    }
}