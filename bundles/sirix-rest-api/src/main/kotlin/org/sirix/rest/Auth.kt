package org.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.ext.auth.authenticateAwait
import io.vertx.kotlin.ext.auth.isAuthorizedAwait
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch

/**
 * Authentication.
 */
class Auth(private val keycloak: OAuth2Auth, private val role: AuthRole) {
    suspend fun handle(ctx: RoutingContext): Route {
        ctx.request().pause()
        val token = ctx.request().getHeader(HttpHeaders.AUTHORIZATION.toString())

        val tokenToAuthenticate = json {
            obj(
                "access_token" to token.substring(7),
                "token_type" to "Bearer"
            )
        }

        val user = keycloak.authenticateAwait(tokenToAuthenticate)
        val database = ctx.pathParam("database")

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

        ctx.request().resume()
        return ctx.currentRoute()
    }

    companion object {
        fun checkIfAuthorized(user: User, dispatcher: CoroutineDispatcher, name: String, role: AuthRole) {
            GlobalScope.launch(dispatcher) {
                val isAuthorized = user.isAuthorizedAwait(role.databaseRole(name))

                require(isAuthorized || user.isAuthorizedAwait(role.keycloakRole())) {
                    IllegalStateException("${HttpResponseStatus.UNAUTHORIZED.code()}: User is not allowed to $role the database $name")
                }
            }
        }
    }
}