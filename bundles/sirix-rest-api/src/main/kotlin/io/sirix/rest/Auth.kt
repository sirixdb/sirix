package io.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authentication.TokenCredentials
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException
import io.vertx.kotlin.coroutines.coAwait

/**
 * Authentication and authorization handler.
 */
class Auth(private val keycloak: OAuth2Auth, private val authz: AuthorizationProvider, private val role: AuthRole) {

    suspend fun handle(ctx: RoutingContext): Route {
        ctx.request().pause()
        val token = ctx.request().getHeader(HttpHeaders.AUTHORIZATION.toString())

        if (token == null || !token.regionMatches(0, BEARER_PREFIX, 0, BEARER_PREFIX.length, ignoreCase = true)) {
            ctx.fail(HttpResponseStatus.UNAUTHORIZED.code())
            return ctx.currentRoute()
        }

        val credentials = TokenCredentials(token.substring(BEARER_PREFIX.length))
        val user = try {
            keycloak.authenticate(credentials).coAwait()
        } catch (e: Exception) {
            ctx.fail(HttpResponseStatus.UNAUTHORIZED.code())
            return ctx.currentRoute()
        }
        val database = ctx.pathParam("database")

        authz.getAuthorizations(user).coAwait()

        val isAuthorized =
            if (database == null) {
                false
            } else {
                RoleBasedAuthorization.create(role.databaseRole(database)).match(user)
            }

        if (!isAuthorized && !RoleBasedAuthorization.create(role.keycloakRole()).match(user)) {
            ctx.fail(HttpResponseStatus.FORBIDDEN.code())
            return ctx.currentRoute()
        }

        ctx.put("user", user)

        ctx.request().resume()
        return ctx.currentRoute()
    }

    companion object {
        private const val BEARER_PREFIX = "Bearer "

        /**
         * Checks if the user is authorized for the given role on the named database.
         * This is a synchronous, blocking check — must be called from a blocking context.
         * Throws [HttpException] with 403 if not authorized.
         */
        fun checkIfAuthorized(user: User, name: String, role: AuthRole, authz: AuthorizationProvider) {
            val isAuthorized = RoleBasedAuthorization.create(role.databaseRole(name)).match(user)

            if (!isAuthorized && !RoleBasedAuthorization.create(role.keycloakRole()).match(user)) {
                throw HttpException(HttpResponseStatus.FORBIDDEN.code(), "Not authorized")
            }
        }
    }
}
