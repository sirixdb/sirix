package io.sirix.rest

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authentication.TokenCredentials
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization
import io.vertx.ext.auth.authorization.RoleBasedAuthorization
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch

/**
 * Authentication.
 */
class Auth(private val keycloak: OAuth2Auth, private val authz: AuthorizationProvider, private val role: io.sirix.rest.AuthRole) {
    suspend fun handle(ctx: RoutingContext): Route {
        ctx.request().pause()
        val token = ctx.request().getHeader(HttpHeaders.AUTHORIZATION.toString())

        if (token == null) {
            ctx.fail(HttpResponseStatus.UNAUTHORIZED.code())
            return ctx.currentRoute()
        }

        val credentials = TokenCredentials(token.substring(7))
        val user = keycloak.authenticate(credentials).await()
        val database = ctx.pathParam("database")

        authz.getAuthorizations(user).await()

        val isAuthorized =
            if (database == null) {
                false
            } else {
                RoleBasedAuthorization.create(role.databaseRole(database)).match(user)
            }

        if (!isAuthorized && !RoleBasedAuthorization.create(role.keycloakRole()).match(user)) {
            ctx.fail(HttpResponseStatus.UNAUTHORIZED.code())
            return ctx.currentRoute()
        }

        ctx.put("user", user)

        ctx.request().resume()
        return ctx.currentRoute()
    }

    companion object {
        @OptIn(DelicateCoroutinesApi::class)
        fun checkIfAuthorized(user: User, dispatcher: CoroutineDispatcher, name: String, role: io.sirix.rest.AuthRole, authz: AuthorizationProvider) {
            GlobalScope.launch(dispatcher) {
                authz.getAuthorizations(user).await()
                val isAuthorized = PermissionBasedAuthorization.create(role.databaseRole(name)).match(user)

                require(isAuthorized || RoleBasedAuthorization.create(role.keycloakRole()).match(user)) {
                    "${HttpResponseStatus.UNAUTHORIZED.code()}: User is not allowed to $role the database $name"
                }
            }
        }
    }
}
