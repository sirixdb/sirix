package io.sirix.rest.crud

import io.sirix.query.MutationAuthorizer
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider

internal class SessionMutationAuthorizer(
    private val user: User,
    private val authz: AuthorizationProvider
) : MutationAuthorizer {
    override fun authorizeWrite(databaseName: String, resourceName: String) {
        Auth.checkIfAuthorized(user, databaseName, AuthRole.MODIFY, authz)
    }

    override fun authorizeCreate(databaseName: String, resourceName: String?) {
        Auth.checkIfAuthorized(user, databaseName, AuthRole.CREATE, authz)
    }

    override fun authorizeDelete(databaseName: String) {
        Auth.checkIfAuthorized(user, databaseName, AuthRole.DELETE, authz)
    }
}
