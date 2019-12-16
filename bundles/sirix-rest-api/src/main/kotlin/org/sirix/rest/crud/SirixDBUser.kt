package org.sirix.rest.crud

import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.KeycloakHelper
import io.vertx.ext.web.RoutingContext
import java.util.*

class SirixDBUser {
    companion object {
        fun create(ctx: RoutingContext): org.sirix.access.User {
            val user = ctx.get("user") as User
            val accessToken = KeycloakHelper.accessToken(user.principal())
            val userId = accessToken.getString("sub")
            val userName = accessToken.getString("preferred_username")
            val userUuid = UUID.fromString(userId)
            return org.sirix.access.User(userName, userUuid)
        }
    }
}
