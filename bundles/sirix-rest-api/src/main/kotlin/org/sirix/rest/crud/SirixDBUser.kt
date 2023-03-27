package org.sirix.rest.crud

import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import java.util.UUID

class SirixDBUser {
    companion object {
        fun create(ctx: RoutingContext): org.sirix.access.User {
            val user = ctx.get("user") as User
            val principal = user.principal()
            val userId = principal.getString("sub")
            val userName = principal.getString("preferred_username")
            val userUuid = UUID.fromString(userId)
            return org.sirix.access.User(userName, userUuid)
        }
    }
}
