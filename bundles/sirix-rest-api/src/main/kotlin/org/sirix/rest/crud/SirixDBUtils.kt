package org.sirix.rest.crud

import org.sirix.access.User as SirixDBUser

import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.KeycloakHelper
import io.vertx.ext.web.RoutingContext
import java.util.*

class SirixDBUtils {
    companion object {
        fun createSirixDBUser(ctx: RoutingContext): SirixDBUser {
            val user = ctx.get("user") as User
            val accessToken = KeycloakHelper.accessToken(user.principal())
            val userId = accessToken.getString("sub")
            val userName = accessToken.getString("userName")
            val userUuid = UUID.fromString(userId)
            return SirixDBUser(userName, userUuid)
        }
    }
}