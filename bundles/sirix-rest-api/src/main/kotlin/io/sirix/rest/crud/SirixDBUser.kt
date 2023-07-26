package io.sirix.rest.crud

import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.json.get
import java.util.UUID

class SirixDBUser {
    companion object {
        fun create(ctx: RoutingContext): io.sirix.access.User {
            val user = ctx.get<User>("user")
            val accessToken = user.attributes().get<JsonObject>("accessToken")
            val userId = accessToken.getString("sub")
            val userName = accessToken.getString("preferred_username")
            val userUuid = UUID.fromString(userId)
            return io.sirix.access.User(userName, userUuid)
        }
    }
}
