package org.sirix.rest.crud

import io.vertx.core.Future
import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import java.nio.file.Files
import java.nio.file.Path

class Delete(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val user = ctx.get("user") as User

        ctx.vertx().executeBlockingAwait { future: Future<Unit> ->
            val databases = Files.list(location)

            databases.use {
                databases.filter { Files.isDirectory(it) }
                    .forEach {
                        it.toFile().deleteRecursively()
                    }

                ctx.response().setStatusCode(200).end()
            }
        }

        return ctx.currentRoute()
    }
}