package org.sirix.rest.crud

import io.vertx.core.Promise
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.rest.crud.json.JsonDelete
import org.sirix.rest.crud.xml.XmlDelete
import java.nio.file.Files
import java.nio.file.Path

class Delete(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        if (ctx.pathParam("database") == null && ctx.pathParam("resource") == null) {
            ctx.vertx().executeBlockingAwait { _: Promise<Unit> ->
                val databases = Files.list(location)

                databases.use {
                    databases.filter { Files.isDirectory(it) }
                        .forEach {
                            it.toFile().deleteRecursively()
                        }

                    ctx.response().setStatusCode(204).end()
                }
            }
        } else {
            val databaseName = ctx.pathParam("database")

            if (databaseName == null) {
                ctx.fail(IllegalStateException("No database name given."))
            }

            val databaseType = Databases.getDatabaseType(location.resolve(databaseName).toAbsolutePath())

            when (databaseType) {
                DatabaseType.JSON -> JsonDelete(location).handle(ctx)
                DatabaseType.XML -> XmlDelete(location).handle(ctx)
            }
        }

        return ctx.currentRoute()
    }
}