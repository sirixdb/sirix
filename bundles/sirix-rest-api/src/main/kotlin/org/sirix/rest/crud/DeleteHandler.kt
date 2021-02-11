package org.sirix.rest.crud

import io.vertx.core.Promise
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.coroutines.await
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.access.DatabasesInternals
import org.sirix.rest.crud.json.JsonDelete
import org.sirix.rest.crud.xml.XmlDelete
import java.nio.file.Files
import java.nio.file.Path

class DeleteHandler(private val location: Path, private val authz: AuthorizationProvider) {
    suspend fun handle(ctx: RoutingContext): Route {
        if (ctx.pathParam("database") == null && ctx.pathParam("resource") == null) {
            val openDatabases = DatabasesInternals.getOpenDatabases()

            if (openDatabases.isNotEmpty()) {
                ctx.fail(IllegalStateException("Open databases found: $openDatabases"))
                return ctx.currentRoute()
            }

            ctx.vertx().executeBlocking { _: Promise<Unit> ->
                val databases = Files.list(location)

                databases.use {
                    databases.filter { Files.isDirectory(it) }
                        .forEach {
                            it.toFile().deleteRecursively()
                        }

                }

                ctx.response().setStatusCode(204).end()
            }.await()
        } else {
            val databaseName = ctx.pathParam("database")

            if (databaseName == null) {
                ctx.fail(IllegalStateException("No database name given."))
            } else {
                val databaseType = Databases.getDatabaseType(location.resolve(databaseName).toAbsolutePath())

                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                when (databaseType) {
                    DatabaseType.JSON -> JsonDelete(location, authz).handle(ctx)
                    DatabaseType.XML -> XmlDelete(location, authz).handle(ctx)
                }
            }
        }

        return ctx.currentRoute()
    }
}