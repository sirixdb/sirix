package io.sirix.rest.crud

import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import io.sirix.access.DatabaseType
import io.sirix.access.Databases
import io.sirix.access.DatabasesInternals
import io.sirix.rest.crud.json.JsonDelete
import io.sirix.rest.crud.xml.XmlDelete
import java.nio.file.Files
import java.nio.file.Path

class DeleteHandler(private val location: Path, private val authz: AuthorizationProvider) {
    suspend fun handle(ctx: RoutingContext): Route {
        if (ctx.pathParam("database") == null && ctx.pathParam("resource") == null) {
            val openDatabases = DatabasesInternals.getOpenDatabases()

            if (openDatabases.isNotEmpty()) {
                throw IllegalStateException("Open databases found: $openDatabases")
            }

            val databases = withContext(Dispatchers.IO) {
                Files.list(location)
            }

            val databaseNames = databases.use {
                return@use databases.filter { Files.isDirectory(it) && Databases.existsDatabase(it) }.map {
                    it.toFile().name
                }.toList()
            }
            databaseNames.forEach { databaseName ->
                ctx.put("databaseName", databaseName)
                removeDatabase(databaseName, ctx)
            }
        } else {
            val databaseName = ctx.pathParam("database")

            if (databaseName == null) {
                throw IllegalStateException("No database name given.")
            } else {
                removeDatabase(databaseName, ctx)
            }
        }

        ctx.response().setStatusCode(204).end()
        return ctx.currentRoute()
    }

    private suspend fun removeDatabase(
        databaseName: String,
        ctx: RoutingContext
    ) {
        val databaseType = Databases.getDatabaseType(location.resolve(databaseName).toAbsolutePath())

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") when (databaseType) {
            DatabaseType.JSON -> JsonDelete(location, authz).handle(ctx)
            DatabaseType.XML -> XmlDelete(location, authz).handle(ctx)
        }
    }
}