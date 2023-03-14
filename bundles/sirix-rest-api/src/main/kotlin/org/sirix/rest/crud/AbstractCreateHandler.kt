package org.sirix.rest.crud

import io.vertx.core.Context
import io.vertx.core.Handler
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext

import org.sirix.access.DatabaseConfiguration

import java.nio.file.Path
abstract class AbstractCreateHandler: HandlerInterface {
    abstract override suspend fun handle(ctx: RoutingContext): Route
    abstract suspend fun createMultipleResources(databaseName: String, ctx: RoutingContext)
    abstract suspend fun shredder(databaseName: String, resPathName: String, ctx: RoutingContext)
    abstract suspend fun insertResource(dbFile: Path?, resPathName: String, ctx: RoutingContext)
    abstract suspend fun createDatabaseIfNotExists(dbFile: Path, context: Context): DatabaseConfiguration?
}