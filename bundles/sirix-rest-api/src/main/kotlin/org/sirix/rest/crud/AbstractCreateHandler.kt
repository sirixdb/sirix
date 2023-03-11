package org.sirix.rest.crud

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import org.sirix.access.DatabaseConfiguration
import org.sirix.api.Database
import org.sirix.api.ResourceSession
import org.sirix.api.json.JsonResourceSession
import java.nio.file.Files
import java.nio.file.Path

abstract class AbstractCreateHandler(location: Path, createMultipleResources: Boolean) {

//    abstract suspend fun handle(ctx: RoutingContext): Route
    abstract suspend fun shredder(databaseName: String, resource: String, ctx: RoutingContext)
    abstract suspend fun createMultipleResources(databaseName: String, ctx: RoutingContext)
    abstract suspend fun createDatabaseIfNotExists(dbFile: Path, context: Context) : DatabaseConfiguration?
    abstract suspend fun insertResource(dbFile: Path, resPathName: String, ctx: RoutingContext)

    protected suspend fun prepareDatabasePath(dbFile: Path, context: Context): DatabaseConfiguration? {
        return context.executeBlocking{ promise: Promise<DatabaseConfiguration> ->
            val dbExists = Files.exists(dbFile)

            if(!dbExists) {
                Files.createDirectories(dbFile.parent)
            }

            val dbConfig = DatabaseConfiguration(dbFile)
            promise.complete(dbConfig)
        }.await()
    }
}