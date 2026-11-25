package io.sirix.rest.crud

import io.vertx.core.Context
import io.vertx.core.buffer.Buffer
import io.vertx.ext.auth.User as VertxUser
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import io.sirix.access.MutationGate
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.ResourceConfiguration
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.api.ResourceSession
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import java.nio.file.Files
import java.nio.file.Path

abstract class AbstractCreateHandler<T : ResourceSession<*, *>>(
    private val location: Path
) : Handler {
    companion object {
        protected const val MAX_NODES_TO_SERIALIZE = 5000

        /**
         * Create [resPathName], or replace it when it already exists. Creating needs only the CREATE
         * role the route checked. Replacing discards the resource's history, so it also needs MODIFY
         * and DELETE on [databaseName], both checked before anything is removed (REST-210, #1477).
         */
        fun <R> createAndWriteResource(
            database: Database<*>,
            resConfig: ResourceConfiguration,
            resPathName: String,
            user: VertxUser,
            databaseName: String,
            block: suspend CoroutineScope.() -> R
        ): R {
            MutationGate.acquireResource(database.databaseConfig.databaseFile, resPathName).use {
                if (!database.createResource(resConfig)) {
                    check(database.existsResource(resPathName)) { "Could not create resource '$resPathName'" }
                    Auth.checkIfAuthorized(user, databaseName, AuthRole.MODIFY)
                    Auth.checkIfAuthorized(user, databaseName, AuthRole.DELETE)
                    database.removeResource(resPathName)
                    check(database.createResource(resConfig)) { "Could not create resource '$resPathName'" }
                }
                try {
                    return runBlocking(block = block)
                } catch (failure: Exception) {
                    try {
                        database.removeResource(resPathName)
                    } catch (cleanup: Exception) {
                        failure.addSuppressed(cleanup)
                    }
                    throw failure
                }
            }
        }
    }

    override suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")

        if (databaseName != null) {
            PathValidation.validatePathParam(databaseName, "database")
        }
        if (resource != null) {
            PathValidation.validatePathParam(resource, "resource")
        }

        if (resource == null) {
            // A database-only route creates the database without consuming a resource body.
            val dbFile = location.resolve(databaseName)
            val vertxContext = ctx.vertx().orCreateContext
            createDatabaseIfNotExists(dbFile, vertxContext)
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()!!
        }

        if (databaseName == null) {
            throw IllegalArgumentException("Database name and resource data to store not given.")
        } else {
            shredder(databaseName, resource, ctx)
            return ctx.currentRoute()!!
        }
    }

    suspend fun prepareDatabasePath(dbFile: Path, context: Context): DatabaseConfiguration? {

        return context.executeBlocking {
            val dbExists = Files.exists(dbFile)

            if (!dbExists) {
                Files.createDirectories(dbFile.parent)
            }

            DatabaseConfiguration(dbFile)
        }.coAwait()
    }

    private suspend fun shredder(
        databaseName: String, resPathName: String = databaseName,
        ctx: RoutingContext
    ) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        ctx.request().pause()
        createDatabaseIfNotExists(dbFile, context)
        // DON'T resume here - let insertResource/shredder handle resume AFTER setting up handlers
        insertResource(dbFile, resPathName, ctx)
    }

    private suspend fun createDatabaseIfNotExists(
        dbFile: Path,
        context: Context
    ): DatabaseConfiguration? {
        val dbConfig = prepareDatabasePath(dbFile, context)
        return context.executeBlocking {
            if (dbConfig != null && !Databases.existsDatabase(dbFile)) {
                createDatabase(dbConfig)
            }
            dbConfig
        }.coAwait()
    }

    abstract suspend fun insertResource(dbFile: Path?, resPathName: String, ctx: RoutingContext)
    abstract fun serializeResource(manager: T, routingContext: RoutingContext): Buffer
    abstract fun createDatabase(dbConfig: DatabaseConfiguration?)
}
