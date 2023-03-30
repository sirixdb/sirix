package org.sirix.rest.crud

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.file.impl.FileResolverImpl
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.ResourceConfiguration
import org.sirix.access.User
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.ResourceSession
import java.nio.file.Files
import java.nio.file.Path

abstract class AbstractCreateHandler<T : ResourceSession<*, *>>(
    private val location: Path,
    private val createMultipleResources: Boolean = false
) : Handler {

    override suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")

        if (resource == null) {
            val dbFile = location.resolve(databaseName)
            val vertxContext = ctx.vertx().orCreateContext
            createDatabaseIfNotExists(dbFile, vertxContext)
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()
        }

        if (databaseName == null) {
            throw IllegalArgumentException("Database name and resource data to store not given.")
        } else {
            if (createMultipleResources) {
                createMultipleResources(databaseName, ctx)
                return ctx.currentRoute()
            }
            shredder(databaseName, resource, ctx)
            return ctx.currentRoute()
        }
    }

    suspend fun prepareDatabasePath(dbFile: Path, context: Context): DatabaseConfiguration? {
        return context.executeBlocking { promise: Promise<DatabaseConfiguration> ->
            val dbExists = Files.exists(dbFile)

            if (!dbExists) {
                Files.createDirectories(dbFile.parent)
            }

            val dbConfig = DatabaseConfiguration(dbFile)
            promise.complete(dbConfig)
        }.await()
    }

    suspend fun createOrRemoveAndCreateResource(
        database: Database<*>,
        resConfig: ResourceConfiguration?,
        resPathName: String, dispatcher: CoroutineDispatcher
    ) {
        withContext(dispatcher) {
            if (!database.createResource(resConfig)) {
                database.removeResource(resPathName)
                database.createResource(resConfig)
            }
        }
    }

    private suspend fun shredder(
        databaseName: String, resPathName: String = databaseName,
        ctx: RoutingContext
    ) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        ctx.request().pause()
        createDatabaseIfNotExists(dbFile, context)
        ctx.request().resume()
        insertResource(dbFile, resPathName, ctx)
    }

    private suspend fun createMultipleResources(databaseName: String, ctx: RoutingContext) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()
        val sirixDBUser = SirixDBUser.create(ctx)

        createDatabaseIfNotExists(dbFile, context)

        withContext(Dispatchers.IO) {
            val database = openDatabase(dbFile, sirixDBUser)
            database.use {
                BodyHandler.create().handle(ctx)
                val fileResolver = FileResolverImpl()
                val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                ctx.fileUploads().forEach { fileUpload ->
                    val fileName = fileUpload.fileName()
                    val resConfig = ResourceConfiguration.Builder(fileName).useDeweyIDs(true)
                        .hashKind(HashType.valueOf(hashType.uppercase())).build()
                    createOrRemoveAndCreateResource(database, resConfig, fileName, dispatcher)

                    val manager = database.beginResourceSession(fileName)

                    manager.use {
                        insertResourceSubtreeAsFirstChild(
                            manager,
                            fileResolver.resolveFile(fileUpload.uploadedFileName()).toPath(),
                            ctx
                        )
                    }
                }

            }
            ctx.response().setStatusCode(201).end()
        }
    }

    abstract suspend fun createDatabaseIfNotExists(dbFile: Path, context: Context): DatabaseConfiguration?
    abstract suspend fun insertResource(dbFile: Path?, resPathName: String, ctx: RoutingContext)
    abstract fun insertResourceSubtreeAsFirstChild(manager: T, filePath: Path, ctx: RoutingContext): Long
    abstract suspend fun openDatabase(dbFile: Path, sirixDBUser: User): Database<T>

    abstract fun serializeResource(manager: T, routingContext: RoutingContext): String
}