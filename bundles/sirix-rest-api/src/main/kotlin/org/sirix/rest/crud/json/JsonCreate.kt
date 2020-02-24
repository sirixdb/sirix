package org.sirix.rest.crud.json

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.file.OpenOptions
import io.vertx.core.file.impl.FileResolver
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.core.file.closeAwait
import io.vertx.kotlin.core.file.deleteAwait
import io.vertx.kotlin.core.file.openAwait
import io.vertx.kotlin.core.http.pipeToAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.json.shredder.JsonShredder
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

class JsonCreate(
    private val location: Path,
    private val createMultipleResources: Boolean = false
) {
    suspend fun handle(ctx: RoutingContext): Route {
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
            ctx.fail(IllegalArgumentException("Database name and resource data to store not given."))
        }

        if (createMultipleResources) {
            createMultipleResources(databaseName, ctx)
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()
        }

        shredder(databaseName, resource, ctx)

        return ctx.currentRoute()
    }

    private suspend fun createMultipleResources(
        databaseName: String,
        ctx: RoutingContext
    ) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()
        createDatabaseIfNotExists(dbFile, context)

        val sirixDBUser = SirixDBUser.create(ctx)
        val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

        database.use {
            BodyHandler.create().handle(ctx)
            val fileResolver = FileResolver()
            ctx.fileUploads().forEach { fileUpload ->
                val fileName = fileUpload.fileName()
                val resConfig = ResourceConfiguration.Builder(fileName).build()

                createOrRemoveAndCreateResource(
                    database,
                    resConfig,
                    fileName,
                    dispatcher
                )

                val manager = database.openResourceManager(fileName)

                manager.use {
                    insertJsonSubtreeAsFirstChild(
                        manager,
                        fileResolver.resolveFile(fileUpload.uploadedFileName()).toPath()
                    )
                }
            }
        }
    }

    private suspend fun shredder(
        databaseName: String, resPathName: String = databaseName,
        ctx: RoutingContext
    ) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()
        ctx.request().pause()
        createDatabaseIfNotExists(dbFile, context)
        ctx.request().resume()

        insertResource(dbFile, resPathName, dispatcher, ctx)
    }

    private suspend fun insertResource(
        dbFile: Path?, resPathName: String,
        dispatcher: CoroutineDispatcher,
        ctx: RoutingContext
    ) {
        ctx.request().pause()
        val fileResolver = FileResolver()
        val filePath = fileResolver.resolveFile(UUID.randomUUID().toString())
        val file = ctx.vertx().fileSystem().openAwait(
            filePath.toString(),
            OpenOptions()
        )
        ctx.request().resume()
        ctx.request().pipeToAwait(file)

        withContext(Dispatchers.IO) {
            val sirixDBUser = SirixDBUser.create(ctx)
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

            database.use {
                val resConfig =
                    ResourceConfiguration.Builder(resPathName).build()
                createOrRemoveAndCreateResource(
                    database,
                    resConfig,
                    resPathName,
                    dispatcher
                )
                val manager = database.openResourceManager(resPathName)

                manager.use {
                    val pathToFile = filePath.toPath()
                    val maxNodeKey = insertJsonSubtreeAsFirstChild(manager, pathToFile.toAbsolutePath())

                    ctx.vertx().fileSystem().deleteAwait(pathToFile.toAbsolutePath().toString())

                    if (maxNodeKey < 5000) {
                        serializeJson(manager, ctx)
                    } else {
                        ctx.response().setStatusCode(200).end()
                    }
                }
            }
        }
    }

    private suspend fun serializeJson(
        manager: JsonResourceManager,
        routingCtx: RoutingContext
    ) {
        withContext(Dispatchers.IO) {
            val out = StringWriter()
            val serializerBuilder = JsonSerializer.newBuilder(manager, out)
            val serializer = serializerBuilder.build()

            JsonSerializeHelper().serialize(
                serializer,
                out,
                routingCtx,
                manager,
                null
            )
        }
    }

    private suspend fun createDatabaseIfNotExists(
        dbFile: Path,
        context: Context
    ): DatabaseConfiguration? {
        return context.executeBlockingAwait { promise: Promise<DatabaseConfiguration> ->
            val dbExists = Files.exists(dbFile)

            if (!dbExists) {
                Files.createDirectories(dbFile.parent)
            }

            val dbConfig = DatabaseConfiguration(dbFile)

            if (!Databases.existsDatabase(dbFile)) {
                Databases.createJsonDatabase(dbConfig)
            }

            promise.complete(dbConfig)
        }
    }

    private suspend fun createOrRemoveAndCreateResource(
        database: Database<JsonResourceManager>,
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

    private suspend fun insertJsonSubtreeAsFirstChild(
        manager: JsonResourceManager,
        resFileToStore: Path
    ): Long {
        return withContext(Dispatchers.IO) {
            val wtx = manager.beginNodeTrx()
            return@withContext wtx.use {
                val eventReader = JsonShredder.createFileReader(resFileToStore)
                eventReader.use {
                    wtx.insertSubtreeAsFirstChild(eventReader)
                }
                return@use wtx.maxNodeKey
            }
        }
    }
}
