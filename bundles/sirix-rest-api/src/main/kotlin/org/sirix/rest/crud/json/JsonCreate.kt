package org.sirix.rest.crud.json

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.file.impl.FileResolver
import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.core.http.endAwait
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.DatabasesInternals
import org.sirix.access.ResourceConfiguration
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.rest.KotlinJsonStreamingShredder
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.json.shredder.JsonShredder
import org.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path

private const val MAX_NODES_TO_SERIALIZE = 5000

private val LOGGER = LogWrapper(LoggerFactory.getLogger(DatabasesInternals::class.java))

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
            throw IllegalArgumentException("Database name and resource data to store not given.")
        } else {
            if (createMultipleResources) {
                createMultipleResources(databaseName, ctx)
                ctx.response().setStatusCode(201).end()
                return ctx.currentRoute()
            }

            shredder(databaseName, resource, ctx)
        }

        return ctx.currentRoute()
    }

    private suspend fun createMultipleResources(
        databaseName: String,
        ctx: RoutingContext
    ) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        createDatabaseIfNotExists(dbFile, context)

        val sirixDBUser = SirixDBUser.create(ctx)

        withContext(Dispatchers.IO) {
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

            database.use {
                BodyHandler.create().handle(ctx)
                val fileResolver = FileResolver()
                ctx.fileUploads().forEach { fileUpload ->
                    val fileName = fileUpload.fileName()
                    val resConfig = ResourceConfiguration.Builder(fileName).useDeweyIDs(true).build()

                    createOrRemoveAndCreateResource(
                        database,
                        resConfig,
                        fileName
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

            ctx.response().setStatusCode(200).end()
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

    private suspend fun insertResource(
        dbFile: Path?, resPathName: String,
        ctx: RoutingContext
    ) {
//        ctx.request().pause()
//        val fileResolver = FileResolver()
//
//        val filePath = withContext(Dispatchers.IO) {
//            fileResolver.resolveFile(Files.createTempFile(UUID.randomUUID().toString(), null).toString())
//        }
//
//        val file = ctx.vertx().fileSystem().open(
//            filePath.toString(),
//            OpenOptions()
//        ).await()
//
//        ctx.request().resume()
//        ctx.request().pipeTo(file).await()

        withContext(Dispatchers.IO) {
            var body: String? = null
            val sirixDBUser = SirixDBUser.create(ctx)
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

            database.use {
                val resConfig =
                    ResourceConfiguration.Builder(resPathName).useDeweyIDs(true).build()

                createOrRemoveAndCreateResource(
                    database,
                    resConfig,
                    resPathName
                )

                val manager = database.openResourceManager(resPathName)

                manager.use {
                    ctx.request().pause()
                    val jsonParser = JsonParser.newParser(ctx.request())
                    val maxNodeKey = insertJsonSubtreeAsFirstChild(manager, jsonParser)
                    ctx.request().resume()
                    ctx.request().end().await()
//                    ctx.vertx().fileSystem().delete(pathToFile.toAbsolutePath().toString()).await()

                    if (maxNodeKey < MAX_NODES_TO_SERIALIZE) {
                        body = serializeJson(manager, ctx)
                    } else {
                        ctx.response().setStatusCode(200)
                    }
                }
            }

            if (body != null) {
                ctx.response().end(body)
            } else {
                ctx.response().end()
            }
        }
    }

    private fun serializeJson(
        manager: JsonResourceManager,
        routingCtx: RoutingContext
    ): String {
        val out = StringWriter()
        val serializerBuilder = JsonSerializer.newBuilder(manager, out)
        val serializer = serializerBuilder.build()

        return JsonSerializeHelper().serialize(
            serializer,
            out,
            routingCtx,
            manager,
            intArrayOf(1),
            null
        )
    }

    private suspend fun createDatabaseIfNotExists(
        dbFile: Path,
        context: Context
    ): DatabaseConfiguration? {
        return context.executeBlocking { promise: Promise<DatabaseConfiguration> ->
            val dbExists = Files.exists(dbFile)

            if (!dbExists) {
                Files.createDirectories(dbFile.parent)
            }

            val dbConfig = DatabaseConfiguration(dbFile)

            if (!Databases.existsDatabase(dbFile)) {
                Databases.createJsonDatabase(dbConfig)
            }

            promise.complete(dbConfig)
        }.await()
    }

    private fun createOrRemoveAndCreateResource(
        database: Database<JsonResourceManager>,
        resConfig: ResourceConfiguration?,
        resPathName: String
    ) {
        LOGGER.debug("Create resource: ${resConfig}")
        if (!database.createResource(resConfig)) {
            LOGGER.debug("Remove resource: ${resConfig}")
            database.removeResource(resPathName)
            LOGGER.debug("Create resource: ${resConfig}")
            database.createResource(resConfig)
        }
    }

    private fun insertJsonSubtreeAsFirstChild(
        manager: JsonResourceManager,
        jsonParser: JsonParser
    ): Long {
        val wtx = manager.beginNodeTrx()
        return wtx.use {
            KotlinJsonStreamingShredder(wtx, jsonParser).call()
            wtx.maxNodeKey
        }
    }

    private fun insertJsonSubtreeAsFirstChild(
        manager: JsonResourceManager,
        resFileToStore: Path
    ): Long {
        val wtx = manager.beginNodeTrx()
        return wtx.use {
            val eventReader = JsonShredder.createFileReader(resFileToStore)
            eventReader.use {
                wtx.insertSubtreeAsFirstChild(eventReader)
            }
            wtx.maxNodeKey
        }
    }
}
