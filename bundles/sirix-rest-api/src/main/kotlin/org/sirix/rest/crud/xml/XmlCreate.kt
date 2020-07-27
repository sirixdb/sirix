package org.sirix.rest.crud.xml

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.file.OpenOptions
import io.vertx.core.file.impl.FileResolver
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.core.executeBlockingAwait
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
import org.sirix.api.xml.XmlResourceManager
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.service.xml.shredder.XmlShredder
import java.io.ByteArrayOutputStream
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

class XmlCreate(private val location: Path, private val createMultipleResources: Boolean = false) {
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
        }

        if (createMultipleResources) {
            createMultipleResources(databaseName, ctx)
            ctx.response().setStatusCode(201).end()
            return ctx.currentRoute()
        }

        shredder(databaseName, resource, ctx)

        return ctx.currentRoute()
    }

    private suspend fun createMultipleResources(databaseName: String, ctx: RoutingContext) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext
        val dispatcher = ctx.vertx().dispatcher()
        createDatabaseIfNotExists(dbFile, context)

        val sirixDBUser = SirixDBUser.create(ctx)
        val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

        database.use {
            BodyHandler.create().handle(ctx)
            val fileResolver = FileResolver()
            ctx.fileUploads().forEach { fileUpload ->
                val fileName = fileUpload.fileName()
                val resConfig = ResourceConfiguration.Builder(fileName).build()

                createOrRemoveAndCreateResource(database, resConfig, fileName, dispatcher)

                val manager = database.openResourceManager(fileName)

                manager.use {
                    insertXmlSubtreeAsFirstChild(manager, fileResolver.resolveFile(fileUpload.uploadedFileName()).toPath())
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

        val filePath = withContext(Dispatchers.IO) {
            fileResolver.resolveFile(Files.createTempFile(UUID.randomUUID().toString(), null).toString())
        }

        val file = ctx.vertx().fileSystem().openAwait(filePath.toString(),
            OpenOptions()
        )
        ctx.request().resume()
        ctx.request().pipeToAwait(file)

        withContext(Dispatchers.IO) {
            val sirixDBUser = SirixDBUser.create(ctx)
            val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

            database.use {
                val resConfig = ResourceConfiguration.Builder(resPathName).build()
                createOrRemoveAndCreateResource(database, resConfig, resPathName, dispatcher)
                val manager = database.openResourceManager(resPathName)

                manager.use {
                    val pathToFile = filePath.toPath()
                    val maxNodeKey = insertXmlSubtreeAsFirstChild(manager, pathToFile.toAbsolutePath())

                    ctx.vertx().fileSystem().deleteAwait(filePath.toString())

                    if (maxNodeKey < 5000) {
                        serializeXml(manager, ctx)
                    } else {
                        ctx.response().setStatusCode(200).end()
                    }
                }
            }
        }
    }

    private suspend fun serializeXml(
        manager: XmlResourceManager,
        routingCtx: RoutingContext
    ) {
        withContext(Dispatchers.IO) {
            val out = ByteArrayOutputStream()
            val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out)
            val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

            XmlSerializeHelper().serializeXml(serializer, out, routingCtx, manager, null)
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
                Databases.createXmlDatabase(dbConfig)
            }

            promise.complete(dbConfig)
        }
    }

    private suspend fun createOrRemoveAndCreateResource(
        database: Database<XmlResourceManager>,
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

    private suspend fun insertXmlSubtreeAsFirstChild(
            manager: XmlResourceManager,
            resFileToStore: Path
    ) : Long {
        return withContext(Dispatchers.IO) {
            val wtx = manager.beginNodeTrx()
            return@withContext wtx.use {
                val inputStream = FileInputStream(resFileToStore.toFile())
                return@use inputStream.use {
                    val eventStream = XmlShredder.createFileReader(inputStream)
                    wtx.insertSubtreeAsFirstChild(eventStream)
                    eventStream.close()
                    wtx.maxNodeKey
                }
            }
        }
    }
}
