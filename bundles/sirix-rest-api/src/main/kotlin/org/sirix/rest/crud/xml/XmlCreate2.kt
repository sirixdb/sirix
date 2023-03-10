package org.sirix.rest.crud.xml

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.file.OpenOptions
import io.vertx.core.file.impl.FileResolver
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.xml.XmlNodeTrx
import org.sirix.api.xml.XmlResourceSession
import org.sirix.rest.crud.Revisions
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.service.xml.shredder.XmlShredder
import java.io.ByteArrayOutputStream
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlinx.coroutines.withContext

class XmlCreate2 {
    suspend fun createMultipleResources(location: Path, databaseName: String, ctx: RoutingContext) {
        val dbFile = location.resolve(databaseName)
        val dispatcher = ctx.vertx().dispatcher()

        val sirixDBUser = SirixDBUser.create(ctx)

        withContext(Dispatchers.IO) {
            val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

            database.use {
                BodyHandler.create().handle(ctx)
                val fileResolver = FileResolver()
                ctx.fileUploads().forEach { fileUpload ->
                    val fileName = fileUpload.fileName()
                    val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                    val resConfig =
                            ResourceConfiguration.Builder(fileName).useDeweyIDs(true)
                                    .hashKind(HashType.valueOf(hashType.uppercase())).build()

                    createOrRemoveAndCreateResource(database, resConfig, fileName, dispatcher)

                    val manager = database.beginResourceSession(fileName)

                    manager.use {
                        insertXmlSubtreeAsFirstChild(
                                manager,
                                fileResolver.resolveFile(fileUpload.uploadedFileName()).toPath(),
                                ctx
                        )
                    }
                }
            }

            ctx.response().setStatusCode(200).end()
        }
    }

    suspend fun insertResource(
                dbFile: Path?, resPathName: String,
                dispatcher: CoroutineDispatcher,
                ctx: RoutingContext
        ) {
            ctx.request().pause()
            val fileResolver = FileResolver()

            val filePath = withContext(Dispatchers.IO) {
                fileResolver.resolveFile(Files.createTempFile(UUID.randomUUID().toString(), null).toString())
            }

            val file = ctx.vertx().fileSystem().open(
                    filePath.toString(),
                    OpenOptions()
            ).await()
            ctx.request().resume()
            ctx.request().pipeTo(file).await()

            withContext(Dispatchers.IO) {
                var body: String? = null
                val sirixDBUser = SirixDBUser.create(ctx)
                val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

                database.use {
                    val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                    val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
                    val resConfig =
                            ResourceConfiguration.Builder(resPathName).hashKind(HashType.valueOf(hashType.uppercase()))
                                    .customCommitTimestamps(commitTimestampAsString != null).build()
                    createOrRemoveAndCreateResource(database, resConfig, resPathName, dispatcher)
                    val manager = database.beginResourceSession(resPathName)

                    manager.use {
                        val pathToFile = filePath.toPath()
                        val maxNodeKey = insertXmlSubtreeAsFirstChild(manager, pathToFile.toAbsolutePath(), ctx)

                        ctx.vertx().fileSystem().delete(filePath.toString()).await()

                        if (maxNodeKey < 5000) {
                            body = serializeXml(manager, ctx)
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
    private fun serializeXml(
            manager: XmlResourceSession,
            routingCtx: RoutingContext
    ): String {
        val out = ByteArrayOutputStream()
        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out)
        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        return XmlSerializeHelper().serializeXml(serializer, out, routingCtx, manager, null)
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
                Databases.createXmlDatabase(dbConfig)
            }

            promise.complete(dbConfig)
        }.await()
    }

    private suspend fun createOrRemoveAndCreateResource(
            database: Database<XmlResourceSession>,
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

    private fun insertXmlSubtreeAsFirstChild(
            manager: XmlResourceSession,
            resFileToStore: Path,
            ctx: RoutingContext
    ): Long {
        val commitMessage = ctx.queryParam("commitMessage").getOrNull(0)
        val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
        val commitTimestamp = if (commitTimestampAsString == null) {
            null
        } else {
            Revisions.parseRevisionTimestamp(commitTimestampAsString).toInstant()
        }

        val wtx = manager.beginNodeTrx()
        return wtx.use {
            val inputStream = FileInputStream(resFileToStore.toFile())
            return@use inputStream.use {
                val eventStream = XmlShredder.createFileReader(inputStream)
                wtx.insertSubtreeAsFirstChild(eventStream, XmlNodeTrx.Commit.No)
                eventStream.close()
                wtx.commit(commitMessage, commitTimestamp)
                wtx.maxNodeKey
            }
        }
    }

}