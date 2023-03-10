package org.sirix.rest.crud.json

import io.vertx.core.file.OpenOptions
import io.vertx.core.file.impl.FileResolver
import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirix.access.Databases
import org.sirix.access.DatabasesInternals
import org.sirix.access.ResourceConfiguration
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceSession
import org.sirix.rest.KotlinJsonStreamingShredder
import org.sirix.rest.crud.Revisions
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.json.shredder.JsonShredder
import org.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.io.StringWriter
import java.nio.file.Path


private const val MAX_NODES_TO_SERIALIZE = 5000

private val logger = LogWrapper(LoggerFactory.getLogger(DatabasesInternals::class.java))


class JsonCreate2 {
    suspend fun createMultipleResources(location:Path, databaseName:String, ctx: RoutingContext) {
        val dbFile = location.resolve(databaseName)
        val context = ctx.vertx().orCreateContext

        val sirixDBUser = SirixDBUser.create(ctx)

        ctx.vertx().executeBlocking<Unit> {
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

            database.use {
                BodyHandler.create().handle(ctx)
                val fileResolver = FileResolver()
                val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                ctx.fileUploads().forEach { fileUpload ->
                    val fileName = fileUpload.fileName()
                    val resConfig = ResourceConfiguration.Builder(fileName).useDeweyIDs(true).hashKind(
                            HashType.valueOf(hashType.uppercase())
                    ).build()

                    val resourceHasBeenCreated = createResourceIfNotExisting(
                            database,
                            resConfig
                    )

                    if (resourceHasBeenCreated) {
                        val manager = database.beginResourceSession(fileName)

                        manager.use {
                            insertJsonSubtreeAsFirstChild(
                                    manager,
                                    fileResolver.resolveFile(fileUpload.uploadedFileName()).toPath()
                            )
                        }
                    }
                }
            }
        }.await()

        ctx.response().setStatusCode(201).end()
    }
    private fun createResourceIfNotExisting(
            database: Database<JsonResourceSession>,
            resConfig: ResourceConfiguration?,
    ): Boolean {
        logger.debug("Try to create resource: $resConfig")
        return database.createResource(resConfig)
    }


    public suspend fun insertResource(
            dbFile: Path?, resPathName: String,
            ctx: RoutingContext
    ) {
        ctx.request().pause()
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
                val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
                val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                val resConfig =
                        ResourceConfiguration.Builder(resPathName).useDeweyIDs(true)
                                .hashKind(HashType.valueOf(hashType.uppercase()))
                                .customCommitTimestamps(commitTimestampAsString != null)
                                .build()

                val resourceHasBeenCreated = createResourceIfNotExisting(
                        database,
                        resConfig
                )

                val manager = database.beginResourceSession(resPathName)

                manager.use {
                    val maxNodeKey = if (resourceHasBeenCreated) {
                        insertJsonSubtreeAsFirstChild(manager, ctx)
                    } else {
                        val rtx = manager.beginNodeReadOnlyTrx()

                        rtx.use {
                            return@use rtx.maxNodeKey
                        }
                    }
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

    private suspend fun insertJsonSubtreeAsFirstChild(
            manager: JsonResourceSession,
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
            val jsonParser = JsonParser.newParser(ctx.request())
            val future = KotlinJsonStreamingShredder(wtx, jsonParser).call()
            ctx.request().resume()
            future.await()
            wtx.commit(commitMessage, commitTimestamp)
            return@use wtx.maxNodeKey
        }
    }

    private fun insertJsonSubtreeAsFirstChild(
            manager: JsonResourceSession,
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

    private fun serializeJson(
            manager: JsonResourceSession,
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

}