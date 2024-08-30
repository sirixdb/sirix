package io.sirix.rest.crud.json

import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.User
import io.sirix.access.trx.node.HashType
import io.sirix.api.Database
import io.sirix.api.json.JsonResourceSession
import io.sirix.rest.KotlinJsonStreamingShredder
import io.sirix.rest.crud.AbstractCreateHandler
import io.sirix.rest.crud.Revisions
import io.sirix.rest.crud.SirixDBUser
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.json.shredder.JsonShredder
import java.io.StringWriter
import java.nio.file.Path


class JsonCreate(
    location: Path,
    createMultipleResources: Boolean = false
) : AbstractCreateHandler<JsonResourceSession>(location, createMultipleResources) {
    override suspend fun insertResource(
        dbFile: Path?, resPathName: String,
        ctx: RoutingContext
    ) {
        ctx.request().pause()

        withContext(Dispatchers.IO) {
            var body: String? = null
            val sirixDBUser = SirixDBUser.create(ctx)
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)
            val dispatcher = ctx.vertx().dispatcher()

            database.use {
                val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
                val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                val resConfig =
                    ResourceConfiguration.Builder(resPathName).useDeweyIDs(true)
                        .hashKind(HashType.valueOf(hashType.uppercase()))
                        .customCommitTimestamps(commitTimestampAsString != null)
                        .build()

                createOrRemoveAndCreateResource(database, resConfig, resPathName, dispatcher)

                val manager = database.beginResourceSession(resPathName)

                manager.use {
                    val maxNodeKey = insertJsonSubtreeAsFirstChild(manager, ctx)

                    if (maxNodeKey < MAX_NODES_TO_SERIALIZE) {
                        body = serializeResource(manager, ctx)
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

    override fun serializeResource(
        manager: JsonResourceSession,
        routingContext: RoutingContext
    ): String {
        val out = StringWriter()
        val serializerBuilder = JsonSerializer.newBuilder(manager, out)
        val serializer = serializerBuilder.build()

        return JsonSerializeHelper().serialize(
            serializer,
            out,
            routingContext,
            manager,
            intArrayOf(1),
            null
        )
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

    override fun insertResourceSubtreeAsFirstChild(
        manager: JsonResourceSession,
        filePath: Path,
        ctx: RoutingContext
    ): Long {
        val wtx = manager.beginNodeTrx()
        return wtx.use {
            val eventReader = JsonShredder.createFileReader(filePath)
            eventReader.use {
                wtx.insertSubtreeAsFirstChild(eventReader)
            }
            wtx.maxNodeKey
        }
    }

    override suspend fun openDatabase(dbFile: Path, sirixDBUser: User): Database<JsonResourceSession> {
        return Databases.openJsonDatabase(dbFile, sirixDBUser)
    }

    override fun createDatabase(dbConfig: DatabaseConfiguration?) {
        Databases.createJsonDatabase(dbConfig)
    }
}
