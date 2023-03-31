package org.sirix.rest.crud.json

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseConfiguration
import org.sirix.access.Databases
import org.sirix.access.ResourceConfiguration
import org.sirix.access.User
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceSession
import org.sirix.rest.KotlinJsonStreamingShredder
import org.sirix.rest.crud.AbstractCreateHandler
import org.sirix.rest.crud.Revisions
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.json.shredder.JsonShredder
import java.io.StringWriter
import java.nio.file.Path

private const val MAX_NODES_TO_SERIALIZE = 5000

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

    override suspend fun createDatabaseIfNotExists(
        dbFile: Path,
        context: Context
    ): DatabaseConfiguration? {
        val dbConfig = prepareDatabasePath(dbFile, context)
        return context.executeBlocking { promise: Promise<DatabaseConfiguration> ->
            if (!Databases.existsDatabase(dbFile)) {
                Databases.createJsonDatabase(dbConfig)
            }

            promise.complete(dbConfig)
        }.await()
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
}
