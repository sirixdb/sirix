package io.sirix.rest.crud.json

import io.vertx.core.parsetools.JsonParser
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import io.sirix.access.DatabaseConfiguration
import io.sirix.access.Databases
import io.sirix.access.ResourceConfiguration
import io.sirix.access.User
import io.sirix.access.ValidTimeConfig
import io.sirix.access.trx.node.HashType
import io.sirix.api.Database
import io.sirix.api.json.JsonResourceSession
import io.sirix.rest.KotlinJsonStreamingShredder
import io.sirix.rest.crud.AbstractCreateHandler
import io.sirix.rest.crud.Revisions
import io.sirix.rest.crud.SirixDBUser
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.json.shredder.JsonShredder
import java.io.File
import java.io.StringWriter
import java.nio.file.Path


class JsonCreate(
    location: Path,
    createMultipleResources: Boolean = false
) : AbstractCreateHandler<JsonResourceSession>(location, createMultipleResources) {
    
    companion object {
        /**
         * Enable debug tracing via system property: -Dsirix.shredder.debug=true
         * Or environment variable: SIRIX_SHREDDER_DEBUG=true
         */
        private val DEBUG_ENABLED: Boolean = System.getProperty("sirix.shredder.debug")?.toBoolean()
            ?: System.getenv("SIRIX_SHREDDER_DEBUG")?.toBoolean()
            ?: false
        
        private val DEBUG_LOG_PATH: String = System.getProperty("sirix.shredder.debug.logfile")
            ?: System.getenv("SIRIX_SHREDDER_DEBUG_LOGFILE")
            ?: "sirix-shredder-debug.log"
        
        private val logFile by lazy { if (DEBUG_ENABLED) File(DEBUG_LOG_PATH) else null }
        
        private inline fun debugLog(msg: String, data: () -> Map<String, Any?> = { emptyMap() }) {
            if (!DEBUG_ENABLED) return
            try {
                val dataMap = data()
                val json = """{"message":"$msg","data":${dataMap.entries.joinToString(",", "{", "}") { "\"${it.key}\":${when(val v = it.value) { is String -> "\"$v\""; null -> "null"; else -> v.toString() }}"}}, "timestamp":${System.currentTimeMillis()}}"""
                logFile?.appendText(json + "\n")
                System.err.println("DEBUG: $msg $dataMap")
            } catch (e: Exception) { System.err.println("DEBUG LOG FAILED: ${e.message}") }
        }
    }
    
    override suspend fun insertResource(
        dbFile: Path?, resPathName: String,
        ctx: RoutingContext
    ) {
        // #region debug trace
        debugLog("insertResource_entry") { mapOf("dbFile" to dbFile.toString(), "resPathName" to resPathName) }
        // #endregion
        withContext(Dispatchers.IO) {
            var body: String? = null
            val sirixDBUser = SirixDBUser.create(ctx)
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)
            val dispatcher = ctx.vertx().dispatcher()

            database.use {
                val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
                val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                val useDeweyIDs = ctx.queryParam("useDeweyIDs").getOrNull(0)?.toBoolean() ?: false

                val validFromPath = ctx.queryParam("validFromPath").getOrNull(0)
                val validToPath = ctx.queryParam("validToPath").getOrNull(0)
                val useConventionalValidTime = ctx.queryParam("useConventionalValidTime").getOrNull(0)?.toBoolean() ?: false

                val validTimeConfig = when {
                    useConventionalValidTime -> ValidTimeConfig.withConventionalPaths()
                    validFromPath != null && validToPath != null -> ValidTimeConfig.withPaths(validFromPath, validToPath)
                    else -> null
                }

                val resConfigBuilder = ResourceConfiguration.Builder(resPathName)
                    .useDeweyIDs(useDeweyIDs)
                    .hashKind(HashType.valueOf(hashType.uppercase()))
                    .customCommitTimestamps(commitTimestampAsString != null)

                if (validTimeConfig != null) {
                    resConfigBuilder.validTimeConfig(validTimeConfig)
                }

                val resConfig = resConfigBuilder.build()

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

        val maxNodes = ctx.queryParam("maxNodes").getOrNull(0)?.toIntOrNull() ?: ((262_144 shl 4) + 262_144)

        // Buffer the request body, then parse synchronously on a worker thread.
        // The cross-thread LinkedBlockingQueue producer/consumer pattern deadlocks
        // in GraalVM native image (Condition.signal() doesn't wake the consumer).
        val body = ctx.request().body().coAwait()

        val wtx = manager.beginNodeTrx(maxNodes)
        return wtx.use {
            ctx.vertx().executeBlocking<Long> {
                val jsonParser = JsonParser.newParser()
                val shredder = KotlinJsonStreamingShredder(wtx, jsonParser, null, null)
                shredder.call()
                jsonParser.write(body)
                jsonParser.end()
                wtx.commit(commitMessage, commitTimestamp)
                wtx.maxNodeKey
            }.coAwait()
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
