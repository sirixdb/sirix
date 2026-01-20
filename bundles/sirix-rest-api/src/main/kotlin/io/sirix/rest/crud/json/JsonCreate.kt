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
        // Request is already paused by AbstractCreateHandler.shredder

        withContext(Dispatchers.IO) {
            var body: String? = null
            val sirixDBUser = SirixDBUser.create(ctx)
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)
            val dispatcher = ctx.vertx().dispatcher()

            database.use {
                val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
                val hashType = ctx.queryParam("hashType").getOrNull(0) ?: "NONE"
                val useDeweyIDs = ctx.queryParam("useDeweyIDs").getOrNull(0)?.toBoolean() ?: false
                val resConfig =
                    ResourceConfiguration.Builder(resPathName).useDeweyIDs(useDeweyIDs)
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
        
        // Auto-commit every N nodes to keep memory bounded during large imports
        // Default: ~4.5M nodes - same as JsonShredderTest for Chicago dataset
        // Smaller values cause frequent commits which trigger cache eviction blocking
        val maxNodes = ctx.queryParam("maxNodes").getOrNull(0)?.toIntOrNull() ?: ((262_144 shl 4) + 262_144)

        val wtx = manager.beginNodeTrx(maxNodes)
        return wtx.use {
            // #region debug trace
            debugLog("insertJsonSubtreeAsFirstChild_entry") { mapOf("maxNodes" to maxNodes) }
            // #endregion
            
            // Switch to Vert.x event loop to set up parser handlers synchronously
            // This is critical: handlers MUST be attached BEFORE request.resume()
            withContext(ctx.vertx().dispatcher()) {
                val jsonParser = JsonParser.newParser(ctx.request())
                val shredder = KotlinJsonStreamingShredder(wtx, jsonParser, ctx.vertx(), ctx.request())
                // call() sets up handlers synchronously and returns a Future
                val future = shredder.call()
                // Resume request AFTER handlers are attached
                ctx.request().resume()
                // Await the future (suspends until processing is complete)
                future.coAwait()
            }
            // #region debug trace
            debugLog("shredder_completed")
            // #endregion
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
