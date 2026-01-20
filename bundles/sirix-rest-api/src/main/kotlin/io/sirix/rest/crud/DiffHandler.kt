package io.sirix.rest.crud

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import io.sirix.access.DatabaseType
import io.sirix.access.Databases.*
import io.sirix.access.DatabasesInternals
import io.sirix.access.ResourceConfiguration
import io.sirix.api.Database
import io.sirix.api.json.JsonNodeReadOnlyTrx
import io.sirix.api.json.JsonResourceSession
import io.sirix.service.json.BasicJsonDiff
import io.sirix.utils.LogWrapper
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/**
 * [LogWrapper] reference.
 */
private val logger =
    LogWrapper(LoggerFactory.getLogger(DiffHandler::class.java))

class DiffHandler(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName = ctx.pathParam("database")
        val resourceName = ctx.pathParam("resource")

        if (databaseName == null || resourceName == null) {
            throw IllegalArgumentException("Database name and resource name must be in the URL path.")
        }

        logger.debug("Open databases before: ${DatabasesInternals.getOpenDatabases()}")
        
        val database = openDatabase(databaseName)

        val diff = context.executeBlocking {
            var diffString: String? = null
            database.use {
                val resourceManager = database.beginResourceSession(resourceName)

                resourceManager.use {
                    if (resourceManager is JsonResourceSession) {
                        val firstRevision: String? = ctx.queryParam("first-revision").getOrNull(0)
                        val secondRevision: String? = ctx.queryParam("second-revision").getOrNull(0)

                        if (firstRevision == null || secondRevision == null) {
                            throw IllegalArgumentException("First and second revision must be specified.")
                        }

                        val startNodeKey: String? = ctx.queryParam("startNodeKey").getOrNull(0)
                        val maxDepth: String? = ctx.queryParam("maxDepth").getOrNull(0)
                        // include-data controls whether full subtree data is included for inserts
                        // Default is false (compact mode) for scalability with large resources
                        val includeData: Boolean = ctx.queryParam("include-data").getOrNull(0)?.toBoolean() ?: false

                        val startNodeKeyAsLong = startNodeKey?.let { startNodeKey.toLong() } ?: 0
                        val maxDepthAsLong = maxDepth?.let { maxDepth.toLong() } ?: Long.MAX_VALUE

                        // For consecutive revisions without filtering, try to read pre-computed diff file
                        if (secondRevision.toInt() - 1 == firstRevision.toInt() && startNodeKey == null && maxDepth == null) {
                            val diffPath = resourceManager.getResourceConfig()
                                .resource
                                .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.path)
                                .resolve("diffFromRev${firstRevision.toInt()}toRev${secondRevision.toInt()}.json")

                            if (Files.exists(diffPath) && !includeData) {
                                // Fast path: read pre-computed diff file directly (no data for jsonFragments)
                                logger.debug("Reading pre-computed diff from: $diffPath")
                                diffString = Files.readString(diffPath)
                            } else if (Files.exists(diffPath) && includeData && resourceManager.resourceConfig.areDeweyIDsStored) {
                                // includeData=true: use update operations to lazily serialize jsonFragment data
                                val rtx = resourceManager.beginNodeReadOnlyTrx(secondRevision.toInt())
                                rtx.use {
                                    diffString = useUpdateOperations(
                                        rtx,
                                        startNodeKeyAsLong,
                                        databaseName,
                                        resourceName,
                                        firstRevision,
                                        secondRevision,
                                        maxDepthAsLong
                                    )
                                }
                            } else if (resourceManager.resourceConfig.areDeweyIDsStored) {
                                // Use update operations if DeweyIDs are stored but file doesn't exist
                                val rtx = resourceManager.beginNodeReadOnlyTrx(secondRevision.toInt())
                                rtx.use {
                                    diffString = useUpdateOperations(
                                        rtx,
                                        startNodeKeyAsLong,
                                        databaseName,
                                        resourceName,
                                        firstRevision,
                                        secondRevision,
                                        maxDepthAsLong
                                    )
                                }
                            } else {
                                // Fall back to computing diff
                                diffString = BasicJsonDiff(databaseName).generateDiff(
                                    resourceManager,
                                    firstRevision.toInt(),
                                    secondRevision.toInt(),
                                    startNodeKeyAsLong,
                                    maxDepthAsLong,
                                    includeData
                                )
                            }
                        } else if (resourceManager.resourceConfig.areDeweyIDsStored && secondRevision.toInt() - 1 == firstRevision.toInt()) {
                            // Consecutive revisions with filtering - use update operations
                            val rtx = resourceManager.beginNodeReadOnlyTrx(secondRevision.toInt())
                            rtx.use {
                                diffString = useUpdateOperations(
                                    rtx,
                                    startNodeKeyAsLong,
                                    databaseName,
                                    resourceName,
                                    firstRevision,
                                    secondRevision,
                                    maxDepthAsLong
                                )
                            }
                        } else {
                            // Non-consecutive revisions or no DeweyIDs with filtering
                            diffString = BasicJsonDiff(databaseName).generateDiff(
                                resourceManager,
                                firstRevision.toInt(),
                                secondRevision.toInt(),
                                startNodeKeyAsLong,
                                maxDepthAsLong,
                                includeData
                            )
                        }
                    }
                }
            }

            diffString
        }.coAwait()

        logger.debug("Open databases after: ${DatabasesInternals.getOpenDatabases()}")

        val res = ctx.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .putHeader(
                HttpHeaders.CONTENT_LENGTH,
                diff!!.toByteArray(StandardCharsets.UTF_8).size.toString()
            )
        res.write(diff)
        res.end()

        return ctx.currentRoute()
    }

    private fun useUpdateOperations(
        rtx: JsonNodeReadOnlyTrx,
        startNodeKeyAsLong: Long,
        databaseName: String,
        resourceName: String,
        firstRevision: String,
        secondRevision: String,
        maxDepthAsLong: Long
    ): String {
        rtx.moveTo(startNodeKeyAsLong)
        val metaInfo = createMetaInfo(
            databaseName,
            resourceName,
            firstRevision.toInt(),
            secondRevision.toInt()
        )

        val diffs = metaInfo.getAsJsonArray("diffs")
        val updateOperations =
            rtx.getUpdateOperationsInSubtreeOfNode(rtx.deweyID, maxDepthAsLong)
        updateOperations.forEach { diffs.add(it) }
        return metaInfo.toString()
    }

    private fun openDatabase(databaseName: String): Database<*> {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        return when (getDatabaseType(location.resolve(databaseName).toAbsolutePath())) {
            DatabaseType.JSON -> openJsonDatabase(location.resolve(databaseName))
            DatabaseType.XML -> openXmlDatabase(location.resolve(databaseName))
        }
    }

    private fun createMetaInfo(
        databaseName: String, resourceName: String, oldRevision: Int,
        newRevision: Int
    ): JsonObject {
        val json = JsonObject()
        json.addProperty("database", databaseName)
        json.addProperty("resource", resourceName)
        json.addProperty("old-revision", oldRevision)
        json.addProperty("new-revision", newRevision)
        val diffsArray = JsonArray()
        json.add("diffs", diffsArray)
        return json
    }
}
