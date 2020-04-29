package org.sirix.rest.crud

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.DatabaseType
import org.sirix.access.Databases.*
import org.sirix.access.ResourceConfiguration
import org.sirix.api.Database
import org.sirix.api.json.JsonNodeReadOnlyTrx
import org.sirix.api.json.JsonResourceManager
import org.sirix.service.json.BasicJsonDiff
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.Consumer

class DiffHandler(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName = ctx.pathParam("database")
        val resourceName = ctx.pathParam("resource")

        if (databaseName == null || resourceName == null) {
            ctx.fail(IllegalArgumentException("Database name and resource name must be in the URL path."))
            return ctx.currentRoute()
        }

        val database = openDatabase(databaseName)

        database.use {
            val resourceManager = database.openResourceManager(resourceName)

            resourceManager.use {
                if (resourceManager is JsonResourceManager) {
                    val diff = context.executeBlockingAwait<String> { resultPromise ->
                        val firstRevision: String? = ctx.queryParam("first-revision").getOrNull(0)
                        val secondRevision: String? = ctx.queryParam("second-revision").getOrNull(0)

                        if (firstRevision == null || secondRevision == null) {
                            ctx.fail(IllegalArgumentException("First and second revision must be specified."))
                            return@executeBlockingAwait
                        }

                        val startNodeKey: String? = ctx.queryParam("startNodeKey").getOrNull(0)
                        val maxDepth: String? = ctx.queryParam("maxDepth").getOrNull(0)

                        val startNodeKeyAsLong = startNodeKey?.let { startNodeKey.toLong() } ?: 0
                        val maxDepthAsLong = maxDepth?.let { maxDepth.toLong() } ?: Long.MAX_VALUE

                        if (resourceManager.resourceConfig.areDeweyIDsStored && secondRevision.toInt() - 1 == firstRevision.toInt()) {
                            if (startNodeKeyAsLong == 0L && maxDepthAsLong == 0L) {
                                val diffPath = resourceManager.getResourceConfig()
                                    .resource
                                    .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.path)
                                    .resolve("diffFromRev${firstRevision.toInt()}toRev${secondRevision.toInt()}.json")

                                resultPromise.complete(Files.readString(diffPath))
                            } else {
                                val rtx = resourceManager.beginNodeReadOnlyTrx(secondRevision.toInt())

                                rtx.use {
                                    useUpdateOperations(
                                        rtx,
                                        startNodeKeyAsLong,
                                        databaseName,
                                        resourceName,
                                        firstRevision,
                                        secondRevision,
                                        maxDepthAsLong,
                                        resultPromise
                                    )
                                }
                            }
                        } else {
                            resultPromise.complete(
                                BasicJsonDiff().generateDiff(
                                    resourceManager,
                                    firstRevision.toInt(),
                                    secondRevision.toInt(),
                                    startNodeKeyAsLong,
                                    maxDepthAsLong
                                )
                            )
                        }
                    }

                    ctx.response().setStatusCode(200)
                        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                        .putHeader(
                            HttpHeaders.CONTENT_LENGTH,
                            diff!!.toByteArray(StandardCharsets.UTF_8).size.toString()
                        )
                        .write(diff)
                        .end()
                }
            }
        }

        return ctx.currentRoute()
    }

    private fun useUpdateOperations(
        rtx: JsonNodeReadOnlyTrx,
        startNodeKeyAsLong: Long,
        databaseName: String,
        resourceName: String,
        firstRevision: String,
        secondRevision: String,
        maxDepthAsLong: Long,
        resultPromise: Promise<String>
    ) {
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
        val json = metaInfo.toString()
        resultPromise.complete(json)
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
