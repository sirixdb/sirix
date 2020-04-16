package org.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.DatabaseType
import org.sirix.access.Databases.*
import org.sirix.access.ResourceConfiguration
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.service.json.BasicJsonDiff
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

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
                    val diff = context.executeBlockingAwait<String> {
                        val firstRevision: String? = ctx.queryParam("first-revision").getOrNull(0)
                        val secondRevision: String? = ctx.queryParam("second-revision").getOrNull(0)

                        if (firstRevision == null || secondRevision == null) {
                            ctx.fail(IllegalArgumentException("First and second revision must be specified."))
                            return@executeBlockingAwait
                        }

                        val startNodeKey: String? = ctx.queryParam("startNodeKey").getOrNull(0)
                        val maxDepth: String? = ctx.queryParam("maxDepth").getOrNull(0)

                        val startNodeKeyAsLong = startNodeKey?.let { startNodeKey.toLong() } ?: 0
                        val maxDepthAsLong = maxDepth?.let { maxDepth.toLong() } ?: 0

                        val diffPath: Path = resourceManager.getResourceConfig()
                            .resource
                            .resolve(ResourceConfiguration.ResourcePaths.DATA.path)
                            .resolve("diffFromRev1toRev2.json")

                        if (startNodeKeyAsLong == 0L && maxDepthAsLong == 0L) {
                            it.complete(Files.readString(diffPath))
                        } else {
                            it.complete(
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

    private fun openDatabase(databaseName: String): Database<*> {
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
        return when (getDatabaseType(location.resolve(databaseName).toAbsolutePath())) {
            DatabaseType.JSON -> openJsonDatabase(location.resolve(databaseName))
            DatabaseType.XML -> openXmlDatabase(location.resolve(databaseName))
        }
    }
}
