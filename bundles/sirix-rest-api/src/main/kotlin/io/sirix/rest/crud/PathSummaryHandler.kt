package io.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import io.sirix.access.DatabaseType
import io.sirix.access.Databases.*
import io.sirix.api.Database
import io.sirix.axis.DescendantAxis
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class PathSummaryHandler(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName = ctx.pathParam("database")
        val resourceName = ctx.pathParam("resource")

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") val database: Database<*> =
            when (getDatabaseType(location.resolve(databaseName).toAbsolutePath())) {
                DatabaseType.JSON -> openJsonDatabase(location.resolve(databaseName))
                DatabaseType.XML -> openXmlDatabase(location.resolve(databaseName))
            }

        context.executeBlocking {
            val buffer = StringBuilder()
            database.use {
                val manager = database.beginResourceSession(resourceName)

                manager.use {
                    if (manager.resourceConfig.withPathSummary) {
                        val revision = ctx.queryParam("revision")[0]

                        val pathSummary = manager.openPathSummary(revision.toInt())
                        val pathSummaryAxis = DescendantAxis(pathSummary)

                        buffer.append("{\"pathSummary\":[")

                        while (pathSummaryAxis.hasNext()) {
                            pathSummaryAxis.nextLong()

                            buffer.append("{")
                            buffer.append("nodeKey:")
                            buffer.append(pathSummary.nodeKey)
                            buffer.append(",")
                            buffer.append("path:")
                            buffer.append(pathSummary.path)
                            buffer.append(",")
                            buffer.append("references:")
                            buffer.append(pathSummary.references)
                            buffer.append(",")
                            buffer.append("level:")
                            buffer.append(pathSummary.level)
                            buffer.append("}")

                            if (pathSummaryAxis.hasNext()) {
                                buffer.append(",")
                            }
                        }

                        buffer.append("]}")
                    } else {
                        buffer.append("{\"pathSummary\":[]}")
                    }
                }
            }

            val content = buffer.toString()

            ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
                .end(content)
        }.coAwait()

        return ctx.currentRoute()
    }
}