package org.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.net.closeAwait
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.coroutines.await
import org.sirix.access.DatabaseType
import org.sirix.access.Databases.*
import org.sirix.api.Database
import org.sirix.axis.DescendantAxis
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

        context.executeBlocking<String> {
            val buffer = StringBuilder()
            database.use {
                val manager = database.openResourceManager(resourceName)

                manager.use {
                    if (manager.getResourceConfig().withPathSummary) {
                        val revision = ctx.queryParam("revision")[0]

                        val pathSummary = manager.openPathSummary(revision.toInt())
                        val pathSummaryAxis = DescendantAxis(pathSummary)

                        buffer.append("{\"pathSummary\":[")

                        while (pathSummaryAxis.hasNext()) {
                            pathSummaryAxis.next()

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

            val res = ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
            res.write(content)
            res.end()
        }.await()

        return ctx.currentRoute()
    }
}