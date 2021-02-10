package org.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.withContext
import org.sirix.access.DatabaseType
import org.sirix.access.Databases.*
import org.sirix.api.Database
import org.sirix.service.json.serialize.StringValue
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class HistoryHandler(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")
        val resourceName = ctx.pathParam("resource")

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") val database: Database<*> =
            when (getDatabaseType(location.resolve(databaseName).toAbsolutePath())) {
                DatabaseType.JSON -> openJsonDatabase(location.resolve(databaseName))
                DatabaseType.XML -> openXmlDatabase(location.resolve(databaseName))
            }

        withContext(ctx.vertx().dispatcher()) {
            val buffer = StringBuilder()
            database.use {
                val manager = database.openResourceManager(resourceName)

                manager.use {
                    val numberOfRevisions = ctx.queryParam("revisions")
                    val startRevision = ctx.queryParam("startRevision")
                    val endRevision = ctx.queryParam("endRevision")


                    val historyList = if (numberOfRevisions.isEmpty()) {
                        if (startRevision.isEmpty() && endRevision.isEmpty()) {
                            manager.getHistory()
                        } else {
                            val startRevisionAsInt = startRevision[0].toInt()
                            val endRevisionAsInt = endRevision[0].toInt()
                            manager.getHistory(startRevisionAsInt, endRevisionAsInt)
                        }
                    } else {
                        val revisions = numberOfRevisions[0].toInt()
                        manager.getHistory(revisions)
                    }

                    buffer.append("{\"history\":[")

                    historyList.forEachIndexed { index, revisionTuple ->
                        buffer.append("{\"revision\":")
                        buffer.append(revisionTuple.revision)
                        buffer.append(",")

                        buffer.append("\"revisionTimestamp\":\"")
                        buffer.append(revisionTuple.revisionTimestamp)
                        buffer.append("\",")

                        buffer.append("\"author\":\"")
                        buffer.append(StringValue.escape(revisionTuple.user.name))
                        buffer.append("\",")

                        buffer.append("\"commitMessage\":\"")
                        buffer.append(StringValue.escape(revisionTuple.commitMessage.orElse("")))
                        buffer.append("\"}")

                        if (index != historyList.size - 1)
                            buffer.append(",")
                    }

                    buffer.append("]}")
                }
            }

            val content = buffer.toString()

            val res = ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
            res.write(content)
            res.end()
        }

        return ctx.currentRoute()
    }
}