package org.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.exception.SirixUsageException
import org.sirix.service.json.serialize.StringValue
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class History {
    fun serialize(
        ctx: RoutingContext,
        location: Path,
        databaseName: String,
        resourceName: String
    ) {
        val databaseType = Databases.getDatabaseType(location.resolve(databaseName).toAbsolutePath())

        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") val database =
            try {
                when (databaseType) {
                    DatabaseType.JSON -> Databases.openJsonDatabase(location.resolve(databaseName))
                    DatabaseType.XML -> Databases.openXmlDatabase(location.resolve(databaseName))
                }

            } catch (e: SirixUsageException) {
                ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
                return
            }

        database.use {
            val manager = database.openResourceManager(resourceName)

            manager.use {
                val numberOfRevisions = ctx.queryParam("revisions")
                val startRevision = ctx.queryParam("startRevision")
                val endRevision = ctx.queryParam("endRevision")

                val buffer = StringBuilder()

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

                val content = buffer.toString()

                ctx.response().setStatusCode(200)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
                    .write(content)
                    .end()
            }
        }
    }
}