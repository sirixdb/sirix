package org.sirix.rest.crud

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.exception.SirixUsageException
import java.nio.file.Path

class HistorySerializer {
    fun getHistory(
        ctx: RoutingContext,
        location: Path,
        databaseName: String,
        resourceName: String,
        type: DatabaseType
    ) {
        val database =
            try {
                when (type) {
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

                when (type) {
                    DatabaseType.JSON -> {
                        buffer.append("{\"history\":[")

                        historyList.forEachIndexed { index, revisionTuple ->
                            buffer.append("{\"revision\":")
                            buffer.append(revisionTuple.revision)
                            buffer.append(",")

                            buffer.append("\"revisionTimestamp\":\"")
                            buffer.append(revisionTuple.revisionTimestamp)
                            buffer.append("\",")

                            buffer.append("\"author\":\"")
                            buffer.append(revisionTuple.user.name)
                            buffer.append("\",")

                            buffer.append("\"commitMessage\":\"")
                            buffer.append(revisionTuple.commitMessage.orElse(""))
                            buffer.append("\"}")

                            if (index != historyList.size - 1)
                                buffer.append(",")
                        }

                        buffer.append("]}")
                    }
                    DatabaseType.XML -> {
                        buffer.appendln("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">")

                        historyList.forEach { revisionTuple ->
                            buffer.append("<revision revisionNumber=\"")
                            buffer.append(revisionTuple.revision)
                            buffer.append("\" ")

                            buffer.append("revisionTimestamp=\"")
                            buffer.append(revisionTuple.revisionTimestamp)
                            buffer.append("\" ")

                            buffer.append("author=\"")
                            buffer.append(revisionTuple.user.name)
                            buffer.append("\" ")

                            buffer.append("commitMessage=\"")
                            buffer.append(revisionTuple.commitMessage.orElse(""))
                            buffer.append("\"/>")
                        }

                        buffer.append("</rest:sequence>")
                    }
                }

                val content = buffer.toString()
                val contentType = when (type) {
                    DatabaseType.JSON -> "application/json"
                    DatabaseType.XML -> "application/xml"
                }

                ctx.response().setStatusCode(200)
                    .putHeader(HttpHeaders.CONTENT_TYPE, contentType)
                    .putHeader(HttpHeaders.CONTENT_LENGTH, content.length.toString())
                    .write(content)
                    .end()
            }
        }
    }
}