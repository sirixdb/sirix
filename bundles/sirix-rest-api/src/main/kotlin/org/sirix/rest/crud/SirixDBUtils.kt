package org.sirix.rest.crud

import org.sirix.access.User as SirixDBUser

import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.KeycloakHelper
import io.vertx.ext.web.RoutingContext
import java.util.*
import org.sirix.api.Database
import org.sirix.access.Databases
import org.sirix.exception.SirixUsageException
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.netty.handler.codec.http.HttpResponseStatus
import java.nio.file.Path
import org.sirix.api.json.JsonResourceManager
import io.vertx.core.http.HttpHeaders
import org.sirix.access.DatabaseType

class SirixDBUtils {
    companion object {
        fun createSirixDBUser(ctx: RoutingContext): SirixDBUser {
            val user = ctx.get("user") as User
            val accessToken = KeycloakHelper.accessToken(user.principal())
            val userId = accessToken.getString("sub")
            val userName = accessToken.getString("userName")
            val userUuid = UUID.fromString(userId)
            return SirixDBUser(userName, userUuid)
        }

        fun getHistory(ctx: RoutingContext, location: Path, databaseName: String, resourceName: String, type: DatabaseType) {
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
                    val buffer = StringBuilder()

                    val historyList = manager.getHistory()

                    when (type) {
                        DatabaseType.JSON -> {
                            buffer.append("{\"history\":[")

                            historyList.forEachIndexed { index, revisionTuple ->
                                buffer.append("{\"revision\":")
                                buffer.append(revisionTuple.getRevision())
                                buffer.append(",");

                                buffer.append("\"revisionTimestamp\":\"")
                                buffer.append(revisionTuple.getRevisionTimestamp())
                                buffer.append("\",");

                                buffer.append("\"user\":\"")
                                buffer.append(revisionTuple.getUser().getName())
                                buffer.append("\",");

                                buffer.append("\"commitMessage\":")
                                buffer.append(revisionTuple.getCommitMessage().orElse(""))
                                buffer.append("\"}");

                                if (index != historyList.size - 1)
                                    buffer.append(",")
                            }

                            buffer.append("]}")
                        }
                        DatabaseType.XML -> {
                            buffer.appendln("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">")

                            historyList.forEach { revisionTuple ->
                                buffer.append("<revision revisionNumber=\"")
                                buffer.append(revisionTuple.getRevision())
                                buffer.append("\" ");

                                buffer.append("revisionTimestamp=\"")
                                buffer.append(revisionTuple.getRevisionTimestamp())
                                buffer.append("\" ");

                                buffer.append("user=\"")
                                buffer.append(revisionTuple.getUser().getName())
                                buffer.append("\" ");

                                buffer.append("commitMessage=\"")
                                buffer.append(revisionTuple.getCommitMessage().orElse(""))
                                buffer.append("\"/>");
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
}