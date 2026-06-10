package io.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import io.sirix.access.DatabaseType
import io.sirix.access.Databases.getDatabaseType
import io.sirix.access.Databases.openJsonDatabase
import io.sirix.access.Databases.openXmlDatabase
import io.sirix.api.Database
import io.sirix.rest.Auth
import io.sirix.rest.AuthRole
import io.sirix.service.json.serialize.StringValue
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class HistoryHandler(private val location: Path, private val authz: AuthorizationProvider) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")!!
        val resourceName = ctx.pathParam("resource")!!

        PathValidation.validatePathParam(databaseName, "database")
        PathValidation.validatePathParam(resourceName, "resource")

        val user = ctx.get<User>("user")!!
        Auth.checkIfAuthorized(user, databaseName, AuthRole.VIEW, authz)

        // Database open + history retrieval + JSON assembly are blocking (file I/O, one
        // RevisionRootPage read per uncached revision). Run them on a worker pool like every
        // other handler does — previously this ran on the Vert.x event loop, stalling the loop
        // for the whole duration and adding the history latency to every concurrent request
        // (including query-editor calls) served by the same event loop.
        val content = withContext(Dispatchers.IO) {
            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") val database: Database<*> =
                when (getDatabaseType(location.resolve(databaseName).toAbsolutePath())) {
                    DatabaseType.JSON -> openJsonDatabase(location.resolve(databaseName))
                    DatabaseType.XML -> openXmlDatabase(location.resolve(databaseName))
                }

            val buffer = StringBuilder()
            database.use {
                val manager = database.beginResourceSession(resourceName)

                manager.use {
                    val numberOfRevisions = ctx.queryParam("revisions")
                    val startRevision = ctx.queryParam("startRevision")
                    val endRevision = ctx.queryParam("endRevision")


                    // Bare toInt() turned malformed params into generic 500s — they are 400s.
                    val historyList = if (numberOfRevisions.isEmpty()) {
                        if (startRevision.isEmpty() && endRevision.isEmpty()) {
                            manager.history
                        } else {
                            val startRevisionAsInt =
                                requireIntParam("startRevision", startRevision.getOrNull(0) ?: "")
                            val endRevisionAsInt =
                                requireIntParam("endRevision", endRevision.getOrNull(0) ?: "")
                            manager.getHistory(startRevisionAsInt, endRevisionAsInt)
                        }
                    } else {
                        val revisions = requireIntParam("revisions", numberOfRevisions[0])
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
            buffer.toString()
        }

        withContext(ctx.vertx().dispatcher()) {
            val res = ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
            res.write(content)
            res.end()
        }

        return ctx.currentRoute()!!
    }
}