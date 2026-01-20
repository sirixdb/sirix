package io.sirix.rest.crud

import io.sirix.access.Databases
import io.sirix.access.trx.node.HashType
import io.sirix.api.Database
import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.ResourceSession
import io.sirix.api.json.JsonResourceSession
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

abstract class AbstractHeadHandler< T : ResourceSession<*, *>> (
        private val location: Path){
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")

        if (databaseName == null || resource == null) {
            throw IllegalStateException("Database name and resource name must be given.")
        }

        ctx.vertx().executeBlocking<Unit> {
            head(databaseName, ctx, resource)
        }.coAwait()

        return ctx.currentRoute()
    }
    fun head(databaseName: String, ctx: RoutingContext, resource: String) {
        // Check if database exists first
        val dbPath = location.resolve(databaseName)
        if (!Databases.existsDatabase(dbPath)) {
            ctx.response().setStatusCode(404).end()
            return
        }
        val revision = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp = ctx.queryParam("revision-timestamp").getOrNull(0)

        val nodeId = ctx.queryParam("nodeId").getOrNull(0)

        val database = openDatabase(location.resolve(databaseName))

        database.use {
            // Check if resource exists - return 404 if not
            if (!database.existsResource(resource)) {
                ctx.response().setStatusCode(404).end()
                return
            }

            val manager = database.beginResourceSession(resource)

            manager.use {
                if (manager.resourceConfig.hashType == HashType.NONE) {
                    ctx.response().putHeader(HttpHeaders.ETAG, "")
                } else {
                    val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)

                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber)

                    rtx.use {
                        if (nodeId != null) {
                            if (!rtx.moveTo(nodeId.toLong())) {
                                ctx.response().setStatusCode(404).end()
                                return
                            }
                        } else if (rtx.isDocumentRoot) {
                            (rtx as NodeCursor).moveToFirstChild()
                        }

                        ctx.response().putHeader(HttpHeaders.ETAG, rtx.hash.toString())
                    }
                }
            }
        }

        ctx.response().end()
    }
    private fun getRevisionNumber(rev: String?, revTimestamp: String?, manager: T): Int {
        return rev?.toInt()
                ?: if (revTimestamp != null) {
                    var revision = getRevisionNumber(manager, revTimestamp)
                    if (revision == 0) {
                        ++revision
                    } else {
                        revision
                    }
                } else {
                    manager.mostRecentRevisionNumber
                }
    }

    private fun getRevisionNumber(manager: T, revision: String): Int {
        val revisionDateTime = LocalDateTime.parse(revision)
        val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
        return manager.getRevisionNumber(zdt.toInstant())
    }
    abstract fun openDatabase(dbFile: Path): Database<T>
}