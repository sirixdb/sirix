package org.sirix.rest.crud.json

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.json.JsonResourceManager
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

class JsonHead(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")

        if (databaseName == null || resource == null) {
            throw IllegalArgumentException("Database name and resource name must be given.")
        }

        ctx.vertx().executeBlocking<Unit> {
            head(databaseName, ctx, resource)
        }.await()

        return ctx.currentRoute()
    }

    private fun head(databaseName: String, ctx: RoutingContext, resource: String) {
        val revision = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp = ctx.queryParam("revision-timestamp").getOrNull(0)

        val nodeId = ctx.queryParam("nodeId").getOrNull(0)

        val database = Databases.openJsonDatabase(location.resolve(databaseName))

        database.use {
            val manager = database.openResourceManager(resource)

            manager.use {
                if (manager.resourceConfig.hashType == HashType.NONE) {
                    ctx.response().putHeader(HttpHeaders.ETAG, "")
                } else {
                    val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)

                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber)

                    rtx.use {
                        if (nodeId != null) {
                            if (!rtx.moveTo(nodeId.toLong()).hasMoved()) {
                                throw IllegalStateException("Node with ID ${nodeId} doesn't exist.")
                            }
                        } else if (rtx.isDocumentRoot) {
                            rtx.moveToFirstChild()
                        }

                        ctx.response().putHeader(HttpHeaders.ETAG, rtx.hash.toString())
                    }
                }
            }
        }

        ctx.response().end()
    }

    private fun getRevisionNumber(rev: String?, revTimestamp: String?, manager: JsonResourceManager): Int {
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

    private fun getRevisionNumber(manager: JsonResourceManager, revision: String): Int {
        val revisionDateTime = LocalDateTime.parse(revision)
        val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
        return manager.getRevisionNumber(zdt.toInstant())
    }
}