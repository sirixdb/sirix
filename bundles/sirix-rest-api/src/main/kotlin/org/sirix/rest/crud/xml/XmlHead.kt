package org.sirix.rest.crud.xml

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.xml.XmlNodeReadOnlyTrx
import org.sirix.api.xml.XmlResourceManager
import org.sirix.exception.SirixUsageException
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

class XmlHead(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")

        if (databaseName == null || resource == null) {
            ctx.fail(IllegalArgumentException("Database name and resource name must be given."))
            return ctx.currentRoute()
        }

        ctx.vertx().orCreateContext.executeBlockingAwait { _: Promise<Unit> ->
            head(databaseName, ctx, resource)
        }

        return ctx.currentRoute()
    }

    private fun head(databaseName: String, ctx: RoutingContext, resource: String) {
        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        val database: Database<XmlResourceManager>
        try {
            database = Databases.openXmlDatabase(location.resolve(databaseName))
        } catch (e: SirixUsageException) {
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            return
        }

        database.use {
            try {
                val manager = database.openResourceManager(resource)

                manager.use {
                    if (manager.resourceConfig.hashType == HashType.NONE)
                        return

                    val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)

                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber)

                    rtx.use {
                        if (nodeId != null) {
                            if (!rtx.moveTo(nodeId.toLong()).hasMoved()) {
                                ctx.fail(
                                    HttpResponseStatus.BAD_REQUEST.code(),
                                    IllegalStateException("Node with ID ${nodeId} doesn't exist.")
                                )
                            } else {
                                writeResponse(ctx, rtx)
                            }
                        } else if (rtx.isDocumentRoot) {
                            rtx.moveToFirstChild()
                            writeResponse(ctx, rtx)
                        }
                    }
                }
            } catch (e: SirixUsageException) {
                ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            }
        }
    }

    private fun writeResponse(ctx: RoutingContext, rtx: XmlNodeReadOnlyTrx) {
        ctx.response().putHeader(HttpHeaders.ETAG, rtx.hash.toString())
        ctx.response().end()
    }

    private fun getRevisionNumber(rev: String?, revTimestamp: String?, manager: XmlResourceManager): Int {
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

    private fun getRevisionNumber(manager: XmlResourceManager, revision: String): Int {
        val revisionDateTime = LocalDateTime.parse(revision)
        val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
        return manager.getRevisionNumber(zdt.toInstant())
    }
}