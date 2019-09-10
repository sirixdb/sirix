package org.sirix.rest.crud.xml

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Future
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.Database
import org.sirix.api.xml.XmlResourceManager
import org.sirix.exception.SirixUsageException
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

class XmlHead(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val dbName = ctx.pathParam("database")
        val resName = ctx.pathParam("resource")

        if (dbName == null || resName == null) {
            ctx.fail(IllegalArgumentException("Database name and resource name must be given."))
        }

        ctx.vertx().orCreateContext.executeBlockingAwait { _: Future<Unit> ->
            head(dbName!!, ctx, resName!!)
        }

        return ctx.currentRoute()
    }

    private fun head(dbName: String, ctx: RoutingContext, resName: String) {
        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        val database: Database<XmlResourceManager>
        try {
            database = Databases.openXmlDatabase(location.resolve(dbName))
        } catch (e: SirixUsageException) {
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            return
        }

        database.use {
            try {
                val manager = database.openResourceManager(resName)

                manager.use {
                    if (manager.resourceConfig.hashType == HashType.NONE)
                        return

                    val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)

                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber)

                    rtx.use {
                        if (nodeId != null) {
                            rtx.moveTo(nodeId.toLong())
                        } else if (rtx.isDocumentRoot) {
                            rtx.moveToFirstChild()
                        }

                        ctx.response().putHeader(HttpHeaders.ETAG, rtx.hash.toString())
                        ctx.response().end()
                    }
                }
            } catch (e: SirixUsageException) {
                ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
                return
            }

        }
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