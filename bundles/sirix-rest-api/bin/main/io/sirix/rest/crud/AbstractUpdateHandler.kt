package io.sirix.rest.crud

import io.sirix.access.ResourceConfiguration
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import java.nio.file.Path
import java.time.Instant
import io.sirix.access.trx.node.HashType
import io.sirix.api.NodeTrx
import io.vertx.core.http.HttpHeaders


abstract class AbstractUpdateHandler(protected val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val insertionMode: String? = ctx.queryParam("insert").getOrNull(0)

        if (databaseName == null || resource == null) {
            throw IllegalArgumentException("Database name and resource name not given.")
        }

        val body = ctx.body().asString()

        update(databaseName, resource, nodeId?.toLongOrNull(), insertionMode, body, ctx)

        return ctx.currentRoute()
    }
    protected abstract suspend fun update(
        databaseName: String,
        resPathName: String,
        nodeId: Long?,
        insertionMode: String?,
        resFileToStore: String,
        ctx: RoutingContext
    )
    protected fun getCommitTimestamp(ctx: RoutingContext): Instant? {
        val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
        return if (commitTimestampAsString == null) {
            null
        } else {
            Revisions.parseRevisionTimestamp(commitTimestampAsString).toInstant()
        }
    }
    protected fun checkHashCode(ctx: RoutingContext, wtx: NodeTrx, resourceConfig: ResourceConfiguration) {
        if (resourceConfig.hashType != HashType.NONE && !wtx.isDocumentRoot) {
            val hashCode = ctx.request().getHeader(HttpHeaders.ETAG)
                ?: throw IllegalStateException("Hash code is missing in ETag HTTP-Header.")

            if (wtx.hash != hashCode.toLong()) {
                throw IllegalArgumentException("Someone might have changed the resource in the meantime.")
            }
        }
    }

    protected fun handleResponse(
        ctx: RoutingContext,
        maxNodeKey: Long,
        hash: Long,
        resourceConfig: ResourceConfiguration,
        body: String?
    ) {
        if (maxNodeKey > 5000) {
            ctx.response().statusCode = 200

            if (resourceConfig.hashType != HashType.NONE) {
                ctx.response().putHeader(HttpHeaders.ETAG, hash.toString())
            }

            ctx.response().end()
        } else if (body != null) {
            ctx.response().end(body)
        } else {
            ctx.response().end()
        }
        }
}