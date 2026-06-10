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
        val databaseName = ctx.pathParam("database")!!
        val resource = ctx.pathParam("resource")!!
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val insertionMode: String? = ctx.queryParam("insert").getOrNull(0)

        PathValidation.validatePathParam(databaseName, "database")
        PathValidation.validatePathParam(resource, "resource")

        val body = ctx.body().asString()!!

        // A non-numeric nodeId must be a client error — toLongOrNull silently degraded
        // "?nodeId=abc" to "no nodeId", redirecting the update to the document root.
        val nodeIdAsLong = nodeId?.let {
            it.toLongOrNull() ?: throw IllegalArgumentException("nodeId must be a long: '$it'")
        }

        update(databaseName, resource, nodeIdAsLong, insertionMode, body, ctx)

        return ctx.currentRoute()!!
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
    /**
     * Optimistic-concurrency guard. KNOWN DESIGN GAP (tracked for an API revision):
     * the standard "If-Match" header is now accepted (the legacy request-"ETag" form remains for
     * compatibility), but the check is still silently skipped for document-root targets and for
     * resources with hashType NONE, and the JSONiq query path (POST /) has no protocol field to
     * carry a hash at all — so concurrent whole-document or query-based edits remain
     * last-writer-wins with no 412. Closing those requires an API design decision.
     */
    protected fun checkHashCode(ctx: RoutingContext, wtx: NodeTrx, resourceConfig: ResourceConfiguration) {
        if (resourceConfig.hashType != HashType.NONE && !wtx.isDocumentRoot) {
            // Standard "If-Match" is now accepted (preferred); the legacy use of the request
            // "ETag" header stays for compatibility. Strip the optional RFC 7232 quotes.
            val rawHash = ctx.request().getHeader("If-Match")
                ?: ctx.request().getHeader(HttpHeaders.ETAG)
                ?: throw IllegalStateException("Hash code is missing in If-Match (or legacy ETag) HTTP-Header.")
            val hashCode = rawHash.trim().removeSurrounding("\"")

            if (wtx.hash != (hashCode.toLongOrNull()
                    ?: throw IllegalArgumentException("If-Match/ETag header must be the node hash (a long): '$rawHash'"))
            ) {
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