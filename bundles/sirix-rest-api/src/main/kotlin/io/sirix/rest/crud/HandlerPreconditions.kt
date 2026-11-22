package io.sirix.rest.crud

import io.sirix.api.NodeCursor
import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.HttpException

/**
 * Shared parse-and-precondition helpers for the REST handlers. Error status mapping belongs
 * to RequestFailureHandling; REST-204 records the missing-header mapping gap.
 */

/** Parse-or-400 for integer params. */
fun requireIntParam(name: String, value: String): Int =
    value.toIntOrNull() ?: throw IllegalArgumentException("$name must be an integer: '$value'")

/** Parse-or-400 for long params. */
fun requireLongParam(name: String, value: String): Long =
    value.toLongOrNull() ?: throw IllegalArgumentException("$name must be a long: '$value'")

/**
 * Optimistic-concurrency precondition shared by the update and delete paths: the standard
 * "If-Match" header (preferred) or the legacy request-"ETag" header must carry the node hash
 * (RFC 7232 quotes are stripped). Callers gate on hashType/document-root themselves — see the
 * design-gap note on AbstractUpdateHandler.checkHashCode.
 */
fun checkNodeHashPrecondition(ctx: RoutingContext, actualHash: Long) {
    val rawHash = ctx.request().getHeader("If-Match")
        ?: ctx.request().getHeader(HttpHeaders.ETAG)
        ?: throw IllegalStateException("Hash code is missing in If-Match (or legacy ETag) HTTP-Header.")
    val hashCode = rawHash.trim().removeSurrounding("\"")

    if (actualHash != (hashCode.toLongOrNull()
            ?: throw IllegalArgumentException("If-Match/ETag header must be the node hash (a long): '$rawHash'"))
    ) {
        throw IllegalArgumentException("Someone might have changed the resource in the meantime.")
    }
}

/**
 * Reject failed movement before a handler can serialize or mutate the previous cursor position.
 * Throws an [HttpException] carrying 404: the node key came from the request, so an unknown key
 * is a client-visible "not found", not an internal failure (an IllegalStateException surfaced
 * as 500).
 */
fun NodeCursor.moveToOrNotFound(nodeId: Long) {
    if (!moveTo(nodeId)) {
        throw HttpException(HttpResponseStatus.NOT_FOUND.code(), "Node with ID $nodeId not found.")
    }
}
