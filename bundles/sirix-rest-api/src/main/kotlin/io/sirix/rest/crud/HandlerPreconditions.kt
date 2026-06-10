package io.sirix.rest.crud

import io.sirix.api.NodeCursor
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext

/**
 * Shared parse-and-precondition helpers for the REST handlers. The failure handler maps
 * [IllegalArgumentException] to 400 and [IllegalStateException] to a client-visible error, so a
 * malformed parameter or stale precondition never surfaces as a 500.
 */

/** Parse-or-400 for integer params — `toInt()` on user input 500s on garbage. */
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
 * Move the cursor to the node or fail with a client-visible "not found": moveTo on a missing key
 * returns false and LEAVES THE CURSOR ON THE DOCUMENT ROOT, so an unchecked call silently
 * operates on an unrelated position (the bug class every handler used to have independently).
 */
fun NodeCursor.moveToOrNotFound(nodeId: Long) {
    if (!moveTo(nodeId)) {
        throw IllegalStateException("Node with ID $nodeId not found.")
    }
}
