package io.sirix.rest.crud.json

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import io.sirix.access.trx.node.HashType
import io.sirix.api.json.JsonResourceSession
import java.io.ByteArrayOutputStream
import java.io.StringWriter
import java.util.concurrent.Callable

class JsonSerializeHelper {
    /**
     * Byte pipeline: the serializer wrote UTF-8 directly to [out], so the bytes are wrapped
     * wire-ready — no intermediate String and no re-encoding on [io.vertx.core.http.HttpServerResponse.end].
     */
    fun serialize(
        serializer: Callable<*>,
        out: ByteArrayOutputStream,
        ctx: RoutingContext,
        manager: JsonResourceSession,
        revisions: IntArray,
        nodeId: Long?,
    ): Buffer {
        serializer.call()
        writeResponseHeaders(ctx, manager, revisions, nodeId)
        return Buffer.buffer(out.toByteArray())
    }

    /** Char pipeline — kept for serializers without a byte-output mode (no main caller left). */
    fun serialize(
        serializer: Callable<*>,
        out: StringWriter,
        ctx: RoutingContext,
        manager: JsonResourceSession,
        revisions: IntArray,
        nodeId: Long?,
    ): Buffer {
        serializer.call()
        writeResponseHeaders(ctx, manager, revisions, nodeId)
        return Buffer.buffer(out.toString())
    }

    private fun writeResponseHeaders(
        ctx: RoutingContext,
        manager: JsonResourceSession,
        revisions: IntArray,
        nodeId: Long?
    ) {
        if (manager.resourceConfig.hashType == HashType.NONE) {
            writeResponseWithoutHashValue(ctx)
        } else {
            writeResponseWithHashValue(manager, revisions[0], ctx, nodeId)
        }
    }

    private fun writeResponseWithoutHashValue(ctx: RoutingContext) {
        ctx.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    }

    private fun writeResponseWithHashValue(
        manager: JsonResourceSession,
        revision: Int,
        ctx: RoutingContext,
        nodeId: Long?
    ) {
        val rtx = manager.beginNodeReadOnlyTrx(revision)

        rtx.use {
            val hash = if (nodeId == null) {
                rtx.moveToFirstChild()
                rtx.hash
            } else {
                rtx.moveTo(nodeId)
                rtx.hash
            }

            ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                .putHeader(HttpHeaders.ETAG, hash.toString())
        }
    }
}
