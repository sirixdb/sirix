package io.sirix.rest.crud.json

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import io.sirix.access.trx.node.HashType
import io.sirix.api.json.JsonResourceSession
import java.io.StringWriter
import java.util.concurrent.Callable

class JsonSerializeHelper {
    fun serialize(
        serializer: Callable<*>,
        out: StringWriter,
        ctx: RoutingContext,
        manager: JsonResourceSession,
        revisions: IntArray,
        nodeId: Long?,
    ): String {
        serializer.call()

        val body = out.toString()

        if (manager.resourceConfig.hashType == HashType.NONE) {
            writeResponseWithoutHashValue(ctx)
        } else {
            writeResponseWithHashValue(manager, revisions[0], ctx, nodeId)
        }

        return body
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