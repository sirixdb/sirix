package org.sirix.rest.crud.json

import io.vertx.ext.web.RoutingContext
import org.sirix.access.trx.node.HashType
import org.sirix.api.json.JsonResourceManager
import org.sirix.service.json.serialize.JsonSerializer
import java.io.StringWriter

class JsonSerializeHelper {
    fun serialize(serializer: JsonSerializer, out: StringWriter, ctx: RoutingContext, manager: JsonResourceManager, nodeId: Long?) {
        serializer.call()

        val body = out.toString()

        if (manager.resourceConfig.hashType == HashType.NONE) {
            writeResponseWithoutHashValue(ctx, body)
        } else {
            writeResponseWithHashValue(manager, ctx, body, nodeId)
        }
    }

    private fun writeResponseWithoutHashValue(ctx: RoutingContext, body: String) {
        ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .putHeader("Content-Length", body.length.toString())
                .write(body)
                .end()
    }

    private fun writeResponseWithHashValue(manager: JsonResourceManager, ctx: RoutingContext, body: String, nodeId: Long?) {
        val rtx = manager.beginNodeReadOnlyTrx()

        rtx.use {
            val hash = if (nodeId == null)
                rtx.moveToFirstChild().trx().hash
            else
                rtx.moveTo(nodeId).trx().hash

            ctx.response().setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .putHeader("Content-Length", body.length.toString())
                    .putHeader("ETag", hash.toString())
                    .write(body)
                    .end()
        }
    }
}