package org.sirix.rest.crud.xml

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import org.sirix.access.trx.node.HashType
import org.sirix.api.xml.XmlResourceManager
import org.sirix.service.xml.serialize.XmlSerializer
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

class XmlSerializeHelper {
    fun serializeXml(
        serializer: XmlSerializer,
        out: ByteArrayOutputStream,
        ctx: RoutingContext,
        manager: XmlResourceManager,
        nodeId: Long?
    ): String {
        serializer.call()
        val body = String(out.toByteArray(), StandardCharsets.UTF_8)

        if (manager.resourceConfig.hashType == HashType.NONE) {
            writeResponseWithoutHashValue(ctx)
        } else {
            writeResponseWithHashValue(manager, ctx, nodeId)
        }

        return body
    }

    private fun writeResponseWithoutHashValue(ctx: RoutingContext) {
        ctx.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
    }

    private fun writeResponseWithHashValue(
        manager: XmlResourceManager,
        ctx: RoutingContext,
        nodeId: Long?
    ) {
        val rtx = manager.beginNodeReadOnlyTrx()

        rtx.use {
            val hash = if (nodeId == null)
                rtx.moveToFirstChild().trx().hash
            else
                rtx.moveTo(nodeId).trx().hash

            ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
                .putHeader(HttpHeaders.ETAG, hash.toString())
        }
    }
}