package io.sirix.rest.crud.xml

import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import io.sirix.access.trx.node.HashType
import io.sirix.api.xml.XmlResourceSession
import io.sirix.service.xml.serialize.XmlSerializer
import java.io.ByteArrayOutputStream

class XmlSerializeHelper {
    fun serializeXml(
        serializer: XmlSerializer,
        out: ByteArrayOutputStream,
        ctx: RoutingContext,
        manager: XmlResourceSession,
        revision: Int,
        nodeId: Long?
    ): Buffer {
        serializer.call()
        // The serializer already wrote UTF-8 bytes — wrap them wire-ready instead of
        // decoding to a String only for Vert.x to re-encode on end().
        val body = Buffer.buffer(out.toByteArray())

        if (manager.resourceConfig.hashType == HashType.NONE) {
            writeResponseWithoutHashValue(ctx)
        } else {
            writeResponseWithHashValue(manager, revision, ctx, nodeId)
        }

        return body
    }

    private fun writeResponseWithoutHashValue(ctx: RoutingContext) {
        ctx.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
    }

    private fun writeResponseWithHashValue(
        manager: XmlResourceSession,
        revision: Int,
        ctx: RoutingContext,
        nodeId: Long?
    ) {
        // The ETag must hash the revision that was serialized — a no-arg trx reads the LATEST
        // revision, yielding a wrong ETag for historical GETs (?revision=N).
        val rtx = manager.beginNodeReadOnlyTrx(revision)

        rtx.use {
            val hash = if (nodeId == null) {
                rtx.moveToFirstChild();
                rtx.hash
            } else {
                rtx.moveTo(nodeId);
                rtx.hash
            }

            ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
                .putHeader(HttpHeaders.ETAG, hash.toString())
        }
    }
}