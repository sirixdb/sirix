package org.sirix.rest

import io.vertx.ext.web.RoutingContext
import org.sirix.service.xml.serialize.XMLSerializer
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

class Serialize {
    fun serializeXml(serializer: XMLSerializer, out: ByteArrayOutputStream, ctx: RoutingContext) {
        serializer.call()
        val body = String(out.toByteArray(), StandardCharsets.UTF_8)

        ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/xml")
                .putHeader("Content-Length", body.length.toString())
                .write(body)
                .end()
    }
}