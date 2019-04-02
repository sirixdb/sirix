package org.sirix.rest

import io.vertx.ext.web.RoutingContext
import org.sirix.service.xml.serialize.XmlSerializer
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

class XdmSerializeHelper {
    fun serializeXml(serializer: XmlSerializer, out: ByteArrayOutputStream, ctx: RoutingContext) {
        serializer.call()
        val body = String(out.toByteArray(), StandardCharsets.UTF_8)

        ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/xml")
                .putHeader("Content-Length", body.length.toString())
                .write(body)
                .end()
    }
}