package org.sirix.rest

import io.vertx.ext.web.RoutingContext
import org.sirix.service.json.serialize.JsonSerializer
import java.io.StringWriter

class JsonSerializeHelper {
    fun serialize(serializer: JsonSerializer, out: StringWriter, ctx: RoutingContext) {
        serializer.call()
        val body = out.toString()

        ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .putHeader("Content-Length", body.length.toString())
                .write(body)
                .end()
    }
}