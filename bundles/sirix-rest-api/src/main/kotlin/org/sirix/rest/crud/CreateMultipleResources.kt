package org.sirix.rest.crud

import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import org.sirix.rest.crud.json.JsonCreate
import org.sirix.rest.crud.xml.XmlCreate
import java.nio.file.Path

class CreateMultipleResources(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val fileUploads = ctx.fileUploads()
        var xmlCount = 0
        var jsonCount = 0

        fileUploads.forEach { fileUpload ->
            when (fileUpload.contentType()) {
                "application/xml" -> xmlCount++
                "application/json" -> jsonCount++
            }
        }

        if (xmlCount > 0 && xmlCount != fileUploads.size) {
            ctx.fail(IllegalArgumentException("All uploaded files must be either of type XML or JSON."))
        } else if (jsonCount > 0 && jsonCount != fileUploads.size) {
            ctx.fail(IllegalArgumentException("All uploaded files must be either of type XML or JSON."))
        }

        if (ctx.failed()) return ctx.currentRoute()

        if (xmlCount > 0) XmlCreate(location, true).handle(ctx)
        else if (jsonCount > 0) JsonCreate(location, true).handle(ctx)

        return ctx.currentRoute()
    }
}