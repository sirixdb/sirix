package org.sirix.rest.crud

import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import org.sirix.rest.crud.json.JsonCreate
import org.sirix.rest.crud.xml.XmlCreate
import java.nio.file.Path

class CreateMultipleResources(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val fileUploads = ctx.fileUploads()
        var isXmlFiles = false
        var isJsonFiles = false

        fileUploads.forEach { fileUpload ->
            when (fileUpload.contentType()) {
                "application/xml", "text/xml" -> {
                    if (isJsonFiles) {
                        ctx.fail(IllegalArgumentException("All uploaded files must be either of type XML or JSON."))
                        return ctx.currentRoute()
                    }

                    isXmlFiles = true
                }
                "application/json", "text/json" -> {
                    if (isXmlFiles) {
                        ctx.fail(IllegalArgumentException("All uploaded files must be either of type XML or JSON."))
                        return ctx.currentRoute()
                    }

                    isJsonFiles = true
                }
            }
        }

        if (isXmlFiles) XmlCreate(location, true).handle(ctx)
        else if (isJsonFiles) JsonCreate(location, true).handle(ctx)

        return ctx.currentRoute()
    }
}
