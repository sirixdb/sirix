package io.sirix.rest.crud

import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.sirix.rest.crud.json.JsonCreate
import io.sirix.rest.crud.xml.XmlCreate
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
                        throw IllegalArgumentException("All uploaded files must be either of type XML or JSON.")
                    }

                    isXmlFiles = true
                }
                "application/json", "text/json" -> {
                    if (isXmlFiles) {
                        throw IllegalArgumentException("All uploaded files must be either of type XML or JSON.")
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
