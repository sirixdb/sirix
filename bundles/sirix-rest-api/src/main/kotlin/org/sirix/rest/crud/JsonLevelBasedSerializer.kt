package org.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import org.sirix.access.Databases
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class JsonLevelBasedSerializer {
    fun serialize(
        ctx: RoutingContext,
        location: Path,
        databaseName: String,
        resourceName: String
    ) {
        val database = Databases.openJsonDatabase(location.resolve(databaseName))

        database.use {
            val manager = database.openResourceManager(resourceName)

            manager.use {
                val buffer = StringBuilder()

                val revisionList = ctx.queryParam("revision")
                val levelList = ctx.queryParam("level")

                val rtx =
                    if (revisionList.isNotEmpty()) {
                        manager.beginNodeReadOnlyTrx(revisionList[0].toInt())
                    } else {
                        manager.beginNodeReadOnlyTrx()
                    }

                val level = if (levelList.isNotEmpty()) levelList[0].toInt() else Integer.MAX_VALUE


                val content = buffer.toString()

                ctx.response().setStatusCode(200)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
                    .write(content)
                    .end()
            }
        }
    }
}