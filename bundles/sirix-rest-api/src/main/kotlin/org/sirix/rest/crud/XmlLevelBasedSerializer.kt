package org.sirix.rest.crud

import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.RoutingContext
import org.sirix.access.Databases
import org.sirix.service.xml.serialize.XmlSerializer
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class XmlLevelBasedSerializer {
    fun serialize(
        ctx: RoutingContext,
        location: Path,
        databaseName: String,
        resourceName: String
    ) {
        val database = Databases.openXmlDatabase(location.resolve(databaseName))

        database.use {
            val manager = database.openResourceManager(resourceName)

            manager.use {
                val buffer = StringBuilder()

                val revisionList = ctx.queryParam("revision")
                val levelList = ctx.queryParam("level")
                val nodeIdList = ctx.queryParam("nodeId")


                val out = ByteArrayOutputStream()

                val serializerBuilder = XmlSerializer.newBuilder(manager, out)

                if (nodeIdList.isNotEmpty())
                    serializerBuilder.startNodeKey(nodeIdList[0].toLong())
                if (revisionList.isNotEmpty())
                    serializerBuilder.revisions(intArrayOf(revisionList[0].toInt()))
//                if (levelList.isNotEmpty())
//                    serializerBuilder.maxLevel(levelList[0].toLong())


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