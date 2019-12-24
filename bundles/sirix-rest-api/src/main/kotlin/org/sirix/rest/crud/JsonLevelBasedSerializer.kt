package org.sirix.rest.crud

import io.vertx.ext.web.RoutingContext
import org.sirix.api.json.JsonResourceManager
import org.sirix.rest.crud.json.JsonSerializeHelper
import org.sirix.service.json.serialize.JsonSerializer
import java.io.StringWriter

class JsonLevelBasedSerializer {
    fun serialize(
        ctx: RoutingContext,
        manager: JsonResourceManager
    ) {
        val revisionList = ctx.queryParam("revision")
        val levelList = ctx.queryParam("maxLevel")
        val nodeIdList = ctx.queryParam("nodeId")

        val out = StringWriter()

        val serializerBuilder = JsonSerializer.newBuilder(manager, out)

        if (nodeIdList.isNotEmpty())
            serializerBuilder.startNodeKey(nodeIdList[0].toLong())
        if (revisionList.isNotEmpty())
            serializerBuilder.revisions(intArrayOf(revisionList[0].toInt()))
        if (levelList.isNotEmpty())
            serializerBuilder.maxLevel(levelList[0].toLong())

        val serializer = serializerBuilder.build()

        val nodeId = nodeIdList.getOrElse(0) {
            null
        }

        JsonSerializeHelper().serialize(serializer, out, ctx, manager, nodeId?.toLong())
    }
}