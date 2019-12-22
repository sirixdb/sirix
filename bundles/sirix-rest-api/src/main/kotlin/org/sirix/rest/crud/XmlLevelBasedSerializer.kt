package org.sirix.rest.crud

import io.vertx.ext.web.RoutingContext
import org.sirix.api.xml.XmlResourceManager
import org.sirix.rest.crud.xml.XmlSerializeHelper
import org.sirix.service.xml.serialize.XmlSerializer
import java.io.ByteArrayOutputStream
import java.nio.file.Path

class XmlLevelBasedSerializer {
    fun serialize(
        ctx: RoutingContext,
        manager: XmlResourceManager
    ) {
        val revisionList = ctx.queryParam("revision")
        val levelList = ctx.queryParam("maxLevel")
        val nodeIdList = ctx.queryParam("nodeId")

        val out = ByteArrayOutputStream()

        val serializerBuilder = XmlSerializer.newBuilder(manager, out)

        if (nodeIdList.isNotEmpty())
            serializerBuilder.startNodeKey(nodeIdList[0].toLong())
        if (revisionList.isNotEmpty())
            serializerBuilder.revisions(intArrayOf(revisionList[0].toInt()))
        if (levelList.isNotEmpty())
            serializerBuilder.maxLevel(levelList[0].toLong())

        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        val nodeId = nodeIdList.getOrElse(0) {
            null
        }

        XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId?.toLong())
    }
}