package org.sirix.rest.crud

import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext
import org.sirix.access.Databases
import org.sirix.rest.Serialize
import org.sirix.service.xml.serialize.XMLSerializer
import org.sirix.service.xml.shredder.XMLShredder
import java.io.ByteArrayOutputStream
import java.nio.file.Path

class Update(private val location: Path) : Handler<RoutingContext> {
    override fun handle(ctx: RoutingContext) {
        val dbName = ctx.pathParam("database")
        val resName = ctx.pathParam("resource")
        val nodeId : String? = if (ctx.queryParam("nodeId").isEmpty()) null else ctx.queryParam("nodeId")[0]

        if (dbName == null || resName == null)
            ctx.fail(IllegalArgumentException("Database name and resource name not given."))

        val body = ctx.bodyAsString

        update(dbName, resName, nodeId?.toLongOrNull(), body, ctx)
    }

    private fun update(dbPathName: String, resPathName: String, nodeId: Long?, resFileToStore: String, ctx: RoutingContext) {
        val dbFile = location.resolve(dbPathName)

        val database = Databases.openDatabase(dbFile)

        database.use {
            val manager = database.getResourceManager(resPathName)

            val wtx = manager.beginNodeWriteTrx()
            wtx.use {
                if (nodeId != null)
                    wtx.moveTo(nodeId)

                wtx.replaceNode(XMLShredder.createStringReader(resFileToStore))
                wtx.commit()
            }

            val out = ByteArrayOutputStream()
            val serializerBuilder = XMLSerializer.XMLSerializerBuilder(manager, out)
            val serializer = serializerBuilder.emitIDs().emitRESTful().prettyPrint().build()

            Serialize().serializeXml(serializer, out, ctx)
        }
    }
}