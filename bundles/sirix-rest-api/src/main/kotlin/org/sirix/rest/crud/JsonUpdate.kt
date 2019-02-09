package org.sirix.rest.crud

import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.api.xdm.XdmNodeTrx
import org.sirix.rest.XdmSerializeHelper
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.service.xml.shredder.XmlShredder
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import javax.xml.stream.XMLEventReader
import org.sirix.service.json.shredder.JsonShredder
import java.io.StringWriter
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.rest.JsonSerializeHelper
import com.google.gson.stream.JsonReader
import org.sirix.api.json.JsonNodeTrx

enum class JsonInsertionMode {
    ASFIRSTCHILD {
        override fun insert(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertSubtreeAsFirstChild(jsonReader)
        }
    },
    ASRIGHTSIBLING {
        override fun insert(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertSubtreeAsRightSibling(jsonReader)
        }
    };

    abstract fun insert(wtx: JsonNodeTrx, jsonReader: JsonReader)

    companion object {
        fun getInsertionModeByName(name: String) = valueOf(name.toUpperCase())
    }
}

class JsonUpdate(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val dbName = ctx.pathParam("database")

        val resName = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val insertionMode: String? = ctx.queryParam("insert").getOrNull(0)

        if (dbName == null || resName == null) {
            ctx.fail(IllegalArgumentException("Database name and resource name not given."))
        }

        val body = ctx.bodyAsString

        update(dbName, resName, nodeId?.toLongOrNull(), insertionMode, body, ctx)

        return ctx.currentRoute()
    }

    private suspend fun update(dbPathName: String, resPathName: String, nodeId: Long?, insertionMode: String?,
                               resFileToStore: String, ctx: RoutingContext) {
        val vertxContext = ctx.vertx().orCreateContext

        vertxContext.executeBlockingAwait(Handler<Future<Nothing>> {
            val dbFile = location.resolve(dbPathName)

            val database = Databases.openJsonDatabase(dbFile)

            database.use {
                val manager = database.openResourceManager(resPathName)

                val wtx = manager.beginNodeTrx()
                wtx.use {
                    if (nodeId != null)
                        wtx.moveTo(nodeId)

                    if (wtx.isDocumentRoot && wtx.hasFirstChild())
                        wtx.moveToFirstChild()

                    val jsonReader = JsonShredder.createStringReader(resFileToStore)

                    if (insertionMode != null)
                        JsonInsertionMode.getInsertionModeByName(insertionMode).insert(wtx, jsonReader)
                }

                val out = StringWriter()
                val serializerBuilder = JsonSerializer.newBuilder(manager, out)
                val serializer = serializerBuilder.build()

                JsonSerializeHelper().serialize(serializer, out, ctx)
            }

            it.complete(null)
        })
    }
}