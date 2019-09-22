package org.sirix.rest.crud.xml

import io.vertx.core.Future
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.xml.XmlNodeTrx
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.service.xml.shredder.XmlShredder
import java.io.ByteArrayOutputStream
import java.math.BigInteger
import java.nio.file.Path
import javax.xml.stream.XMLEventReader
import io.vertx.core.http.HttpHeaders
import org.sirix.rest.crud.SirixDBUtils

enum class XmlInsertionMode {
    ASFIRSTCHILD {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsFirstChild(xmlReader)
        }
    },
    ASRIGHTSIBLING {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsRightSibling(xmlReader)
        }
    },
    ASLEFTSIBLING {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.insertSubtreeAsLeftSibling(xmlReader)
        }
    },
    REPLACE {
        override fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader) {
            wtx.replaceNode(xmlReader)
        }
    };

    abstract fun insert(wtx: XmlNodeTrx, xmlReader: XMLEventReader)

    companion object {
        fun getInsertionModeByName(name: String) = valueOf(name.toUpperCase())
    }
}

class XmlUpdate(private val location: Path) {
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

        vertxContext.executeBlockingAwait { future: Future<Nothing> ->
            
            val sirixDBUser = SirixDBUtils.createSirixDBUser(ctx)
            val dbFile = location.resolve(dbPathName)
            val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

            database.use {
                val manager = database.openResourceManager(resPathName)

                val wtx = manager.beginNodeTrx()
                wtx.use {
                    if (nodeId != null)
                        wtx.moveTo(nodeId)

                    if (wtx.isDocumentRoot && wtx.hasFirstChild())
                        wtx.moveToFirstChild()

                    if (manager.resourceConfig.hashType != HashType.NONE && !wtx.isDocumentRoot) {
                        val hashCode = ctx.request().getHeader(HttpHeaders.ETAG)

                        if (hashCode == null) {
                            ctx.fail(IllegalStateException("Hash code is missing in ETag HTTP-Header."))
                        }

                        if (wtx.hash != BigInteger(hashCode)) {
                            ctx.fail(IllegalArgumentException("Someone might have changed the resource in the meantime."))
                        }
                    }

                    val xmlReader = XmlShredder.createStringReader(resFileToStore)

                    if (insertionMode != null)
                        XmlInsertionMode.getInsertionModeByName(insertionMode).insert(wtx, xmlReader)
                    else
                        wtx.replaceNode(xmlReader)
                }

                val out = ByteArrayOutputStream()
                val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out)
                val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

                XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
            }

            future.complete(null)
        }
    }
}