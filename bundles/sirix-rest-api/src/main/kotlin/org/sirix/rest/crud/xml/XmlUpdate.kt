package org.sirix.rest.crud.xml

import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.xml.XmlNodeTrx
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.service.xml.shredder.XmlShredder
import java.io.ByteArrayOutputStream
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import javax.xml.stream.XMLEventReader

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
        val databaseName = ctx.pathParam("database")

        val resource = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val insertionMode: String? = ctx.queryParam("insert").getOrNull(0)

        if (databaseName == null || resource == null) {
            ctx.fail(IllegalArgumentException("Database name and resource name not given."))
        }

        val body = ctx.bodyAsString

        update(databaseName, resource, nodeId?.toLongOrNull(), insertionMode, body, ctx)

        return ctx.currentRoute()
    }

    private suspend fun update(
        databaseName: String, resPathName: String, nodeId: Long?, insertionMode: String?,
        resFileToStore: String, ctx: RoutingContext
    ) {
        val vertxContext = ctx.vertx().orCreateContext

        vertxContext.executeBlockingAwait { promise: Promise<Nothing> ->

            val sirixDBUser = SirixDBUser.create(ctx)
            val dbFile = location.resolve(databaseName)
            val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

            database.use {
                val manager = database.openResourceManager(resPathName)

                val wtx = manager.beginNodeTrx()
                val (maxNodeKey, hash) = wtx.use {
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

                    if (nodeId != null)
                        wtx.moveTo(nodeId)

                    if (wtx.isDocumentRoot && wtx.hasFirstChild())
                        wtx.moveToFirstChild()

                    Pair(wtx.maxNodeKey, wtx.hash)
                }

                if (maxNodeKey > 5000) {
                    ctx.response().statusCode = 200

                    if (manager.resourceConfig.hashType == HashType.NONE) {
                        ctx.response().end()
                    } else {
                        ctx.response().putHeader(HttpHeaders.ETAG, hash.toString()).end()
                    }
                } else {
                    val out = ByteArrayOutputStream()
                    val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out)

                    val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

                    XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
                }
            }

            promise.complete(null)
        }
    }
}