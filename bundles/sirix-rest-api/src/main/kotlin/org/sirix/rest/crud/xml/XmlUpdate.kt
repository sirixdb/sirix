package org.sirix.rest.crud.xml

import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.xml.XmlNodeTrx
import org.sirix.rest.crud.Revisions
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.service.xml.shredder.XmlShredder
import java.io.ByteArrayOutputStream
import java.math.BigInteger
import java.nio.file.Path
import java.time.Instant
import java.util.*
import javax.xml.stream.XMLEventReader

enum class XmlInsertionMode {
    ASFIRSTCHILD {
        override fun insert(
            wtx: XmlNodeTrx, xmlReader: XMLEventReader, commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertSubtreeAsFirstChild(xmlReader)
        }
    },
    ASRIGHTSIBLING {
        override fun insert(
            wtx: XmlNodeTrx, xmlReader: XMLEventReader, commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertSubtreeAsRightSibling(xmlReader)
        }
    },
    ASLEFTSIBLING {
        override fun insert(
            wtx: XmlNodeTrx, xmlReader: XMLEventReader, commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertSubtreeAsLeftSibling(xmlReader)
        }
    },
    REPLACE {
        override fun insert(
            wtx: XmlNodeTrx, xmlReader: XMLEventReader, commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.replaceNode(xmlReader)
        }
    };

    abstract fun insert(
        wtx: XmlNodeTrx, xmlReader: XMLEventReader, commitMessage: String?,
        commitTimestamp: Instant?
    )

    companion object {
        fun getInsertionModeByName(name: String) = valueOf(name.uppercase(Locale.getDefault()))
    }
}

class XmlUpdate(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val databaseName = ctx.pathParam("database")

        val resource = ctx.pathParam("resource")
        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val insertionMode: String? = ctx.queryParam("insert").getOrNull(0)

        if (databaseName == null || resource == null) {
            throw IllegalArgumentException("Database name and resource name not given.")
        }

        val body = ctx.body().asString()

        update(databaseName, resource, nodeId?.toLongOrNull(), insertionMode, body, ctx)

        return ctx.currentRoute()
    }

    private suspend fun update(
        databaseName: String, resPathName: String, nodeId: Long?, insertionMode: String?,
        resFileToStore: String, ctx: RoutingContext
    ) {
        val vertxContext = ctx.vertx().orCreateContext

        vertxContext.executeBlocking { promise: Promise<Nothing> ->
            val sirixDBUser = SirixDBUser.create(ctx)
            val dbFile = location.resolve(databaseName)

            var body: String? = null

            val database = Databases.openXmlDatabase(dbFile, sirixDBUser)

            database.use {
                val manager = database.beginResourceSession(resPathName)

                manager.use {
                    val commitMessage = ctx.queryParam("commitMessage").getOrNull(0)
                    val commitTimestampAsString = ctx.queryParam("commitTimestamp").getOrNull(0)
                    val commitTimestamp = if (commitTimestampAsString == null) {
                        null
                    } else {
                        Revisions.parseRevisionTimestamp(commitTimestampAsString).toInstant()
                    }

                    val wtx = manager.beginNodeTrx()
                    val (maxNodeKey, hash) = wtx.use {
                        if (nodeId != null) {
                            wtx.moveTo(nodeId)
                        }

                        if (wtx.isDocumentRoot && wtx.hasFirstChild())
                            wtx.moveToFirstChild()

                        if (manager.resourceConfig.hashType != HashType.NONE && !wtx.isDocumentRoot) {
                            val hashCode = ctx.request().getHeader(HttpHeaders.ETAG)
                                ?: throw IllegalStateException("Hash code is missing in ETag HTTP-Header.")

                            if (wtx.hash != hashCode.toLong()) {
                                throw IllegalArgumentException("Someone might have changed the resource in the meantime.")
                            }
                        }

                        val xmlReader = XmlShredder.createStringReader(resFileToStore)

                        if (insertionMode != null)
                            XmlInsertionMode.getInsertionModeByName(insertionMode)
                                .insert(wtx, xmlReader, commitMessage, commitTimestamp)
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
                            ctx.response()
                        } else {
                            ctx.response().putHeader(HttpHeaders.ETAG, hash.toString())
                        }
                    } else {
                        val out = ByteArrayOutputStream()
                        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out)

                        val serializer =
                            serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

                        body = XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
                    }
                }
            }

            if (body != null) {
                ctx.response().end(body)
            } else {
                ctx.response().end()
            }

            promise.complete(null)
        }.await()
    }
}
