package org.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.json.JsonNodeTrx
import org.sirix.rest.crud.SirixDBUser
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.json.shredder.JsonShredder
import java.io.StringWriter
import java.math.BigInteger
import java.nio.file.Path

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
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

            database.use {
                val manager = database.openResourceManager(resPathName)

                manager.use {
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

                        val jsonReader = JsonShredder.createStringReader(resFileToStore)

                        if (insertionMode != null)
                            JsonInsertionMode.getInsertionModeByName(insertionMode).insert(wtx, jsonReader)

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
                        val out = StringWriter()
                        val serializerBuilder = JsonSerializer.newBuilder(manager, out)
                        val serializer = serializerBuilder.build()

                        JsonSerializeHelper().serialize(serializer, out, ctx, manager, nodeId)
                    }
                }
            }

            promise.complete(null)
        }
    }
}
