package org.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.api.json.JsonNodeTrx
import org.sirix.rest.crud.SirixDBUtils
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

    private suspend fun update(
        dbPathName: String, resPathName: String, nodeId: Long?, insertionMode: String?,
        resFileToStore: String, ctx: RoutingContext
    ) {
        val vertxContext = ctx.vertx().orCreateContext

        vertxContext.executeBlockingAwait { promise: Promise<Nothing> ->
            val sirixDBUser = SirixDBUtils.createSirixDBUser(ctx)
            val dbFile = location.resolve(dbPathName)
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

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

                    val jsonReader = JsonShredder.createStringReader(resFileToStore)

                    if (insertionMode != null)
                        JsonInsertionMode.getInsertionModeByName(insertionMode).insert(wtx, jsonReader)
                }

                val out = StringWriter()
                val serializerBuilder = JsonSerializer.newBuilder(manager, out)
                val serializer = serializerBuilder.build()

                JsonSerializeHelper().serialize(serializer, out, ctx, manager, nodeId)
            }

            promise.complete(null)
        }
    }
}