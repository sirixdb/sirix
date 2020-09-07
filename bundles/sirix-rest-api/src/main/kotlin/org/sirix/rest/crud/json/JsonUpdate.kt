package org.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.access.trx.node.HashType
import org.sirix.access.trx.node.json.objectvalue.*
import org.sirix.api.json.JsonNodeTrx
import org.sirix.rest.crud.SirixDBUser
import org.sirix.rest.crud.json.JsonInsertionMode.Companion.getInsertionModeByName
import org.sirix.service.json.JsonNumber
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.service.json.shredder.JsonShredder
import java.io.IOException
import java.io.StringWriter
import java.math.BigInteger
import java.nio.file.Path

enum class JsonInsertionMode {
    ASFIRSTCHILD {
        override fun insertSubtree(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertSubtreeAsFirstChild(jsonReader)
        }

        override fun insertString(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertStringValueAsFirstChild(jsonReader.nextString())
            wtx.commit()
        }

        override fun insertNumber(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertNumberValueAsFirstChild(JsonNumber.stringToNumber(jsonReader.nextString()))
            wtx.commit()
        }

        override fun insertNull(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            jsonReader.nextNull()
            wtx.insertNullValueAsFirstChild()
            wtx.commit()
        }

        override fun insertBoolean(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertBooleanValueAsFirstChild(jsonReader.nextBoolean())
            wtx.commit()
        }

        override fun insertObjectRecord(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertObjectRecordAsFirstChild(jsonReader.nextName(), getObjectRecordValue(jsonReader))
            wtx.commit()
        }
    },
    ASRIGHTSIBLING {
        override fun insertSubtree(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertSubtreeAsRightSibling(jsonReader)
        }

        override fun insertString(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertStringValueAsRightSibling(jsonReader.nextString())
            wtx.commit()
        }

        override fun insertNumber(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertNumberValueAsRightSibling(JsonNumber.stringToNumber(jsonReader.nextString()))
            wtx.commit()
        }

        override fun insertNull(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            jsonReader.nextNull()
            wtx.insertNullValueAsRightSibling()
            wtx.commit()
        }

        override fun insertBoolean(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertBooleanValueAsRightSibling(jsonReader.nextBoolean())
            wtx.commit()
        }

        override fun insertObjectRecord(wtx: JsonNodeTrx, jsonReader: JsonReader) {
            wtx.insertObjectRecordAsRightSibling(jsonReader.nextName(), getObjectRecordValue(jsonReader))
            wtx.commit()
        }
    };

    @Throws(IOException::class)
    fun getObjectRecordValue(jsonReader: JsonReader): ObjectRecordValue<*>? {
        val nextToken: JsonToken = jsonReader.peek()
        val value: ObjectRecordValue<*>
        value = when (nextToken) {
            JsonToken.BEGIN_OBJECT -> {
                jsonReader.beginObject()
                ObjectValue()
            }
            JsonToken.BEGIN_ARRAY -> {
                jsonReader.beginArray()
                ArrayValue()
            }
            JsonToken.BOOLEAN -> {
                val booleanVal: Boolean = jsonReader.nextBoolean()
                BooleanValue(booleanVal)
            }
            JsonToken.STRING -> {
                val stringVal: String = jsonReader.nextString()
                StringValue(stringVal)
            }
            JsonToken.NULL -> {
                jsonReader.nextNull()
                NullValue()
            }
            JsonToken.NUMBER -> {
                val numberVal: Number = JsonNumber.stringToNumber(jsonReader.nextString())
                NumberValue(numberVal)
            }
            JsonToken.END_ARRAY, JsonToken.END_DOCUMENT, JsonToken.END_OBJECT, JsonToken.NAME -> throw AssertionError()
            else -> throw AssertionError()
        }
        return value
    }

    abstract fun insertSubtree(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertString(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertNumber(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertNull(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertBoolean(wtx: JsonNodeTrx, jsonReader: JsonReader)

    abstract fun insertObjectRecord(wtx: JsonNodeTrx, jsonReader: JsonReader)

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
            throw IllegalArgumentException("Database name and resource name not given.")
        }

        val body = ctx.bodyAsString

        update(databaseName, resource, nodeId?.toLongOrNull(), insertionMode, body, ctx)

        return ctx.currentRoute()
    }

    private suspend fun update(
        databaseName: String, resPathName: String, nodeId: Long?, insertionModeAsString: String?,
        resFileToStore: String, ctx: RoutingContext
    ) {
        val vertxContext = ctx.vertx().orCreateContext

        vertxContext.executeBlockingAwait { promise: Promise<Nothing> ->
            val sirixDBUser = SirixDBUser.create(ctx)
            val dbFile = location.resolve(databaseName)

            var body: String? = null
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

            database.use {
                val manager = database.openResourceManager(resPathName)

                manager.use {
                    val wtx = manager.beginNodeTrx()
                    val (maxNodeKey, hash) = wtx.use {
                        if (nodeId != null) {
                            wtx.moveTo(nodeId)
                        }

                        if (wtx.isDocumentRoot && wtx.hasFirstChild()) {
                            wtx.moveToFirstChild()
                        }

                        if (manager.resourceConfig.hashType != HashType.NONE && !wtx.isDocumentRoot) {
                            val hashCode = ctx.request().getHeader(HttpHeaders.ETAG)
                                ?: throw IllegalStateException("Hash code is missing in ETag HTTP-Header.")

                            if (wtx.hash != BigInteger(hashCode)) {
                                throw IllegalArgumentException("Someone might have changed the resource in the meantime.")
                            }
                        }

                        if (insertionModeAsString == null) {
                            throw IllegalArgumentException("Insertion mode must be given.")
                        }

                        val jsonReader = JsonShredder.createStringReader(resFileToStore)

                        val insertionModeByName = getInsertionModeByName(insertionModeAsString)

                        if (jsonReader.peek() != JsonToken.BEGIN_ARRAY && jsonReader.peek() != JsonToken.BEGIN_OBJECT) {
                            when (jsonReader.peek()) {
                                JsonToken.STRING -> insertionModeByName.insertString(wtx, jsonReader)
                                JsonToken.NULL -> insertionModeByName.insertNull(wtx, jsonReader)
                                JsonToken.NUMBER -> insertionModeByName.insertNumber(wtx, jsonReader)
                                JsonToken.BOOLEAN -> insertionModeByName.insertBoolean(wtx, jsonReader)
                                JsonToken.NAME -> insertionModeByName.insertObjectRecord(wtx, jsonReader)
                                else -> throw IllegalStateException()
                            }
                        } else {
                            insertionModeByName.insertSubtree(wtx, jsonReader)
                        }

                        if (nodeId != null) {
                            wtx.moveTo(nodeId)
                        }

                        if (wtx.isDocumentRoot && wtx.hasFirstChild()) {
                            wtx.moveToFirstChild()
                        }

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
                        val out = StringWriter()
                        val serializerBuilder = JsonSerializer.newBuilder(manager, out)
                        val serializer = serializerBuilder.build()

                        body = JsonSerializeHelper().serialize(serializer, out, ctx, manager, nodeId)
                    }
                }
            }

            if (body != null) {
                ctx.response().end(body)
            } else {
                ctx.response().end()
            }

            promise.complete(null)
        }
    }
}
