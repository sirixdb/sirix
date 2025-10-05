package io.sirix.rest.crud.json

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.sirix.access.Databases
import io.sirix.access.trx.node.HashType
import io.sirix.access.trx.node.json.objectvalue.*
import io.sirix.api.json.JsonNodeTrx
import io.sirix.rest.crud.Revisions
import io.sirix.rest.crud.SirixDBUser
import io.sirix.rest.crud.json.JsonInsertionMode.Companion.getInsertionModeByName
import io.sirix.service.json.JsonNumber
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.service.json.shredder.JsonShredder
import java.io.IOException
import java.io.StringWriter
import java.nio.file.Path
import java.time.Instant
import java.util.*

@Suppress("unused")
enum class JsonInsertionMode {
    ASFIRSTCHILD {
        override fun insertSubtree(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertSubtreeAsFirstChild(jsonReader, JsonNodeTrx.Commit.NO)
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertString(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertStringValueAsFirstChild(jsonReader.nextString())
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertNumber(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertNumberValueAsFirstChild(JsonNumber.stringToNumber(jsonReader.nextString()))
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertNull(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            jsonReader.nextNull()
            wtx.insertNullValueAsFirstChild()
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertBoolean(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertBooleanValueAsFirstChild(jsonReader.nextBoolean())
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertObjectRecord(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertObjectRecordAsFirstChild(jsonReader.nextName(), getObjectRecordValue(jsonReader))
            wtx.commit(commitMessage, commitTimestamp)
        }
    },
    ASRIGHTSIBLING {
        override fun insertSubtree(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertSubtreeAsRightSibling(jsonReader, JsonNodeTrx.Commit.NO)
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertString(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertStringValueAsRightSibling(jsonReader.nextString())
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertNumber(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertNumberValueAsRightSibling(JsonNumber.stringToNumber(jsonReader.nextString()))
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertNull(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            jsonReader.nextNull()
            wtx.insertNullValueAsRightSibling()
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertBoolean(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertBooleanValueAsRightSibling(jsonReader.nextBoolean())
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertObjectRecord(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertObjectRecordAsRightSibling(jsonReader.nextName(), getObjectRecordValue(jsonReader))
            wtx.commit(commitMessage, commitTimestamp)
        }
    },
    ASLEFTSIBLING {
        override fun insertSubtree(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertSubtreeAsLeftSibling(jsonReader, JsonNodeTrx.Commit.NO)
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertString(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertStringValueAsLeftSibling(jsonReader.nextString())
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertNumber(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertNumberValueAsLeftSibling(JsonNumber.stringToNumber(jsonReader.nextString()))
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertNull(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            jsonReader.nextNull()
            wtx.insertNullValueAsLeftSibling()
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertBoolean(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertBooleanValueAsLeftSibling(jsonReader.nextBoolean())
            wtx.commit(commitMessage, commitTimestamp)
        }

        override fun insertObjectRecord(
            wtx: JsonNodeTrx,
            jsonReader: JsonReader,
            commitMessage: String?,
            commitTimestamp: Instant?
        ) {
            wtx.insertObjectRecordAsLeftSibling(jsonReader.nextName(), getObjectRecordValue(jsonReader))
            wtx.commit(commitMessage, commitTimestamp)
        }
    };

    @Throws(IOException::class)
    fun getObjectRecordValue(jsonReader: JsonReader): ObjectRecordValue<*> {
        val value: ObjectRecordValue<*> = when (jsonReader.peek()) {
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

    abstract fun insertSubtree(
        wtx: JsonNodeTrx,
        jsonReader: JsonReader,
        commitMessage: String?,
        commitTimestamp: Instant?
    )

    abstract fun insertString(
        wtx: JsonNodeTrx,
        jsonReader: JsonReader,
        commitMessage: String?,
        commitTimestamp: Instant?
    )

    abstract fun insertNumber(
        wtx: JsonNodeTrx,
        jsonReader: JsonReader,
        commitMessage: String?,
        commitTimestamp: Instant?
    )

    abstract fun insertNull(wtx: JsonNodeTrx, jsonReader: JsonReader, commitMessage: String?, commitTimestamp: Instant?)

    abstract fun insertBoolean(
        wtx: JsonNodeTrx,
        jsonReader: JsonReader,
        commitMessage: String?,
        commitTimestamp: Instant?
    )

    abstract fun insertObjectRecord(
        wtx: JsonNodeTrx,
        jsonReader: JsonReader,
        commitMessage: String?,
        commitTimestamp: Instant?
    )

    companion object {
        fun getInsertionModeByName(name: String) = valueOf(name.uppercase(Locale.getDefault()))
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

        val body = ctx.body().asString()

        update(databaseName, resource, nodeId?.toLongOrNull(), insertionMode, body, ctx)

        return ctx.currentRoute()
    }

    private suspend fun update(
        databaseName: String, resPathName: String, nodeId: Long?, insertionModeAsString: String?,
        resFileToStore: String, ctx: RoutingContext
    ) {
        val vertxContext = ctx.vertx().orCreateContext

        vertxContext.executeBlocking { promise: Promise<Nothing> ->
            val sirixDBUser = SirixDBUser.create(ctx)
            val dbFile = location.resolve(databaseName)

            var body: String? = null
            val database = Databases.openJsonDatabase(dbFile, sirixDBUser)

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
                    val revision = wtx.revisionNumber
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

                            if (wtx.hash != hashCode.toLong()) {
                                throw IllegalArgumentException("Someone might have changed the resource in the meantime.")
                            }
                        }

                        if (insertionModeAsString == null) {
                            throw IllegalArgumentException("Insertion mode must be given.")
                        }

                        val jsonReader = JsonShredder.createStringReader(resFileToStore)

                        val insertionModeByName = getInsertionModeByName(insertionModeAsString)

                        @Suppress("unused")
                        if (jsonReader.peek() != JsonToken.BEGIN_ARRAY && jsonReader.peek() != JsonToken.BEGIN_OBJECT) {
                            when (jsonReader.peek()) {
                                JsonToken.STRING -> insertionModeByName.insertString(wtx, jsonReader, commitMessage, commitTimestamp)
                                JsonToken.NULL -> insertionModeByName.insertNull(wtx, jsonReader, commitMessage, commitTimestamp)
                                JsonToken.NUMBER -> insertionModeByName.insertNumber(wtx, jsonReader, commitMessage, commitTimestamp)
                                JsonToken.BOOLEAN -> insertionModeByName.insertBoolean(wtx, jsonReader, commitMessage, commitTimestamp)
                                JsonToken.NAME -> insertionModeByName.insertObjectRecord(wtx, jsonReader, commitMessage, commitTimestamp)
                                else -> throw IllegalStateException()
                            }
                        } else {
                            insertionModeByName.insertSubtree(wtx, jsonReader, commitMessage, commitTimestamp)
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

                        body = JsonSerializeHelper().serialize(
                            serializer,
                            out,
                            ctx,
                            manager,
                            intArrayOf(revision),
                            nodeId
                        )
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
