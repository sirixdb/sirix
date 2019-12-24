package org.sirix.rest.crud.json

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.withContext
import org.brackit.xquery.XQuery
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.json.JsonNodeReadOnlyTrx
import org.sirix.api.json.JsonResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.node.NodeKind
import org.sirix.rest.crud.History
import org.sirix.rest.crud.JsonLevelBasedSerializer
import org.sirix.rest.crud.QuerySerializer
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.xquery.JsonDBSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.json.*
import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

class JsonGet(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val dbName: String? = ctx.pathParam("database")
        val resName: String? = ctx.pathParam("resource")
        val jsonBody = ctx.bodyAsJson
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }

        get(dbName, ctx, resName, query, context, ctx.get("user") as User)

        return ctx.currentRoute()
    }

    private suspend fun get(
        dbName: String?, ctx: RoutingContext, resName: String?, query: String?,
        vertxContext: Context, user: User
    ) {
        val history = ctx.pathParam("history")

        if (history != null && dbName != null && resName != null) {
            vertxContext.executeBlockingAwait { _: Promise<Unit> ->
                History().serialize(ctx, location, dbName, resName)
            }

            return
        }

        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)
        val startRevision: String? = ctx.queryParam("start-revision").getOrNull(0)
        val endRevision: String? = ctx.queryParam("end-revision").getOrNull(0)
        val startRevisionTimestamp: String? = ctx.queryParam("start-revision-timestamp").getOrNull(0)
        val endRevisionTimestamp: String? = ctx.queryParam("end-revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        val database: Database<JsonResourceManager>
        try {
            database = Databases.openJsonDatabase(location.resolve(dbName))
        } catch (e: SirixUsageException) {
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            return
        }

        database.use {
            try {
                val manager = database.openResourceManager(resName)

                manager.use {
                    if (query != null && query.isNotEmpty()) {
                        queryResource(
                            dbName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                            vertxContext, user
                        )
                    } else {
                        val revisions: Array<Int> =
                            getRevisionsToSerialize(
                                startRevision, endRevision, startRevisionTimestamp,
                                endRevisionTimestamp, manager, revision, revisionTimestamp
                            )

                        serializeResource(manager, revisions, nodeId?.toLongOrNull(), ctx)
                    }
                }
            } catch (e: SirixUsageException) {
                ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
                return
            }

        }
    }

    private fun getRevisionsToSerialize(
        startRevision: String?, endRevision: String?, startRevisionTimestamp: String?,
        endRevisionTimestamp: String?, manager: JsonResourceManager, revision: String?,
        revisionTimestamp: String?
    ): Array<Int> {
        return when {
            startRevision != null && endRevision != null -> parseIntRevisions(startRevision, endRevision)
            startRevisionTimestamp != null && endRevisionTimestamp != null -> {
                val tspRevisions = parseTimestampRevisions(startRevisionTimestamp, endRevisionTimestamp)
                getRevisionNumbers(manager, tspRevisions).toList().toTypedArray()
            }
            else -> getRevisionNumber(revision, revisionTimestamp, manager)
        }
    }

    private suspend fun queryResource(
        dbName: String?, database: Database<JsonResourceManager>, revision: String?,
        revisionTimestamp: String?, manager: JsonResourceManager, ctx: RoutingContext,
        nodeId: String?, query: String, vertxContext: Context, user: User
    ) {

        withContext(vertxContext.dispatcher()) {
            val dbCollection = JsonDBCollection(dbName, database)

            dbCollection.use {
                val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)

                val trx: JsonNodeReadOnlyTrx
                try {
                    trx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    trx.use {
                        if (nodeId == null)
                            trx.moveToFirstChild()
                        else
                            trx.moveTo(nodeId.toLong())

                        val jsonItem: JsonDBItem = when {
                            trx.kind === NodeKind.ARRAY -> JsonDBArray(trx, dbCollection)
                            trx.kind === NodeKind.OBJECT -> JsonDBObject(trx, dbCollection)
                            else -> throw IllegalStateException()
                        }

                        val startResultSeqIndex = ctx.queryParam("startResultSeqIndex").getOrElse(0) { null }
                        val endResultSeqIndex = ctx.queryParam("endResultSeqIndex").getOrElse(0) { null }

                        xquery(
                            query,
                            jsonItem,
                            ctx,
                            vertxContext,
                            user,
                            startResultSeqIndex?.toLong(),
                            endResultSeqIndex?.toLong()
                        )
                    }
                } catch (e: SirixUsageException) {
                    ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
                }
            }
        }
    }

    private fun getRevisionNumber(rev: String?, revTimestamp: String?, manager: JsonResourceManager): Array<Int> {
        return if (rev != null) {
            arrayOf(rev.toInt())
        } else if (revTimestamp != null) {
            var revision = getRevisionNumber(manager, revTimestamp)
            if (revision == 0)
                arrayOf(++revision)
            else
                arrayOf(revision)
        } else {
            arrayOf(manager.mostRecentRevisionNumber)
        }
    }

    suspend fun xquery(
        query: String, node: JsonDBItem?, routingContext: RoutingContext, vertxContext: Context,
        user: User, startResultSeqIndex: Long?, endResultSeqIndex: Long?
    ) {
        vertxContext.executeBlockingAwait { promise: Promise<Nothing> ->
            // Initialize queryResource context and store.
            val dbStore = JsonSessionDBStore(routingContext, BasicJsonDBStore.newBuilder().build(), user)

            dbStore.use {
                val queryCtx = SirixQueryContext.createWithJsonStore(dbStore)

                node.let { queryCtx.contextItem = node }

                val out = StringBuilder()

                executeQueryAndSerialize(dbStore, out, startResultSeqIndex, query, queryCtx, endResultSeqIndex)

                val body = out.toString()

                routingContext.response().setStatusCode(200)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .putHeader(HttpHeaders.CONTENT_LENGTH, body.toByteArray(StandardCharsets.UTF_8).size.toString())
                    .write(body)
                    .end()
            }

            promise.complete(null)
        }
    }

    private fun executeQueryAndSerialize(
        dbStore: JsonSessionDBStore,
        out: StringBuilder,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext?,
        endResultSeqIndex: Long?
    ) {
        SirixCompileChain.createWithJsonStore(dbStore).use { sirixCompileChain ->
            if (startResultSeqIndex == null) {
                val serializer = JsonDBSerializer(out, false)
                XQuery(sirixCompileChain, query).prettyPrint().serialize(queryCtx, serializer)
            } else {
                QuerySerializer.serializePaginated(
                    sirixCompileChain,
                    query,
                    queryCtx,
                    startResultSeqIndex,
                    endResultSeqIndex,
                    JsonDBSerializer(out, true)
                ) { serializer, startItem -> serializer.serialize(startItem) }
            }
        }
    }

    private fun getRevisionNumber(manager: JsonResourceManager, revision: String): Int {
        val revisionDateTime = LocalDateTime.parse(revision)
        val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
        return manager.getRevisionNumber(zdt.toInstant())
    }

    private fun getRevisionNumbers(
        manager: JsonResourceManager,
        revisions: Pair<LocalDateTime, LocalDateTime>
    ): Array<Int> {
        val zdtFirstRevision = revisions.first.atZone(ZoneId.systemDefault())
        val zdtLastRevision = revisions.second.atZone(ZoneId.systemDefault())
        var firstRevisionNumber = manager.getRevisionNumber(zdtFirstRevision.toInstant())
        var lastRevisionNumber = manager.getRevisionNumber(zdtLastRevision.toInstant())

        if (firstRevisionNumber == 0) ++firstRevisionNumber
        if (lastRevisionNumber == 0) ++lastRevisionNumber

        return (firstRevisionNumber..lastRevisionNumber).toSet().toTypedArray()
    }

    private fun serializeResource(
        manager: JsonResourceManager, revisions: Array<Int>, nodeId: Long?,
        ctx: RoutingContext
    ) {
        val out = StringWriter()

        val serializerBuilder = JsonSerializer.newBuilder(manager, out).revisions(revisions.toIntArray())

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        val withMetaData: String? = ctx.queryParam("withMetaData").getOrNull(0)
        val maxLevel: String? = ctx.queryParam("maxLevel").getOrNull(0)

        if (withMetaData != null)
            serializerBuilder.withMetaData(withMetaData.toBoolean())

        if (maxLevel != null)
            serializerBuilder.maxLevel(maxLevel.toLong())

        val serializer = serializerBuilder.build()

        JsonSerializeHelper().serialize(serializer, out, ctx, manager, nodeId)
    }

    private fun parseIntRevisions(startRevision: String, endRevision: String): Array<Int> {
        return (startRevision.toInt()..endRevision.toInt()).toSet().toTypedArray()
    }

    private fun parseTimestampRevisions(
        startRevision: String,
        endRevision: String
    ): Pair<LocalDateTime, LocalDateTime> {
        val firstRevisionDateTime = LocalDateTime.parse(startRevision)
        val lastRevisionDateTime = LocalDateTime.parse(endRevision)

        return Pair(firstRevisionDateTime, lastRevisionDateTime)
    }
}
