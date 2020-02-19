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
import org.brackit.xquery.xdm.Item
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.rest.crud.QuerySerializer
import org.sirix.rest.crud.Revisions
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.xquery.JsonDBSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.json.*
import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class JsonGet(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName = ctx.pathParam("database")
        val resource: String? = ctx.pathParam("resource")
        val jsonBody = ctx.bodyAsJson
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }

        get(databaseName, ctx, resource, query, context, ctx.get("user") as User)

        return ctx.currentRoute()
    }

    private suspend fun get(
            databaseName: String, ctx: RoutingContext, resource: String?, query: String?,
            vertxContext: Context, user: User
    ) {
        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)
        val startRevision: String? = ctx.queryParam("start-revision").getOrNull(0)
        val endRevision: String? = ctx.queryParam("end-revision").getOrNull(0)
        val startRevisionTimestamp: String? = ctx.queryParam("start-revision-timestamp").getOrNull(0)
        val endRevisionTimestamp: String? = ctx.queryParam("end-revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        val database: Database<JsonResourceManager>
        try {
            database = Databases.openJsonDatabase(location.resolve(databaseName))
        } catch (e: SirixUsageException) {
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            return
        }

        database.use {
            try {
                val manager = database.openResourceManager(resource)

                manager.use {
                    if (query != null && query.isNotEmpty()) {
                        queryResource(
                                databaseName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                                vertxContext, user
                        )
                    } else {
                        val revisions: Array<Int> =
                                Revisions.getRevisionsToSerialize(
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

    private suspend fun queryResource(
            databaseName: String?, database: Database<JsonResourceManager>, revision: String?,
            revisionTimestamp: String?, manager: JsonResourceManager, ctx: RoutingContext,
            nodeId: String?, query: String, vertxContext: Context, user: User
    ) {
        withContext(vertxContext.dispatcher()) {
            val dbCollection = JsonDBCollection(databaseName, database)

            dbCollection.use {
                val revisionNumber = Revisions.getRevisionNumber(revision, revisionTimestamp, manager)

                try {
                    val trx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    trx.use {
                        if (nodeId == null)
                            trx.moveToFirstChild()
                        else
                            trx.moveTo(nodeId.toLong())

                        val jsonItem = JsonItemFactory().getSequence(trx, dbCollection)

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

    suspend fun xquery(
            query: String, node: Item?, routingContext: RoutingContext, vertxContext: Context,
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

    private fun serializeResource(
            manager: JsonResourceManager, revisions: Array<Int>, nodeId: Long?,
            ctx: RoutingContext
    ) {
        val out = StringWriter()

        val serializerBuilder = JsonSerializer.newBuilder(manager, out).revisions(revisions.toIntArray())

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        val withMetaData: String? = ctx.queryParam("withMetaData").getOrNull(0)
        val maxLevel: String? = ctx.queryParam("maxLevel").getOrNull(0)
        val prettyPrint: String? = ctx.queryParam("prettyPrint").getOrNull(0)

        if (withMetaData != null)
            serializerBuilder.withMetaData(withMetaData.toBoolean())

        if (maxLevel != null)
            serializerBuilder.maxLevel(maxLevel.toLong())

        if (prettyPrint != null)
            serializerBuilder.prettyPrint()

        val serializer = serializerBuilder.build()

        JsonSerializeHelper().serialize(serializer, out, ctx, manager, nodeId)
    }
}
