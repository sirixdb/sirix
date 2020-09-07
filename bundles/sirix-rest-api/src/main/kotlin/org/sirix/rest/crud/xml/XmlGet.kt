package org.sirix.rest.crud.xml

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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.brackit.xquery.XQuery
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.api.xml.XmlNodeReadOnlyTrx
import org.sirix.api.xml.XmlResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.rest.crud.QuerySerializer
import org.sirix.rest.crud.Revisions
import org.sirix.rest.crud.json.JsonSessionDBStore
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.xquery.JsonDBSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.XmlDBSerializer
import org.sirix.xquery.json.BasicJsonDBStore
import org.sirix.xquery.json.JsonDBCollection
import org.sirix.xquery.node.BasicXmlDBStore
import org.sirix.xquery.node.XmlDBCollection
import org.sirix.xquery.node.XmlDBNode
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class XmlGet(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName: String? = ctx.pathParam("database")
        val resource: String? = ctx.pathParam("resource")
        val jsonBody = ctx.bodyAsJson
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }

        withContext(context.dispatcher()) {
            get(databaseName, ctx, resource, query, context, ctx.get("user") as User)
        }

        return ctx.currentRoute()
    }

    private suspend fun get(
        databaseName: String?, ctx: RoutingContext, resource: String?, query: String?,
        vertxContext: Context, user: User
    ) {
        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)
        val startRevision: String? = ctx.queryParam("start-revision").getOrNull(0)
        val endRevision: String? = ctx.queryParam("end-revision").getOrNull(0)
        val startRevisionTimestamp: String? = ctx.queryParam("start-revision-timestamp").getOrNull(0)
        val endRevisionTimestamp: String? = ctx.queryParam("end-revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        val database: Database<XmlResourceManager>
        try {
            database = Databases.openXmlDatabase(location.resolve(databaseName))
        } catch (e: SirixUsageException) {
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            return
        }

        var body: String? = null

        database.use {
            try {
                val manager = database.openResourceManager(resource)

                manager.use {
                    if (query != null && query.isNotEmpty()) {
                        body = queryResource(
                            databaseName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                            vertxContext, user
                        )
                    } else {
                        val revisions: Array<Int> =
                            Revisions.getRevisionsToSerialize(
                                startRevision, endRevision, startRevisionTimestamp,
                                endRevisionTimestamp, manager, revision, revisionTimestamp
                            )

                        body = serializeResource(manager, revisions, nodeId?.toLongOrNull(), ctx)
                    }
                }
            } catch (e: SirixUsageException) {
                ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            }
        }

        if (body != null) {
            ctx.response().end(body)
        } else {
            ctx.response().end()
        }
    }

    private suspend fun queryResource(
        databaseName: String?, database: Database<XmlResourceManager>, revision: String?,
        revisionTimestamp: String?, manager: XmlResourceManager, ctx: RoutingContext,
        nodeId: String?, query: String, vertxContext: Context, user: User
    ): String? {
        val dbCollection = XmlDBCollection(databaseName, database)

        dbCollection.use {
            val revisionNumber = Revisions.getRevisionNumber(revision, revisionTimestamp, manager)

            val startResultSeqIndex = ctx.queryParam("startResultSeqIndex").getOrElse(0) { null }
            val endResultSeqIndex = ctx.queryParam("endResultSeqIndex").getOrElse(0) { null }

            return xquery(
                manager,
                dbCollection,
                nodeId,
                revisionNumber,
                query,
                ctx,
                vertxContext,
                user,
                startResultSeqIndex?.toLong(),
                endResultSeqIndex?.toLong()
            )
        }
    }

    suspend fun xquery(
        manager: XmlResourceManager?,
        dbCollection: XmlDBCollection?,
        nodeId: String?,
        revisionNumber: Array<Int>?, query: String, routingContext: RoutingContext, vertxContext: Context,
        user: User, startResultSeqIndex: Long?, endResultSeqIndex: Long?
    ): String? {
        return vertxContext.executeBlockingAwait { promise: Promise<String> ->
            // Initialize queryResource context and store.
            val jsonDBStore = JsonSessionDBStore(routingContext, BasicJsonDBStore.newBuilder().build(), user)
            val xmlDBStore = XmlSessionDBStore(routingContext, BasicXmlDBStore.newBuilder().build(), user)

            val queryCtx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(
                xmlDBStore,
                jsonDBStore,
                SirixQueryContext.CommitStrategy.AUTO
            )

            var body: String? = null

            queryCtx.use {
                if (manager != null && dbCollection != null && revisionNumber != null) {
                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    rtx.use {
                        if (nodeId == null) {
                            rtx.moveToFirstChild()
                        } else {
                            rtx.moveTo(nodeId.toLong())
                        }

                        val dbNode = XmlDBNode(rtx, dbCollection)

                        queryCtx.contextItem = dbNode

                        body = query(
                            xmlDBStore,
                            jsonDBStore,
                            startResultSeqIndex,
                            query,
                            queryCtx,
                            endResultSeqIndex,
                            routingContext
                        )
                    }
                } else {
                    body = query(
                        xmlDBStore,
                        jsonDBStore,
                        startResultSeqIndex,
                        query,
                        queryCtx,
                        endResultSeqIndex,
                        routingContext
                    )
                }
            }

            promise.complete(body)
        }
    }

    private fun query(
        xmlDBStore: XmlSessionDBStore,
        jsonDBStore: JsonSessionDBStore,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext?,
        endResultSeqIndex: Long?,
        routingContext: RoutingContext
    ): String {
        val out = ByteArrayOutputStream()

        executeQueryAndSerialize(
            xmlDBStore,
            jsonDBStore,
            out,
            startResultSeqIndex,
            query,
            queryCtx,
            endResultSeqIndex
        )

        val body = out.toString()

        routingContext.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")

        return body
    }

    private fun executeQueryAndSerialize(
        xmlDBStore: XmlSessionDBStore,
        jsonDBStore: JsonSessionDBStore,
        out: ByteArrayOutputStream,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext?,
        endResultSeqIndex: Long?
    ) {
        PrintStream(out).use { printStream ->
            SirixCompileChain.createWithNodeAndJsonStore(xmlDBStore, jsonDBStore).use { sirixCompileChain ->
                if (startResultSeqIndex == null) {
                    XQuery(sirixCompileChain, query).prettyPrint().serialize(
                        queryCtx,
                        XmlDBSerializer(printStream, true, true)
                    )
                } else {
                    QuerySerializer.serializePaginated(
                        sirixCompileChain,
                        query,
                        queryCtx,
                        startResultSeqIndex,
                        endResultSeqIndex,
                        XmlDBSerializer(printStream, true, true)
                    ) { serializer, startItem -> serializer.serialize(startItem) }
                }
            }
        }
    }

    private fun serializeResource(
        manager: XmlResourceManager, revisions: Array<Int>, nodeId: Long?,
        ctx: RoutingContext
    ): String {
        val out = ByteArrayOutputStream()

        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out).revisions(revisions.toIntArray())

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        if (ctx.queryParam("maxLevel").isNotEmpty())
            serializerBuilder.maxLevel(ctx.queryParam("maxLevel")[0].toLong())

        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        return XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
    }
}