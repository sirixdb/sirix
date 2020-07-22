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
import kotlinx.coroutines.withContext
import org.brackit.xquery.XQuery
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.xml.XmlNodeReadOnlyTrx
import org.sirix.api.xml.XmlResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.rest.crud.QuerySerializer
import org.sirix.rest.crud.Revisions
import org.sirix.service.xml.serialize.XmlSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.XmlDBSerializer
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

        get(databaseName, ctx, resource, query, context, ctx.get("user") as User)

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
            }
        }
    }

    private suspend fun queryResource(
        databaseName: String?, database: Database<XmlResourceManager>, revision: String?,
        revisionTimestamp: String?, manager: XmlResourceManager, ctx: RoutingContext,
        nodeId: String?, query: String, vertxContext: Context, user: User
    ) {
        withContext(vertxContext.dispatcher()) {
            val dbCollection = XmlDBCollection(databaseName, database)

            dbCollection.use {
                val revisionNumber = Revisions.getRevisionNumber(revision, revisionTimestamp, manager)

                val trx: XmlNodeReadOnlyTrx
                try {
                    trx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    trx.use {
                        if (nodeId == null)
                            trx.moveToFirstChild()
                        else
                            trx.moveTo(nodeId.toLong())

                        val dbNode = XmlDBNode(trx, dbCollection)

                        val startResultSeqIndex = ctx.queryParam("startResultSeqIndex").getOrElse(0) { null }
                        val endResultSeqIndex = ctx.queryParam("endResultSeqIndex").getOrElse(0) { null }

                        xquery(
                            query,
                            dbNode,
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
        query: String, node: XmlDBNode?, routingContext: RoutingContext, context: Context,
        user: User, startResultSeqIndex: Long?, endResultSeqIndex: Long?
    ) {
        context.executeBlockingAwait { promise: Promise<Unit> ->
            // Initialize queryResource context and store.
            val dbStore = XmlSessionDBStore(routingContext, BasicXmlDBStore.newBuilder().build(), user)

            dbStore.use {
                val queryCtx = SirixQueryContext.createWithNodeStore(dbStore)

                node.let { queryCtx.contextItem = node }

                val out = ByteArrayOutputStream()

                out.use {
                    executeQueryAndSerialize(
                        out,
                        dbStore,
                        startResultSeqIndex,
                        query,
                        queryCtx,
                        endResultSeqIndex
                    )

                    val body = String(out.toByteArray(), StandardCharsets.UTF_8)

                    routingContext.response().setStatusCode(200)
                        .putHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
                        .putHeader(HttpHeaders.CONTENT_LENGTH, body.toByteArray(StandardCharsets.UTF_8).size.toString())
                        .write(body)
                        .end()
                }
            }

            promise.complete(null)
        }
    }

    private fun executeQueryAndSerialize(
        out: ByteArrayOutputStream,
        dbStore: XmlSessionDBStore,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext?,
        endResultSeqIndex: Long?
    ) {
        PrintStream(out).use { printStream ->
            SirixCompileChain.createWithNodeStore(dbStore).use { sirixCompileChain ->
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
    ) {
        val out = ByteArrayOutputStream()

        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out).revisions(revisions.toIntArray())

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        if (ctx.queryParam("maxLevel").isNotEmpty())
            serializerBuilder.maxLevel(ctx.queryParam("maxLevel")[0].toLong())

        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
    }
}