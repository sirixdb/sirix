package io.sirix.rest.crud

import io.sirix.api.Database
import io.sirix.api.NodeCursor
import io.sirix.api.ResourceSession
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.rest.crud.xml.XmlSessionDBStore
import io.vertx.core.Context
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.util.concurrent.Callable

abstract class AbstractGetHandler<T : ResourceSession<*, *>,
        W : AutoCloseable, R : NodeCursor>(
    private val location: Path,
    private val authz: AuthorizationProvider
) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName: String = ctx.pathParam("database")!!
        val resource: String = ctx.pathParam("resource")!!

        PathValidation.validatePathParam(databaseName, "database")
        PathValidation.validatePathParam(resource, "resource")

        val jsonBody = ctx.body().asJsonObject()
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }
        // Database open + resource-session open + revision(-timestamp) resolution are blocking
        // I/O (file reads, uber-page load, per-revision lookups). Run the whole request on a
        // worker pool — previously this ran on the Vert.x event loop (only the query evaluation
        // and serialization inside already offload via executeBlocking), so cold session opens
        // stalled the loop and queued every concurrent request on it.
        withContext(Dispatchers.IO) {
            get(databaseName, ctx, resource, query, context, ctx.get("user") as User, jsonBody)
        }
        return ctx.currentRoute()!!
    }

    suspend fun get(
        databaseName: String, ctx: RoutingContext, resource: String?, query: String?,
        vertxContext: Context, user: User, jsonBody: JsonObject?
    ) {
        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)
        val startRevision: String? = ctx.queryParam("start-revision").getOrNull(0)
        val endRevision: String? = ctx.queryParam("end-revision").getOrNull(0)
        val startRevisionTimestamp: String? = ctx.queryParam("start-revision-timestamp").getOrNull(0)
        val endRevisionTimestamp: String? = ctx.queryParam("end-revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        var body: Buffer?

        val database = openDatabase(location.resolve(databaseName))

        database.use {
            val manager = database.beginResourceSession(resource)

            manager.use {
                body = if (!query.isNullOrEmpty()) {
                    queryResource(
                        databaseName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                        vertxContext, user, jsonBody
                    )
                } else {
                    val revisions: IntArray =
                        Revisions.getRevisionsToSerialize(
                            startRevision, endRevision, startRevisionTimestamp,
                            endRevisionTimestamp, manager, revision, revisionTimestamp
                        )

                    serializeResource(manager, revisions, nodeId?.toLongOrNull(), ctx, vertxContext)
                }
            }
        }

        if (body != null) {
            ctx.response().end(body)
        } else {
            ctx.response().end()
        }
    }

    private suspend fun queryResource(
        databaseName: String?, database: Database<T>, revision: String?,
        revisionTimestamp: String?, manager: T, ctx: RoutingContext,
        nodeId: String?, query: String, vertxContext: Context, user: User, jsonBody: JsonObject?
    ): Buffer? {
        val dbCollection = getDBCollection(databaseName, database)
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
                endResultSeqIndex?.toLong(),
                jsonBody
            )
        }
    }

    suspend fun xquery(
        manager: T?,
        dbCollection: W?,
        nodeId: String?,
        revisionNumber: IntArray?, query: String, routingContext: RoutingContext, vertxContext: Context,
        user: User, startResultSeqIndex: Long?, endResultSeqIndex: Long?, jsonBody: JsonObject?
    ): Buffer? {
        // ordered = false: the default (ordered = true) runs every blocking task of this verticle
        // context strictly serially server-wide, so one slow query stalled ALL concurrent requests.
        // Updating queries (CommitStrategy.AUTO) stay safe — per-resource exclusivity is enforced
        // by sirix's single-writer lock, and HTTP never guaranteed cross-request ordering anyway.
        return vertxContext.executeBlocking(Callable {
            // Initialize queryResource context and store.
            val jsonDBStore = JsonSessionDBStore(
                routingContext,
                BasicJsonDBStore.newBuilder().location(location).storeDeweyIds(true).build(),
                user,
                authz
            )
            val xmlDBStore =
                XmlSessionDBStore(routingContext, BasicXmlDBStore.newBuilder().location(location).storeDeweyIds(true).build(), user, authz)

            val commitMessage = routingContext.queryParam("commitMessage").getOrElse(0) {
                jsonBody?.getString("commitMessage")
            }
            val commitTimestampAsString = routingContext.queryParam("commitTimestamp").getOrElse(0) {
                jsonBody?.getString("commitTimestamp")
            }
            val commitTimestamp = if (commitTimestampAsString == null) {
                null
            } else {
                Revisions.parseRevisionTimestamp(commitTimestampAsString).toInstant()
            }
            val queryCtx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(
                xmlDBStore,
                jsonDBStore,
                SirixQueryContext.CommitStrategy.AUTO,
                commitMessage,
                commitTimestamp
            )
            var body: Buffer? = null
            queryCtx.use {
                if (manager != null && dbCollection != null && revisionNumber != null) {
                    @Suppress("UNCHECKED_CAST") val rtx = manager.beginNodeReadOnlyTrx(revisionNumber[0]) as R

                    rtx.use {
                        if (nodeId == null) {
                            rtx.moveToFirstChild()
                        } else {
                            // An unchecked moveTo would bind the context item to the document
                            // root: the JSON path throws AssertionError (HTTP 500) and the XML
                            // path silently evaluates the query against the WRONG context.
                            rtx.moveToOrNotFound(nodeId.toLong())
                        }

                        handleQueryExtra(rtx, dbCollection, queryCtx, jsonDBStore)

                        body = executeQueryAndSerialize(
                            routingContext,
                            xmlDBStore,
                            jsonDBStore,
                            createOutputStream(),
                            startResultSeqIndex,
                            query,
                            queryCtx,
                            endResultSeqIndex,
                            manager,
                            revisionNumber
                        )
                    }
                } else {
                    body = executeQueryAndSerialize(
                        routingContext,
                        xmlDBStore,
                        jsonDBStore,
                        createOutputStream(),
                        startResultSeqIndex,
                        query,
                        queryCtx,
                        endResultSeqIndex,
                        null,
                        null
                    )
                }
            }
            body
        }, false).coAwait()
    }

    private suspend fun serializeResource(
        manager: T,
        revisions: IntArray,
        nodeId: Long?,
        ctx: RoutingContext,
        vertxContext: Context
    ): Buffer {
        // Status code and Content-Type are set by the format-specific serialize helper —
        // stamping "application/json" here would overwrite the XML helper's header.
        // ordered = false: read-only serialization, no reason to serialize it behind every
        // other blocking task on this verticle context.
        return vertxContext.executeBlocking(Callable {
            getSerializedBody(manager, revisions, nodeId, ctx)
        }, false).coAwait()!!
    }

    abstract suspend fun openDatabase(dbFile: Path): Database<T>

    protected abstract suspend fun getDBCollection(databaseName: String?, database: Database<T>): W

    protected abstract fun handleQueryExtra(
        rtx: R,
        dbCollection: W,
        queryContext: SirixQueryContext,
        jsonSessionDBStore: JsonSessionDBStore
    )

    protected abstract fun createOutputStream(): OutputWrapper

    /**
     * Executes [query] and serializes the result. When the request is resource-scoped, [manager]
     * is the open resource session and [revisionNumber] the resolved revision(s) — format-specific
     * handlers may use them to wire analytical fast paths (see JsonGet); both are `null` for
     * queries without a resource context.
     */
    protected abstract fun executeQueryAndSerialize(
        routingContext: RoutingContext,
        xmlDBStore: XmlSessionDBStore,
        jsonDBStore: JsonSessionDBStore,
        out: OutputWrapper,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext,
        endResultSeqIndex: Long?,
        manager: T?,
        revisionNumber: IntArray?
    ): Buffer

    /** Serializes the resource (or a subtree of it) into a wire-ready response body. */
    protected abstract fun getSerializedBody(
        manager: T,
        revisions: IntArray,
        nodeId: Long?,
        ctx: RoutingContext
    ): Buffer

}

sealed class OutputWrapper {
    class StringBuilderWrapper(val sb: StringBuilder) : OutputWrapper()
    class ByteArrayOutputStreamWrapper(val baos: ByteArrayOutputStream) : OutputWrapper()
}


