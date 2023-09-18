package io.sirix.rest.crud

import io.brackit.query.jdm.StructuredItemCollection
import io.brackit.query.jdm.node.TemporalNodeCollection
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.api.NodeCursor
import io.sirix.api.NodeReadOnlyTrx
import io.sirix.api.ResourceSession
import io.sirix.api.json.JsonResourceSession
import io.sirix.api.xml.XmlResourceSession
import io.sirix.query.SirixQueryContext
import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await

import io.sirix.query.json.*
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.query.node.XmlDBCollection
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.rest.crud.xml.XmlSessionDBStore
import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.withContext
import java.nio.file.Path

abstract class AbstractGetHandler <T : ResourceSession<*, *>,
        W: AutoCloseable, R: NodeCursor>(
private val location: Path,
private val authz: AuthorizationProvider){
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName: String = ctx.pathParam("database")
        val resource: String = ctx.pathParam("resource")
        val jsonBody = ctx.body().asJsonObject()
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }

        withContext(context.dispatcher()) {
            get(databaseName, ctx, resource, query, context, ctx.get("user") as User, jsonBody)
        }

        return ctx.currentRoute()
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

        var body: String?

        val database = openDatabase(location.resolve(databaseName))

        database.use {
            val manager = database.beginResourceSession(resource)

            manager.use {
                body = if (query != null && query.isNotEmpty()) {
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

     suspend fun queryResource(
            databaseName: String?, database: Database<T>, revision: String?,
            revisionTimestamp: String?, manager: T, ctx: RoutingContext,
            nodeId: String?, query: String, vertxContext: Context, user: User, jsonBody: JsonObject?
    ): String? {
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
    ): String? {
        return vertxContext.executeBlocking { promise: Promise<String> ->
            // Initialize queryResource context and store.
            val jsonDBStore = JsonSessionDBStore(
                    routingContext,
                    BasicJsonDBStore.newBuilder().storeDeweyIds(true).build(),
                    user,
                    authz
            )
            val xmlDBStore =
                    XmlSessionDBStore(routingContext, BasicXmlDBStore.newBuilder().storeDeweyIds(true).build(), user, authz)

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

            var body: String?

            queryCtx.use {
                if (manager != null && dbCollection != null && revisionNumber != null) {
                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber[0]) as R

                    rtx.use {
                        if (nodeId == null) {
                            rtx.moveToFirstChild()
                        } else {
                            rtx.moveTo(nodeId.toLong())
                        }

                        handleQueryExtra(rtx, dbCollection, queryCtx, jsonDBStore)

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
        }.await()
    }

    abstract suspend fun serializeResource(
            manager: T, revisions: IntArray, nodeId: Long?,
            ctx: RoutingContext,
            vertxContext: Context
    ): String

    abstract suspend fun openDatabase(dbFile: Path): Database<T>

    abstract suspend fun getDBCollection(databaseName: String?, database: Database<T>): W

    abstract  fun handleQueryExtra(rtx: R, dbCollection: W, queryContext: SirixQueryContext, jsonSessionDBStore: JsonSessionDBStore)
    abstract fun query(
            xmlDBStore: XmlSessionDBStore,
            jsonDBStore: JsonSessionDBStore,
            startResultSeqIndex: Long?,
            query: String,
            queryCtx: SirixQueryContext,
            endResultSeqIndex: Long?,
            routingContext: RoutingContext
    ): String
}
