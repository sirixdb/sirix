package io.sirix.rest.crud.xml

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.User
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.withContext
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.api.xml.XmlResourceSession
import io.sirix.rest.crud.PermissionCheckingQuery
import io.sirix.rest.crud.QuerySerializer
import io.sirix.rest.crud.Revisions
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.service.xml.serialize.XmlSerializer
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.XmlDBSerializer
import io.sirix.query.json.BasicJsonDBStore
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.query.node.XmlDBCollection
import io.sirix.query.node.XmlDBNode
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Path

class XmlGet(private val location: Path, private val keycloak: OAuth2Auth, private val authz: AuthorizationProvider) {
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

    private suspend fun get(
        databaseName: String, ctx: RoutingContext, resource: String, query: String?,
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

        val database = Databases.openXmlDatabase(location.resolve(databaseName))

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

                    serializeResource(manager, revisions, nodeId?.toLongOrNull(), ctx)
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
        databaseName: String?, database: Database<XmlResourceSession>, revision: String?,
        revisionTimestamp: String?, manager: XmlResourceSession, ctx: RoutingContext,
        nodeId: String?, query: String, vertxContext: Context, user: User, jsonBody: JsonObject?
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
                endResultSeqIndex?.toLong(),
                jsonBody
            )
        }
    }

    suspend fun xquery(
        manager: XmlResourceSession?,
        dbCollection: XmlDBCollection?,
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
        }.await()
    }

    private fun query(
        xmlDBStore: XmlSessionDBStore,
        jsonDBStore: JsonSessionDBStore,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext,
        endResultSeqIndex: Long?,
        routingContext: RoutingContext
    ): String {
        val out = ByteArrayOutputStream()

        executeQueryAndSerialize(
            routingContext,
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
        routingContext: RoutingContext,
        xmlDBStore: XmlSessionDBStore,
        jsonDBStore: JsonSessionDBStore,
        out: ByteArrayOutputStream,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext,
        endResultSeqIndex: Long?
    ) {
        PrintStream(out).use { printStream ->
            SirixCompileChain.createWithNodeAndJsonStore(xmlDBStore, jsonDBStore).use { sirixCompileChain ->
                if (startResultSeqIndex == null) {
                    PermissionCheckingQuery(
                        sirixCompileChain,
                        query,
                        keycloak,
                        routingContext.get("user"),
                        authz
                    ).prettyPrint().serialize(
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
                        keycloak,
                        authz,
                        routingContext.get("user"),
                        XmlDBSerializer(printStream, true, true),
                    ) { serializer, startItem -> serializer.serialize(startItem) }
                }
            }
        }
    }

    private fun serializeResource(
        manager: XmlResourceSession, revisions: IntArray, nodeId: Long?,
        ctx: RoutingContext
    ): String {
        val out = ByteArrayOutputStream()

        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out).revisions(revisions)

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        if (ctx.queryParam("maxLevel").isNotEmpty())
            serializerBuilder.maxLevel(ctx.queryParam("maxLevel")[0].toLong())

        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        return XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
    }
}