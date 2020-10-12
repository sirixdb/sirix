package org.sirix.rest.crud.json

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.User
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.kotlin.core.executeBlockingAwait
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.rest.AuthRole
import org.sirix.rest.crud.PermissionCheckingXQuery
import org.sirix.rest.crud.QuerySerializer
import org.sirix.rest.crud.Revisions
import org.sirix.rest.crud.xml.XmlSessionDBStore
import org.sirix.service.json.serialize.JsonRecordSerializer
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.xquery.JsonDBSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.json.*
import org.sirix.xquery.node.BasicXmlDBStore
import java.io.StringWriter
import java.nio.file.Path

class JsonGet(private val location: Path, private val keycloak: OAuth2Auth) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")
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

        var body: String? = null

        database.use {
            try {
                val manager = database.openResourceManager(resource)

                manager.use {
                    body = if (query != null && query.isNotEmpty()) {
                        queryResource(
                            databaseName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                            vertxContext, user
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
        databaseName: String?, database: Database<JsonResourceManager>, revision: String?,
        revisionTimestamp: String?, manager: JsonResourceManager, ctx: RoutingContext,
        nodeId: String?, query: String, vertxContext: Context, user: User
    ): String? {
        val dbCollection = JsonDBCollection(databaseName, database)

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
        manager: JsonResourceManager?,
        dbCollection: JsonDBCollection?,
        nodeId: String?,
        revisionNumber: IntArray?, query: String, routingContext: RoutingContext, vertxContext: Context,
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

            queryCtx.use {
                var body: String? = null

                if (manager != null && dbCollection != null && revisionNumber != null) {
                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    rtx.use {
                        if (nodeId == null) {
                            rtx.moveToFirstChild()
                        } else {
                            rtx.moveTo(nodeId.toLong())
                        }

                        val jsonItem = JsonItemFactory().getSequence(rtx, dbCollection)

                        if (jsonItem != null) {
                            queryCtx.contextItem = jsonItem

                            when (jsonItem) {
                                is AbstractJsonDBArray<*> -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }
                                is JsonDBObject -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }
                                is AtomicJsonDBItem -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }
                                is NumericJsonDBItem -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }
                                else -> throw IllegalStateException("Node type not known.")
                            }
                        }

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

                promise.complete(body)
            }
        }
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
        val out = StringBuilder()

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
        out: StringBuilder,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext,
        endResultSeqIndex: Long?
    ) {
        SirixCompileChain.createWithNodeAndJsonStore(xmlDBStore, jsonDBStore).use { sirixCompileChain ->
            if (startResultSeqIndex == null) {
                val serializer = JsonDBSerializer(out, false)
                PermissionCheckingXQuery(
                    sirixCompileChain,
                    query,
                    AuthRole.MODIFY,
                    keycloak,
                    routingContext.get("user")
                ).prettyPrint().serialize(queryCtx, serializer)
            } else {
                QuerySerializer.serializePaginated(
                    sirixCompileChain,
                    query,
                    queryCtx,
                    startResultSeqIndex,
                    endResultSeqIndex,
                    AuthRole.MODIFY,
                    keycloak,
                    routingContext.get("user"),
                    JsonDBSerializer(out, true)
                ) { serializer, startItem -> serializer.serialize(startItem) }
            }
        }
    }

    private suspend fun serializeResource(
        manager: JsonResourceManager, revisions: IntArray, nodeId: Long?,
        ctx: RoutingContext,
        vertxContext: Context
    ): String {
        val serializedString =  vertxContext.executeBlockingAwait { promise: Promise<String> ->
            val nextTopLevelNodes = ctx.queryParam("nextTopLevelNodes").getOrNull(0)?.toInt()
            val lastTopLevelNodeKey = ctx.queryParam("lastTopLevelNodeKey").getOrNull(0)?.toLong()

            val numberOfNodes = ctx.queryParam("numberOfNodes").getOrNull(0)?.toLong()

            val out = StringWriter()

            val withMetaData: String? = ctx.queryParam("withMetaData").getOrNull(0)
            val maxLevel: String? = ctx.queryParam("maxLevel").getOrNull(0)
            val prettyPrint: String? = ctx.queryParam("prettyPrint").getOrNull(0)

            if (nextTopLevelNodes == null) {
                val serializerBuilder = JsonSerializer.newBuilder(manager, out).revisions(revisions)

                nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

                if (withMetaData != null) {
                    when (withMetaData) {
                        "nodeKeyAndChildCount" -> serializerBuilder.withNodeKeyAndChildCountMetaData(true)
                        "nodeKey" -> serializerBuilder.withNodeKeyMetaData(true)
                        else -> serializerBuilder.withMetaData(true)
                    }
                }

                if (maxLevel != null) {
                    serializerBuilder.maxLevel(maxLevel.toLong())
                }

                if (prettyPrint != null) {
                    serializerBuilder.prettyPrint()
                }

                if (numberOfNodes != null) {
                    serializerBuilder.numberOfNodes(numberOfNodes)
                }

                val serializer = serializerBuilder.build()

                promise.complete(JsonSerializeHelper().serialize(serializer, out, ctx, manager, revisions, nodeId))
            } else {
                val serializerBuilder =
                    JsonRecordSerializer.newBuilder(manager, nextTopLevelNodes, out).revisions(revisions)

                nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

                if (withMetaData != null) {
                    when (withMetaData) {
                        "nodeKeyAndChildCount" -> serializerBuilder.withNodeKeyAndChildCountMetaData(true)
                        "nodeKey" -> serializerBuilder.withNodeKeyMetaData(true)
                        else -> serializerBuilder.withMetaData(true)
                    }
                }

                if (maxLevel != null) {
                    serializerBuilder.maxLevel(maxLevel.toLong())
                }

                if (prettyPrint != null) {
                    serializerBuilder.prettyPrint()
                }

                if (lastTopLevelNodeKey != null) {
                    serializerBuilder.lastTopLevelNodeKey(lastTopLevelNodeKey)
                }

                if (numberOfNodes != null) {
                    serializerBuilder.numberOfNodes(numberOfNodes)
                }

                val serializer = serializerBuilder.build()

                promise.complete(JsonSerializeHelper().serialize(serializer, out, ctx, manager, revisions, nodeId))
            }
        }

        ctx.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")

        return serializedString!!
    }
}
