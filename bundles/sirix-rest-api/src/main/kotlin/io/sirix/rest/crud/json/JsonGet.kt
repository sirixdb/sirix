package io.sirix.rest.crud.json

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
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.api.json.JsonResourceSession
import io.sirix.rest.crud.PermissionCheckingXQuery
import io.sirix.rest.crud.QuerySerializer
import io.sirix.rest.crud.Revisions
import io.sirix.rest.crud.xml.XmlSessionDBStore
import io.sirix.service.json.serialize.JsonRecordSerializer
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.query.JsonDBSerializer
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.*
import io.sirix.query.node.BasicXmlDBStore
import java.io.StringWriter
import java.nio.file.Path

class JsonGet(private val location: Path, private val keycloak: OAuth2Auth, private val authz: AuthorizationProvider) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName = ctx.pathParam("database")
        val resource = ctx.pathParam("resource")
        val jsonBody = ctx.body().asJsonObject()
        val query: String? = ctx.queryParam("query").getOrElse(0) {
            jsonBody?.getString("query")
        }

        get(databaseName, ctx, resource, query, context, ctx.get("user") as User, jsonBody)

        return ctx.currentRoute()
    }

    private suspend fun get(
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

        val database = Databases.openJsonDatabase(location.resolve(databaseName))

        database.use {
            val manager = database.beginResourceSession(resource)

            manager.use {
                body = if (query.isNullOrEmpty()) {
                    val revisions: IntArray =
                        Revisions.getRevisionsToSerialize(
                            startRevision, endRevision, startRevisionTimestamp,
                            endRevisionTimestamp, manager, revision, revisionTimestamp
                        )

                    serializeResource(manager, revisions, nodeId?.toLongOrNull(), ctx, vertxContext)
                } else {
                    queryResource(
                        databaseName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                        vertxContext, user, jsonBody
                    )
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
        databaseName: String?, database: Database<JsonResourceSession>, revision: String?,
        revisionTimestamp: String?, manager: JsonResourceSession, ctx: RoutingContext,
        nodeId: String?, query: String, vertxContext: Context, user: User, jsonBody: JsonObject?
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
                endResultSeqIndex?.toLong(),
                jsonBody
            )
        }
    }

    suspend fun xquery(
        manager: JsonResourceSession?,
        dbCollection: JsonDBCollection?,
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

                        val jsonItem = JsonItemFactory()
                            .getSequence(rtx, dbCollection)

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

                                is AtomicBooleanJsonDBItem -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }

                                is AtomicStrJsonDBItem -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }

                                is AtomicNullJsonDBItem -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }

                                is NumericJsonDBItem -> {
                                    jsonItem.collection.setJsonDBStore(jsonDBStore)
                                    jsonDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                                }

                                else -> {
                                    throw IllegalStateException("Node type not known.")
                                }
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
                    keycloak,
                    routingContext.get("user"),
                    authz
                ).prettyPrint().serialize(queryCtx, serializer)
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
                    JsonDBSerializer(out, true)
                ) { serializer, startItem -> serializer.serialize(startItem) }
            }
        }
    }

    private suspend fun serializeResource(
        manager: JsonResourceSession, revisions: IntArray, nodeId: Long?,
        ctx: RoutingContext,
        vertxContext: Context
    ): String {
        val serializedString = vertxContext.executeBlocking { promise: Promise<String> ->
            val nextTopLevelNodes = ctx.queryParam("nextTopLevelNodes").getOrNull(0)?.toInt()
            val lastTopLevelNodeKey = ctx.queryParam("lastTopLevelNodeKey").getOrNull(0)?.toLong()

            val numberOfNodes = ctx.queryParam("numberOfNodes").getOrNull(0)?.toLong()
            val maxChildren = ctx.queryParam("maxChildren").getOrNull(0)?.toLong()

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

                if (maxChildren != null) {
                    serializerBuilder.maxChildren(maxChildren.toLong())
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

                if (maxChildren != null) {
                    serializerBuilder.maxChildren(maxChildren.toLong())
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
        }.await()

        ctx.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")

        return serializedString!!
    }
}
