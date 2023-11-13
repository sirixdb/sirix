package io.sirix.rest.crud.json

import io.vertx.core.Context
import io.vertx.core.Promise
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.await
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.api.json.JsonNodeReadOnlyTrx
import io.sirix.api.json.JsonResourceSession
import io.sirix.rest.crud.PermissionCheckingQuery
import io.sirix.rest.crud.QuerySerializer
import io.sirix.rest.crud.xml.XmlSessionDBStore
import io.sirix.service.json.serialize.JsonRecordSerializer
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.query.JsonDBSerializer
import io.sirix.query.SirixCompileChain
import io.sirix.rest.crud.AbstractGetHandler
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.*
import java.io.StringWriter
import java.nio.file.Path

class JsonGet(private val location: Path, private val keycloak: OAuth2Auth, private val authz: AuthorizationProvider): AbstractGetHandler <JsonResourceSession, JsonDBCollection, JsonNodeReadOnlyTrx> (location, authz) {
//    override suspend fun xquery(
//        manager: JsonResourceSession?,
//        dbCollection: JsonDBCollection?,
//        nodeId: String?,
//        revisionNumber: IntArray?, query: String, routingContext: RoutingContext, vertxContext: Context,
//        user: User, startResultSeqIndex: Long?, endResultSeqIndex: Long?, jsonBody: JsonObject?
//    ): String? {
//        return vertxContext.executeBlocking { promise: Promise<String> ->
//            // Initialize queryResource context and store.
//            val jsonDBStore = JsonSessionDBStore(
//                routingContext,
//                BasicJsonDBStore.newBuilder().storeDeweyIds(true).build(),
//                user,
//                authz
//            )
//            val xmlDBStore =
//                XmlSessionDBStore(routingContext, BasicXmlDBStore.newBuilder().storeDeweyIds(true).build(), user, authz)
//
//            val commitMessage = routingContext.queryParam("commitMessage").getOrElse(0) {
//                jsonBody?.getString("commitMessage")
//            }
//            val commitTimestampAsString = routingContext.queryParam("commitTimestamp").getOrElse(0) {
//                jsonBody?.getString("commitTimestamp")
//            }
//            val commitTimestamp = if (commitTimestampAsString == null) {
//                null
//            } else {
//                Revisions.parseRevisionTimestamp(commitTimestampAsString).toInstant()
//            }
//
//            val queryCtx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(
//                xmlDBStore,
//                jsonDBStore,
//                SirixQueryContext.CommitStrategy.AUTO,
//                commitMessage,
//                commitTimestamp
//            )
//
//            var body: String? = null
//
//            queryCtx.use {
//                if (manager != null && dbCollection != null && revisionNumber != null) {
//                    val rtx = manager.beginNodeReadOnlyTrx(revisionNumber[0])
//
//                    rtx.use {
//                        if (nodeId == null) {
//                            rtx.moveToFirstChild()
//                        } else {
//                            rtx.moveTo(nodeId.toLong())
//                        }
//
//                        handleQueryExtra(rtx, dbCollection, queryCtx, jsonDBStore)
//
//                        body = query(
//                            xmlDBStore,
//                            jsonDBStore,
//                            startResultSeqIndex,
//                            query,
//                            queryCtx,
//                            endResultSeqIndex,
//                            routingContext
//                        )
//                    }
//                } else {
//                    body = query(
//                        xmlDBStore,
//                        jsonDBStore,
//                        startResultSeqIndex,
//                        query,
//                        queryCtx,
//                        endResultSeqIndex,
//                        routingContext
//                    )
//                }
//            }
//
//            promise.complete(body)
//        }.await()
//    }

    override fun query(
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
                PermissionCheckingQuery(
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

    override suspend fun serializeResource(
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

    override suspend fun openDatabase(dbFile: Path): Database<JsonResourceSession> {
        return Databases.openJsonDatabase(dbFile)
    }

    override suspend fun getDBCollection(databaseName: String?, database: Database<JsonResourceSession>): JsonDBCollection {
        return JsonDBCollection(databaseName, database)
    }

    override fun  handleQueryExtra(rtx: JsonNodeReadOnlyTrx, dbCollection: JsonDBCollection, queryContext: SirixQueryContext, jsonDBStore: JsonSessionDBStore) {
        val jsonItem = JsonItemFactory()
                .getSequence(rtx, dbCollection)

        if (jsonItem != null) {
            queryContext.contextItem = jsonItem

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
    }
}
