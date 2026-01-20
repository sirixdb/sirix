package io.sirix.rest.crud.json

import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
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
import io.sirix.rest.crud.OutputWrapper
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.*
import io.vertx.core.http.HttpHeaders
import io.brackit.query.compiler.CompileChain
import java.io.StringWriter
import java.nio.file.Path

class JsonGet(location: Path, private val keycloak: OAuth2Auth, private val authz: AuthorizationProvider) :
    AbstractGetHandler<JsonResourceSession, JsonDBCollection, JsonNodeReadOnlyTrx>(location, authz) {

    override fun executeQueryAndSerialize(
        routingContext: RoutingContext,
        xmlDBStore: XmlSessionDBStore,
        jsonDBStore: JsonSessionDBStore,
        out: OutputWrapper,
        startResultSeqIndex: Long?,
        query: String,
        queryCtx: SirixQueryContext,
        endResultSeqIndex: Long?
    ): String {
        val stringBuilder = (out as OutputWrapper.StringBuilderWrapper).sb

        // Parse plan parameters from request
        val jsonBody = routingContext.body().asJsonObject()
        val includePlan = routingContext.queryParam("plan").firstOrNull()?.toBoolean()
            ?: jsonBody?.getBoolean("plan")
            ?: false
        val planStageStr = routingContext.queryParam("planStage").firstOrNull()
            ?: jsonBody?.getString("planStage")
            ?: "optimized"

        var permissionCheckingQuery: PermissionCheckingQuery? = null

        SirixCompileChain.createWithNodeAndJsonStore(xmlDBStore, jsonDBStore).use { sirixCompileChain ->
            if (startResultSeqIndex == null) {
                val serializer = JsonDBSerializer(stringBuilder, false)
                permissionCheckingQuery = PermissionCheckingQuery(
                    sirixCompileChain,
                    query,
                    keycloak,
                    routingContext.get("user"),
                    authz
                )
                permissionCheckingQuery!!.prettyPrint().serialize(queryCtx, serializer)
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
                    JsonDBSerializer(stringBuilder, true)
                ) { serializer, startItem -> serializer.serialize(startItem) }
            }
        }

        routingContext.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")

        // If plan is requested, wrap results with plan information
        if (includePlan && permissionCheckingQuery != null) {
            val compileChain = permissionCheckingQuery!!.compileChain
            if (compileChain != null) {
                val planStage = when (planStageStr.lowercase()) {
                    "parsed" -> CompileChain.PlanStage.PARSED
                    "both" -> CompileChain.PlanStage.BOTH
                    else -> CompileChain.PlanStage.OPTIMIZED
                }
                val planJson = compileChain.getPlanAsJSON(planStage)

                val resultBuilder = StringBuilder()
                resultBuilder.append("{\"results\":")
                resultBuilder.append(stringBuilder.toString())
                resultBuilder.append(",\"plan\":")
                resultBuilder.append(planJson ?: "null")
                resultBuilder.append("}")
                return resultBuilder.toString()
            }
        }

        return stringBuilder.toString()
    }

    override fun getSerializedString(
        manager: JsonResourceSession,
        revisions: IntArray,
        nodeId: Long?,
        ctx: RoutingContext
    ): String {
        val nextTopLevelNodes = ctx.queryParam("nextTopLevelNodes").getOrNull(0)?.toInt()
        val startNodeKey = ctx.queryParam("startNodeKey").getOrNull(0)?.toLong()

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

            return JsonSerializeHelper().serialize(serializer, out, ctx, manager, revisions, nodeId)
        } else {
            val serializerBuilder =
                JsonRecordSerializer.newBuilder(manager, nextTopLevelNodes, out).revisions(revisions)

            // For pagination: startNodeKey is the last loaded child, serialize its right siblings
            // For initial load: startNodeKey is 0 (default), serialize from document root
            if (startNodeKey != null) {
                serializerBuilder.startNodeKey(startNodeKey)
            }

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

            return JsonSerializeHelper().serialize(serializer, out, ctx, manager, revisions, nodeId)
        }
    }

    override suspend fun openDatabase(dbFile: Path): Database<JsonResourceSession> {
        return Databases.openJsonDatabase(dbFile)
    }

    override suspend fun getDBCollection(
        databaseName: String?,
        database: Database<JsonResourceSession>
    ): JsonDBCollection {
        return JsonDBCollection(databaseName, database)
    }

    override fun handleQueryExtra(
        rtx: JsonNodeReadOnlyTrx,
        dbCollection: JsonDBCollection,
        queryContext: SirixQueryContext,
        jsonSessionDBStore: JsonSessionDBStore
    ) {
        val jsonItem = JsonItemFactory()
            .getSequence(rtx, dbCollection)

        if (jsonItem != null) {
            queryContext.contextItem = jsonItem

            when (jsonItem) {
                is AbstractJsonDBArray<*> -> {
                    jsonItem.collection.setJsonDBStore(jsonSessionDBStore)
                    jsonSessionDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                }

                is JsonDBObject -> {
                    jsonItem.collection.setJsonDBStore(jsonSessionDBStore)
                    jsonSessionDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                }

                is AtomicBooleanJsonDBItem -> {
                    jsonItem.collection.setJsonDBStore(jsonSessionDBStore)
                    jsonSessionDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                }

                is AtomicStrJsonDBItem -> {
                    jsonItem.collection.setJsonDBStore(jsonSessionDBStore)
                    jsonSessionDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                }

                is AtomicNullJsonDBItem -> {
                    jsonItem.collection.setJsonDBStore(jsonSessionDBStore)
                    jsonSessionDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                }

                is NumericJsonDBItem -> {
                    jsonItem.collection.setJsonDBStore(jsonSessionDBStore)
                    jsonSessionDBStore.addDatabase(jsonItem.collection, jsonItem.collection.database)
                }

                else -> {
                    throw IllegalStateException("Node type not known.")
                }
            }
        }
    }

    override fun createOutputStream(): OutputWrapper = OutputWrapper.StringBuilderWrapper(StringBuilder())
}
