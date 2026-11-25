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
import io.sirix.query.SirixCompileChain
import io.sirix.query.function.sdb.explain.QueryPlanSerializer
import io.sirix.service.json.serialize.JsonRecordSerializer
import io.sirix.service.json.serialize.JsonSerializer
import io.sirix.query.JsonDBSerializer
import io.sirix.rest.crud.AbstractGetHandler
import io.sirix.rest.crud.OutputWrapper
import io.sirix.query.SirixQueryContext
import io.sirix.query.json.JsonDBCollection
import io.sirix.query.json.JsonDBCollectionImpl
import io.sirix.query.json.JsonItemFactory
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.brackit.query.compiler.CompileChain
import java.io.ByteArrayOutputStream
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
        endResultSeqIndex: Long?,
        manager: JsonResourceSession?,
        revisionNumber: IntArray?
    ): Buffer {
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

        createCompileChain(routingContext, query, manager, revisionNumber, xmlDBStore, jsonDBStore)
            .use { sirixCompileChain ->
            if (startResultSeqIndex == null) {
                val serializer = JsonDBSerializer(stringBuilder, false)
                permissionCheckingQuery = PermissionCheckingQuery(
                    sirixCompileChain,
                    query,
                    keycloak,
                    routingContext.get("user")!!,
                    authz
                )
                permissionCheckingQuery.prettyPrint().serialize(queryCtx, serializer)
            } else {
                QuerySerializer.serializePaginated(
                    sirixCompileChain,
                    query,
                    queryCtx,
                    startResultSeqIndex,
                    endResultSeqIndex,
                    keycloak,
                    authz,
                    routingContext.get("user")!!,
                    JsonDBSerializer(stringBuilder, true)
                ) { serializer, startItem -> serializer.serialize(startItem) }
            }
        }

        routingContext.response().setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")

        // If plan is requested, wrap results with plan information
        if (includePlan && permissionCheckingQuery != null) {
            val compileChain = permissionCheckingQuery.compileChain
            if (compileChain != null) {
                val planJson: String? = if (planStageStr.lowercase() == "candidates" && compileChain is SirixCompileChain) {
                    // Return parsed + optimized + all candidate plans from the Mesh
                    val optimizedAST = compileChain.optimizedAST
                    val mesh = compileChain.mesh
                    if (optimizedAST != null) {
                        val sb = StringBuilder(8192)
                        sb.append("{\"parsed\": ")
                        val parsedAST = compileChain.parsedAST
                        sb.append(if (parsedAST != null) QueryPlanSerializer.serialize(parsedAST) else "null")
                        sb.append(", \"optimized\": ")
                        sb.append(QueryPlanSerializer.serialize(optimizedAST))
                        sb.append(", \"candidates\": ")
                        if (mesh != null && mesh.classCount() > 0) {
                            sb.append(QueryPlanSerializer.serializeWithCandidates(optimizedAST, mesh))
                        } else {
                            sb.append("{\"chosenPlan\": null, \"candidates\": []}")
                        }
                        sb.append("}")
                        sb.toString()
                    } else {
                        null
                    }
                } else {
                    val planStage = when (planStageStr.lowercase()) {
                        "parsed" -> CompileChain.PlanStage.PARSED
                        "both" -> CompileChain.PlanStage.BOTH
                        else -> CompileChain.PlanStage.OPTIMIZED
                    }
                    compileChain.getPlanAsJSON(planStage)
                }

                val resultBuilder = StringBuilder()
                resultBuilder.append("{\"results\":")
                resultBuilder.append(stringBuilder.toString())
                resultBuilder.append(",\"plan\":")
                resultBuilder.append(planJson ?: "null")
                resultBuilder.append("}")
                return Buffer.buffer(resultBuilder.toString())
            }
        }

        return Buffer.buffer(stringBuilder.toString())
    }

    /**
     * Compile chain for [query]: session-bound — analytical projection/scan serving at the
     * request's resolved revision — when the request is resource-scoped AND the serving gate
     * proves the query targets only that resource; the plain store-backed chain otherwise.
     * Any wiring problem falls back to the plain chain: the generic pipeline is always
     * correct, so a refusal can only cost performance, never correctness.
     */
    private fun createCompileChain(
        routingContext: RoutingContext,
        query: String,
        manager: JsonResourceSession?,
        revisionNumber: IntArray?,
        xmlDBStore: XmlSessionDBStore,
        jsonDBStore: JsonSessionDBStore
    ): SirixCompileChain {
        if (manager != null && revisionNumber != null && revisionNumber.isNotEmpty()) {
            try {
                // A nodeId-scoped request binds the context item to a SUBTREE; the
                // vectorized executor serves whole-resource projections and never reads
                // the bound context item, so wiring it would answer subtree queries with
                // whole-resource numbers. Refuse — the plain chain stays exact.
                if (routingContext.queryParam("nodeId").isNotEmpty()) {
                    return SirixCompileChain.createWithNodeAndJsonStore(xmlDBStore, jsonDBStore)
                }
                val databaseName = routingContext.pathParam("database")
                val resourceName = routingContext.pathParam("resource")
                if (databaseName != null && resourceName != null) {
                    val revision = revisionNumber[0]
                    // jn:doc always opens the MOST RECENT revision — when the request pins
                    // an older one, only pure context-item queries may be served.
                    val latest = revision == manager.mostRecentRevisionNumber
                    if (revision >= 1 && VectorizedServingGate.targetsOnlyResource(
                            query, databaseName, resourceName, requireContextItemOnly = !latest
                        )
                    ) {
                        return SirixCompileChain.createWithNodeAndJsonStore(
                            xmlDBStore, jsonDBStore, manager, revision
                        )
                    }
                }
            } catch (e: RuntimeException) {
                // Fall through to the plain chain — never fail the request over an
                // analytical fast-path wiring problem.
            }
        }
        return SirixCompileChain.createWithNodeAndJsonStore(xmlDBStore, jsonDBStore)
    }

    override fun getSerializedBody(
        manager: JsonResourceSession,
        revisions: IntArray,
        nodeId: Long?,
        ctx: RoutingContext
    ): Buffer {
        val nextTopLevelNodes = ctx.queryParam("nextTopLevelNodes").getOrNull(0)?.toInt()
        val startNodeKey = ctx.queryParam("startNodeKey").getOrNull(0)?.toLong()

        if (nodeId != null && startNodeKey != null) {
            requireCursorInSubtree(manager, revisions, nodeId, startNodeKey)
        }

        val numberOfNodes = ctx.queryParam("numberOfNodes").getOrNull(0)?.toLong()
        val maxChildren = ctx.queryParam("maxChildren").getOrNull(0)?.toLong()

        val withMetaData: String? = ctx.queryParam("withMetaData").getOrNull(0)
        val maxLevel: String? = ctx.queryParam("maxLevel").getOrNull(0)
        val prettyPrint: String? = ctx.queryParam("prettyPrint").getOrNull(0)

        if (nextTopLevelNodes == null) {
            // Byte pipeline: the serializer emits UTF-8 straight into the stream (escape-free
            // string values bulk-copy from storage), so the body never round-trips through a
            // String before hitting the wire.
            val out = ByteArrayOutputStream()
            val serializerBuilder = JsonSerializer.newBuilder(manager, out).revisions(revisions)

            nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

            withMetaData?.let {
                when (it) {
                    "nodeKeyAndChildCount" -> serializerBuilder.withNodeKeyAndChildCountMetaData(true)
                    "nodeKey" -> serializerBuilder.withNodeKeyMetaData(true)
                    else -> serializerBuilder.withMetaData(true)
                }
            }

            maxLevel?.let { serializerBuilder.maxLevel(it.toLong()) }

            maxChildren?.let { serializerBuilder.maxChildren(it) }

            if (prettyPrint != null) {
                serializerBuilder.prettyPrint()
            }

            numberOfNodes?.let { serializerBuilder.numberOfNodes(it) }

            val serializer = serializerBuilder.build()

            return JsonSerializeHelper().serialize(serializer, out, ctx, manager, revisions, nodeId)
        } else {
            // Byte pipeline (like the non-paginated branch): the record serializer and its inner
            // per-record serializers share one UTF-8 sink straight into the stream.
            val out = ByteArrayOutputStream()
            val serializerBuilder =
                JsonRecordSerializer.newBuilder(manager, nextTopLevelNodes, out).revisions(revisions)

            nodeId?.let { serializerBuilder.rootNodeKey(it) }

            startNodeKey?.let { serializerBuilder.startNodeKey(it) }

            withMetaData?.let {
                when (it) {
                    "nodeKeyAndChildCount" -> serializerBuilder.withNodeKeyAndChildCountMetaData(true)
                    "nodeKey" -> serializerBuilder.withNodeKeyMetaData(true)
                    else -> serializerBuilder.withMetaData(true)
                }
            }

            maxLevel?.let { serializerBuilder.maxLevel(it.toLong()) }

            maxChildren?.let { serializerBuilder.maxChildren(it) }

            if (prettyPrint != null) {
                serializerBuilder.prettyPrint()
            }

            numberOfNodes?.let { serializerBuilder.numberOfNodes(it) }

            val serializer = serializerBuilder.build()

            return JsonSerializeHelper().serialize(serializer, out, ctx, manager, revisions, nodeId)
        }
    }

    private fun requireCursorInSubtree(
        manager: JsonResourceSession, revisions: IntArray, nodeId: Long, startNodeKey: Long
    ) {
        for (revision in revisions) {
            manager.beginNodeReadOnlyTrx(revision).use { rtx ->
                require(rtx.moveTo(startNodeKey)) { "startNodeKey must exist inside the nodeId subtree" }
                while (rtx.nodeKey != nodeId && rtx.moveToParent()) {
                }
                require(rtx.nodeKey == nodeId) { "startNodeKey must exist inside the nodeId subtree" }
            }
        }
    }

    override suspend fun openDatabase(dbFile: Path): Database<JsonResourceSession> {
        return Databases.openJsonDatabase(dbFile)
    }

    override suspend fun getDBCollection(
        databaseName: String?,
        database: Database<JsonResourceSession>
    ): JsonDBCollection {
        return JsonDBCollectionImpl(databaseName, database)
    }

    override fun handleQueryExtra(
        rtx: JsonNodeReadOnlyTrx,
        dbCollection: JsonDBCollection,
        queryContext: SirixQueryContext,
        jsonSessionDBStore: JsonSessionDBStore
    ) {
        queryContext.contextItem = JsonItemFactory.INSTANCE.getSequence(rtx, dbCollection)
        dbCollection.setJsonDBStore(jsonSessionDBStore)
        jsonSessionDBStore.addDatabase(dbCollection, dbCollection.database)
    }

    override fun createOutputStream(): OutputWrapper = OutputWrapper.StringBuilderWrapper(StringBuilder())
}
