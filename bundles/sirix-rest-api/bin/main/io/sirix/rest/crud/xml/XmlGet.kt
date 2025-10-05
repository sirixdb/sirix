package io.sirix.rest.crud.xml

import io.vertx.core.Context
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.auth.authorization.AuthorizationProvider
import io.vertx.ext.auth.oauth2.OAuth2Auth
import io.vertx.ext.web.RoutingContext
import io.sirix.access.Databases
import io.sirix.api.Database
import io.sirix.api.xml.XmlNodeReadOnlyTrx
import io.sirix.api.xml.XmlResourceSession
import io.sirix.rest.crud.PermissionCheckingQuery
import io.sirix.rest.crud.QuerySerializer
import io.sirix.rest.crud.json.JsonSessionDBStore
import io.sirix.service.xml.serialize.XmlSerializer
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.XmlDBSerializer
import io.sirix.rest.crud.AbstractGetHandler
import io.sirix.query.node.XmlDBCollection
import io.sirix.query.node.XmlDBNode
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Path

class XmlGet(private val location: Path, private val keycloak: OAuth2Auth, private val authz: AuthorizationProvider): AbstractGetHandler <XmlResourceSession, XmlDBCollection, XmlNodeReadOnlyTrx> (location, authz) {
//    override suspend fun xquery(
//        manager: XmlResourceSession?,
//        dbCollection: XmlDBCollection?,
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
//            var body: String?
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
//
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

    override suspend fun serializeResource(
        manager: XmlResourceSession, revisions: IntArray, nodeId: Long?,
        ctx: RoutingContext,
        vertxContext: Context
    ): String {
        val out = ByteArrayOutputStream()

        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out).revisions(revisions)

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        if (ctx.queryParam("maxLevel").isNotEmpty())
            serializerBuilder.maxLevel(ctx.queryParam("maxLevel")[0].toLong())

        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        return XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
    }

    override fun  handleQueryExtra(rtx: XmlNodeReadOnlyTrx, dbCollection: XmlDBCollection, queryContext: SirixQueryContext, jsonDBStore: JsonSessionDBStore) {
        val dbNode = XmlDBNode(rtx, dbCollection)

        queryContext.contextItem = dbNode
    }


    override suspend fun openDatabase(dbFile: Path): Database<XmlResourceSession> {
        return Databases.openXmlDatabase(dbFile)
    }

    override suspend fun getDBCollection(databaseName: String?, database: Database<XmlResourceSession>): XmlDBCollection {
        return XmlDBCollection(databaseName, database)
    }
}