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
import org.sirix.access.DatabaseType
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.xml.XmlNodeReadOnlyTrx
import org.sirix.api.xml.XmlResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.rest.crud.QuerySerializer
import org.sirix.rest.crud.SirixDBUtils
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
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

class XmlGet(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val context = ctx.vertx().orCreateContext
        val databaseName: String? = ctx.pathParam("database")
        val resource: String? = ctx.pathParam("resource")

        val query: String? = ctx.queryParam("query").getOrElse(0) {
            val json = ctx.bodyAsJson
            json.getString("query")
        }

        if (databaseName == null && resource == null) {
            if (query == null || query.isEmpty()) {
                listDatabases(ctx, context)
            } else {
                val json = ctx.bodyAsJson
                val startResultSeqIndex = json.getString("startResultSeqIndex")
                val endResultSeqIndex = json.getString("endResultSeqIndex")
                xquery(
                    query,
                    null,
                    ctx,
                    context,
                    ctx.get("user") as User,
                    startResultSeqIndex?.toLong(),
                    endResultSeqIndex?.toLong()
                )
            }
        } else {
            get(databaseName, ctx, resource, query, context, ctx.get("user") as User)
        }

        return ctx.currentRoute()
    }

    private suspend fun listDatabases(ctx: RoutingContext, context: Context) {
        context.executeBlockingAwait { _: Promise<Unit> ->
            val databases = Files.list(location)

            val buffer = StringBuilder()

            buffer.appendln("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">")

            databases.use {
                databases.filter { Files.isDirectory(it) }.forEach { database ->
                    val databaseName = database.fileName
                    val databaseType = Databases.getDatabaseType(database.toAbsolutePath()).stringType
                    buffer.appendln("  <rest:item databaseName=\"${databaseName}\" dbtype=\"${databaseType}\" />")
                }
            }

            buffer.append("</rest:sequence>")

            val content = buffer.toString()

            ctx.response().setStatusCode(200)
                .putHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
                .putHeader(HttpHeaders.CONTENT_LENGTH, content.toByteArray(StandardCharsets.UTF_8).size.toString())
                .write(content)
                .end()
        }
    }

    private suspend fun get(
        databaseName: String?, ctx: RoutingContext, resource: String?, query: String?,
        vertxContext: Context, user: User
    ) {
        val history = ctx.pathParam("history")

        if (history != null && databaseName != null && resource != null) {
            vertxContext.executeBlockingAwait { _: Promise<Unit> ->
                SirixDBUtils.getHistory(ctx, location, databaseName, resource, DatabaseType.XML)
            }

            return
        }

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
                if (resource == null) {
                    val buffer = StringBuilder()
                    buffer.appendln("<rest:sequence xmlns:rest=\"https://sirix.io/rest\">")

                    for (resource in it.listResources()) {
                        buffer.appendln("  <rest:item resource-name=\"${resource.fileName}\"/>")
                    }

                    buffer.appendln("</rest:sequence>")
                } else {
                    val manager = database.openResourceManager(resource)

                    manager.use {
                        if (query != null && query.isNotEmpty()) {
                            queryResource(
                                databaseName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                                vertxContext, user
                            )
                        } else {
                            val revisions: Array<Int> =
                                getRevisionsToSerialize(
                                    startRevision, endRevision, startRevisionTimestamp,
                                    endRevisionTimestamp, manager, revision, revisionTimestamp
                                )
                            serializeResource(manager, revisions, nodeId?.toLongOrNull(), ctx)
                        }
                    }
                }
            } catch (e: SirixUsageException) {
                ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
                return
            }

        }
    }

    private fun getRevisionsToSerialize(
        startRevision: String?, endRevision: String?, startRevisionTimestamp: String?,
        endRevisionTimestamp: String?, manager: XmlResourceManager, revision: String?,
        revisionTimestamp: String?
    ): Array<Int> {
        return when {
            startRevision != null && endRevision != null -> parseIntRevisions(startRevision, endRevision)
            startRevisionTimestamp != null && endRevisionTimestamp != null -> {
                val tspRevisions = parseTimestampRevisions(startRevisionTimestamp, endRevisionTimestamp)
                getRevisionNumbers(manager, tspRevisions).toList().toTypedArray()
            }
            else -> getRevisionNumber(revision, revisionTimestamp, manager)
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
                val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)

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

    private fun getRevisionNumber(rev: String?, revTimestamp: String?, manager: XmlResourceManager): Array<Int> {
        return if (rev != null) {
            arrayOf(rev.toInt())
        } else if (revTimestamp != null) {
            var revision = getRevisionNumber(manager, revTimestamp)
            if (revision == 0)
                arrayOf(++revision)
            else
                arrayOf(revision)
        } else {
            arrayOf(manager.mostRecentRevisionNumber)
        }
    }

    private suspend fun xquery(
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

    private fun getRevisionNumber(manager: XmlResourceManager, revision: String): Int {
        val revisionDateTime = LocalDateTime.parse(revision)
        val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
        return manager.getRevisionNumber(zdt.toInstant())
    }

    private fun getRevisionNumbers(
        manager: XmlResourceManager,
        revisions: Pair<LocalDateTime, LocalDateTime>
    ): Array<Int> {
        val zdtFirstRevision = revisions.first.atZone(ZoneId.systemDefault())
        val zdtLastRevision = revisions.second.atZone(ZoneId.systemDefault())
        var firstRevisionNumber = manager.getRevisionNumber(zdtFirstRevision.toInstant())
        var lastRevisionNumber = manager.getRevisionNumber(zdtLastRevision.toInstant())

        if (firstRevisionNumber == 0) ++firstRevisionNumber
        if (lastRevisionNumber == 0) ++lastRevisionNumber

        return (firstRevisionNumber..lastRevisionNumber).toSet().toTypedArray()
    }

    private fun serializeResource(
        manager: XmlResourceManager, revisions: Array<Int>, nodeId: Long?,
        ctx: RoutingContext
    ) {
        val out = ByteArrayOutputStream()

        val serializerBuilder = XmlSerializer.XmlSerializerBuilder(manager, out).revisions(revisions.toIntArray())

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        XmlSerializeHelper().serializeXml(serializer, out, ctx, manager, nodeId)
    }

    private fun parseIntRevisions(startRevision: String, endRevision: String): Array<Int> {
        return (startRevision.toInt()..endRevision.toInt()).toSet().toTypedArray()
    }

    private fun parseTimestampRevisions(
        startRevision: String,
        endRevision: String
    ): Pair<LocalDateTime, LocalDateTime> {
        val firstRevisionDateTime = LocalDateTime.parse(startRevision)
        val lastRevisionDateTime = LocalDateTime.parse(endRevision)

        return Pair(firstRevisionDateTime, lastRevisionDateTime)
    }
}