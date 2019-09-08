package org.sirix.rest.crud.json

import io.netty.handler.codec.http.HttpResponseStatus
import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.ext.auth.User
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.impl.HttpStatusException
import io.vertx.kotlin.core.executeBlockingAwait
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.withContext
import org.brackit.xquery.XQuery
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.json.JsonNodeReadOnlyTrx
import org.sirix.api.json.JsonResourceManager
import org.sirix.exception.SirixUsageException
import org.sirix.node.NodeKind
import org.sirix.service.json.serialize.JsonSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.json.BasicJsonDBStore
import org.sirix.xquery.json.JsonDBArray
import org.sirix.xquery.json.JsonDBCollection
import org.sirix.xquery.json.JsonDBItem
import org.sirix.xquery.json.JsonDBObject
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.stream.Collectors.toList
import org.sirix.xquery.JsonDBSerializer

class JsonGet(private val location: Path) {
    suspend fun handle(ctx: RoutingContext): Route {
        val vertxContext = ctx.vertx().orCreateContext
        val dbName: String? = ctx.pathParam("database")
        val resName: String? = ctx.pathParam("resource")

        val query: String? = ctx.queryParam("query").getOrElse(0) { ctx.bodyAsString }

        if (dbName == null && resName == null) {
            if (query == null || query.isEmpty())
                listDatabases(ctx)
            else
                xquery(query, null, ctx, vertxContext, ctx.get("user") as User)
        } else {
            get(dbName, ctx, resName, query, vertxContext, ctx.get("user") as User)
        }

        return ctx.currentRoute()
    }

    private fun listDatabases(ctx: RoutingContext) {
        val databases = Files.list(location)

        val buffer = StringBuilder()

        buffer.append("{\"databases\":[")

        databases.use {
            val databasesList = it.collect(toList())
            for ((index, database) in databasesList.withIndex()) {
                if (Files.isDirectory(database)) {
                    buffer.append(database.fileName)

                    if (index != databasesList.size)
                        buffer.append(",")
                }
            }
        }

        buffer.append("]}")

        val content = buffer.toString()

        ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/xml")
                .putHeader("Content-Length", content.length.toString())
                .write(content)
                .end()
    }

    private suspend fun get(dbName: String?, ctx: RoutingContext, resName: String?, query: String?,
                            vertxContext: Context, user: User) {
        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)
        val startRevision: String? = ctx.queryParam("start-revision").getOrNull(0)
        val endRevision: String? = ctx.queryParam("end-revision").getOrNull(0)
        val startRevisionTimestamp: String? = ctx.queryParam("start-revision-timestamp").getOrNull(0)
        val endRevisionTimestamp: String? = ctx.queryParam("end-revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)

        val database: Database<JsonResourceManager>
        try {
            database = Databases.openJsonDatabase(location.resolve(dbName))
        } catch (e: SirixUsageException) {
            ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
            return
        }

        database.use {
            val manager: JsonResourceManager
            try {
                if (resName == null) {
                    val buffer = StringBuilder()
                    buffer.append("{\"databases\":[")

                    val resources = it.listResources()

                    for ((index, resource) in resources.withIndex()) {
                        buffer.append(resource)
                        if (index != resources.size)
                            buffer.append(",")
                    }

                    buffer.append("]}")
                } else {
                    manager = database.openResourceManager(resName)

                    manager.use {
                        if (query != null && query.isNotEmpty()) {
                            queryResource(dbName, database, revision, revisionTimestamp, manager, ctx, nodeId, query,
                                    vertxContext, user)
                        } else {
                            val revisions: Array<Int> =
                                    getRevisionsToSerialize(startRevision, endRevision, startRevisionTimestamp,
                                            endRevisionTimestamp, manager, revision, revisionTimestamp)
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

    private fun getRevisionsToSerialize(startRevision: String?, endRevision: String?, startRevisionTimestamp: String?,
                                        endRevisionTimestamp: String?, manager: JsonResourceManager, revision: String?,
                                        revisionTimestamp: String?): Array<Int> {
        return when {
            startRevision != null && endRevision != null -> parseIntRevisions(startRevision, endRevision)
            startRevisionTimestamp != null && endRevisionTimestamp != null -> {
                val tspRevisions = parseTimestampRevisions(startRevisionTimestamp, endRevisionTimestamp)
                getRevisionNumbers(manager, tspRevisions).toList().toTypedArray()
            }
            else -> getRevisionNumber(revision, revisionTimestamp, manager)
        }
    }

    private suspend fun queryResource(dbName: String?, database: Database<JsonResourceManager>, revision: String?,
                                      revisionTimestamp: String?, manager: JsonResourceManager, ctx: RoutingContext,
                                      nodeId: String?, query: String, vertxContext: Context, user: User) {

        withContext(vertxContext.dispatcher()) {
            val dbCollection = JsonDBCollection(dbName, database)

            dbCollection.use {
                val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)

                val trx: JsonNodeReadOnlyTrx
                try {
                    trx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    trx.use {
                        if (nodeId == null)
                            trx.moveToFirstChild()
                        else
                            trx.moveTo(nodeId.toLong())

                        val jsonItem: JsonDBItem = when {
                            trx.kind === NodeKind.ARRAY -> JsonDBArray(trx, dbCollection)
                            trx.kind === NodeKind.OBJECT -> JsonDBObject(trx, dbCollection)
                            else -> throw IllegalStateException()
                        }

                        xquery(query, jsonItem, ctx, vertxContext, user)
                    }
                } catch (e: SirixUsageException) {
                    ctx.fail(HttpStatusException(HttpResponseStatus.NOT_FOUND.code(), e))
                }
            }
        }
    }

    private fun getRevisionNumber(rev: String?, revTimestamp: String?, manager: JsonResourceManager): Array<Int> {
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

    private suspend fun xquery(query: String, node: JsonDBItem?, routingContext: RoutingContext, vertxContext: Context,
                               user: User) {
        vertxContext.executeBlockingAwait { future: Future<Nothing> ->
            // Initialize queryResource context and store.
            val dbStore = JsonSessionDBStore(BasicJsonDBStore.newBuilder().build(), user)

            dbStore.use {
                val queryCtx = SirixQueryContext.createWithJsonStore(dbStore)

                node.let { queryCtx.contextItem = node }

                val out = StringBuilder()

                SirixCompileChain.createWithJsonStore(dbStore).use { sirixCompileChain ->
                    val serializer = JsonDBSerializer(out, false)
                    XQuery(sirixCompileChain, query).prettyPrint().serialize(queryCtx, serializer)
                }

                val body = out.toString()

                routingContext.response().setStatusCode(200)
                        .putHeader("Content-Type", "application/json")
                        .putHeader("Content-Length", body.length.toString())
                        .write(body)
                        .end()
            }

            future.complete(null)
        }
    }

    private fun getRevisionNumber(manager: JsonResourceManager, revision: String): Int {
        val revisionDateTime = LocalDateTime.parse(revision)
        val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
        return manager.getRevisionNumber(zdt.toInstant())
    }

    private fun getRevisionNumbers(manager: JsonResourceManager,
                                   revisions: Pair<LocalDateTime, LocalDateTime>): Array<Int> {
        val zdtFirstRevision = revisions.first.atZone(ZoneId.systemDefault())
        val zdtLastRevision = revisions.second.atZone(ZoneId.systemDefault())
        var firstRevisionNumber = manager.getRevisionNumber(zdtFirstRevision.toInstant())
        var lastRevisionNumber = manager.getRevisionNumber(zdtLastRevision.toInstant())

        if (firstRevisionNumber == 0) ++firstRevisionNumber
        if (lastRevisionNumber == 0) ++lastRevisionNumber

        return (firstRevisionNumber..lastRevisionNumber).toSet().toTypedArray()
    }

    private fun serializeResource(manager: JsonResourceManager, revisions: Array<Int>, nodeId: Long?,
                                  ctx: RoutingContext) {
        val out = StringWriter()

        val serializerBuilder = JsonSerializer.newBuilder(manager, out).revisions(revisions.toIntArray())

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        val serializer = serializerBuilder.build()

        JsonSerializeHelper().serialize(serializer, out, ctx, manager, nodeId)
    }

    private fun parseIntRevisions(startRevision: String, endRevision: String): Array<Int> {
        return (startRevision.toInt()..endRevision.toInt()).toSet().toTypedArray()
    }

    private fun parseTimestampRevisions(startRevision: String,
                                        endRevision: String): Pair<LocalDateTime, LocalDateTime> {
        val firstRevisionDateTime = LocalDateTime.parse(startRevision)
        val lastRevisionDateTime = LocalDateTime.parse(endRevision)

        return Pair(firstRevisionDateTime, lastRevisionDateTime)
    }
}