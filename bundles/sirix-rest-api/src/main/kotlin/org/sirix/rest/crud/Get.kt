package org.sirix.rest.crud

import io.vertx.core.Handler
import io.vertx.ext.web.RoutingContext
import org.brackit.xquery.XQuery
import org.sirix.access.Databases
import org.sirix.api.ResourceManager
import org.sirix.rest.Serialize
import org.sirix.service.xml.serialize.XMLSerializer
import org.sirix.xquery.DBSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.node.DBCollection
import org.sirix.xquery.node.DBNode
import org.sirix.xquery.node.DBStore
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZoneId

class Get(private val location: Path) : Handler<RoutingContext> {
    override fun handle(ctx: RoutingContext) {
        val dbName: String? = ctx.pathParam("database")
        val resName: String? = ctx.pathParam("resource")

        val revision: String? = ctx.queryParam("revision").getOrNull(0)
        val revisionTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)
        val startRevision: String? = ctx.queryParam("start-revision").getOrNull(0)
        val endRevision: String? = ctx.queryParam("end-revision").getOrNull(0)
        val startRevisionTimestamp: String? = ctx.queryParam("start-revision-timestamp").getOrNull(0)
        val endRevisionTimestamp: String? = ctx.queryParam("end-revision-timestamp").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val query: String? = ctx.queryParam("query").getOrNull(0)

        if (dbName == null && resName == null) {
            if (query == null)
                ctx.fail(IllegalArgumentException("Query must be given if database name and resource name are not given."))
            else
                xquery(query, null, ctx)
        }

        if (ctx.failed())
            return

        val database = Databases.openDatabase(location.resolve(dbName))

        database.use {
            val manager = database.getResourceManager(resName)

            manager.use {
                if (query != null) {
                    val dbCollection = DBCollection(dbName, database)

                    dbCollection.use {
                        val revisionNumber = getRevisionNumber(revision, revisionTimestamp, manager)
                        val trx = manager.beginNodeReadTrx(revisionNumber[0])

                        trx.use {
                            nodeId?.let { trx.moveTo(nodeId.toLong()) }
                            val dbNode = DBNode(trx, dbCollection)

                            xquery(query, dbNode, ctx)
                        }
                    }
                } else {
                    val revisions: Array<Int> = when {
                        startRevision != null && endRevision != null -> parseIntRevisions(startRevision, endRevision, ctx)
                        startRevisionTimestamp != null && endRevisionTimestamp != null -> {
                            val tspRevisions = parseTimestampRevisions(startRevisionTimestamp, endRevisionTimestamp, ctx)
                            getRevisionNumbers(manager, tspRevisions).toList().toTypedArray()
                        }
                        else -> getRevisionNumber(revision, revisionTimestamp, manager)
                    }

                    serializeResource(manager, revisions, nodeId?.toLongOrNull(), ctx)
                }
            }
        }
    }

    private fun getRevisionNumber(rev: String?, revTimestamp: String?, manager: ResourceManager): Array<Int> {
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

    private fun xquery(query: String, node: DBNode?, ctx: RoutingContext) {
        // Initialize query context and store.
        val dbStore = DBStore.newBuilder().build()

        dbStore.use {
            val queryCtx = SirixQueryContext(dbStore)
            node.let { queryCtx.contextItem = node }

            // Use XQuery to load sample document into store.
            val out = ByteArrayOutputStream()

            out.use {
                PrintStream(out).use {
                    XQuery(SirixCompileChain(dbStore), query).prettyPrint().serialize(queryCtx, DBSerializer(it, true, true))
                }

                val body = String(out.toByteArray(), StandardCharsets.UTF_8)

                ctx.response().setStatusCode(200)
                        .putHeader("Content-Type", "application/xml")
                        .putHeader("Content-Length", body.length.toString())
                        .write(body)
                        .end()
            }
        }
    }

    private fun getRevisionNumber(manager: ResourceManager, revision: String): Int {
        val revisionDateTime = LocalDateTime.parse(revision)
        val zdt = revisionDateTime.atZone(ZoneId.systemDefault())
        return manager.getRevisionNumber(zdt.toInstant())
    }

    private fun getRevisionNumbers(manager: ResourceManager, revisions: Pair<LocalDateTime, LocalDateTime>): Array<Int> {
        val zdtFirstRevision = revisions.first.atZone(ZoneId.systemDefault())
        val zdtLastRevision = revisions.second.atZone(ZoneId.systemDefault())
        var firstRevisionNumber = manager.getRevisionNumber(zdtFirstRevision.toInstant())
        var lastRevisionNumber = manager.getRevisionNumber(zdtLastRevision.toInstant())

        if (firstRevisionNumber == 0) ++firstRevisionNumber
        if (lastRevisionNumber == 0) ++lastRevisionNumber

        return (firstRevisionNumber..lastRevisionNumber).toSet().toTypedArray()
    }

    private fun serializeResource(manager: ResourceManager, revisions: Array<Int>, nodeId: Long?, ctx: RoutingContext) {
        val out = ByteArrayOutputStream()

        val serializerBuilder = XMLSerializer.XMLSerializerBuilder(manager, out).revisions(revisions.toIntArray())

        nodeId?.let { serializerBuilder.startNodeKey(nodeId) }

        val serializer = serializerBuilder.emitIDs().emitRESTful().emitRESTSequence().prettyPrint().build()

        Serialize().serializeXml(serializer, out, ctx)
    }

    private fun parseIntRevisions(startRevision: String, endRevision: String, ctx: RoutingContext): Array<Int> {
        return (startRevision.toInt()..endRevision.toInt()).toSet().toTypedArray()
    }

    private fun parseTimestampRevisions(startRevision: String, endRevision: String, ctx: RoutingContext): Pair<LocalDateTime, LocalDateTime> {
        val firstRevisionDateTime = LocalDateTime.parse(startRevision)
        val lastRevisionDateTime = LocalDateTime.parse(endRevision)

        return Pair(firstRevisionDateTime, lastRevisionDateTime)
    }
}