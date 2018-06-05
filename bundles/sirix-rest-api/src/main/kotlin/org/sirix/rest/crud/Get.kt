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
        val dbName = ctx.pathParam("database")
        val resName = ctx.pathParam("resource")

        val rev: String? = ctx.queryParam("revision").getOrNull(0)
        val revTimestamp: String? = ctx.queryParam("revision-timestamp").getOrNull(0)
        val revRange: String? = ctx.queryParam("revision-range").getOrNull(0)
        val revRangeByTimestamp: String? = ctx.queryParam("revision-timestamp-range").getOrNull(0)

        val nodeId: String? = ctx.queryParam("nodeId").getOrNull(0)
        val query: String? = ctx.queryParam("query").getOrNull(0)

        checkPreconditions(rev, revTimestamp, revRange, revRangeByTimestamp, ctx, dbName, resName)

        if (ctx.failed())
            return

        val database = Databases.openDatabase(location.resolve(dbName))

        database.use {
            val manager = database.getResourceManager(resName)

            manager.use {
                if (query != null) {
                    val dbCollection = DBCollection(dbName, database)

                    dbCollection.use {
                        val revisionNumber = getRevisionNumber(rev, revTimestamp, manager)
                        val trx = manager.beginNodeReadTrx(revisionNumber[0])

                        trx.use {
                            nodeId?.let { trx.moveTo(nodeId.toLong()) }
                            val dbNode = DBNode(trx, dbCollection)

                            xquery(query, dbNode, ctx)
                        }
                    }
                } else {
                    val revisions: Array<Int> = when {
                        revRange != null -> parseIntRevisions(revRange, ctx)
                        revRangeByTimestamp != null -> {
                            val tspRevisions = parseTimestampRevisions(revRangeByTimestamp, ctx)
                            getRevisionNumbers(manager, tspRevisions).toList().toTypedArray()
                        }
                        else -> getRevisionNumber(rev, revTimestamp, manager)
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

    private fun xquery(query: String, node: DBNode, ctx: RoutingContext) {
        // Initialize query context and store.
        val dbStore = DBStore.newBuilder().build()

        dbStore.use {
            val queryCtx = SirixQueryContext(dbStore)
            queryCtx.contextItem = node

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

    private fun checkPreconditions(rev: String?, revTsp: String?, revRange: String?, revRangeByTsp: String?, ctx: RoutingContext, dbName: String?, resName: String?) {
        if (rev != null && (revTsp != null || revRange != null || revRangeByTsp != null)
                || revTsp != null && (rev != null || revRange != null || revRangeByTsp != null)
                || revRange != null && (rev != null || revTsp != null || revRangeByTsp != null)
                || revRangeByTsp != null && (rev != null || revRange != null || revTsp != null))
            ctx.fail(IllegalArgumentException("Either revision, revision-tsp, revision-range param or revision-tsp-range param must be specified but not more than one."))

        if (dbName == null || resName == null)
            ctx.fail(IllegalArgumentException("Database name and resource data to store not given."))
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

    private fun parseIntRevisions(revisions: String, ctx: RoutingContext): Array<Int> {
        val (firstRevision: String, lastRevision: String) = split(revisions, ctx, '-')

        return (firstRevision.toInt()..lastRevision.toInt()).toSet().toTypedArray()
    }

    private fun split(revisions: String, ctx: RoutingContext, separator: Char): Pair<String, String> {
        if (revisions.indexOf(separator) == -1) {
            ctx.fail(IllegalArgumentException("revision-range must specify a hyphen to disambiguate start- and end-revision"))
        }

        val firstRevision = revisions.substring(0, revisions.indexOf(separator))
        val lastRevision = revisions.substring(revisions.indexOf(separator) + 1)

        return Pair(firstRevision, lastRevision)
    }

    private fun parseTimestampRevisions(revisions: String, ctx: RoutingContext): Pair<LocalDateTime, LocalDateTime> {
        val (firstRevision: String, lastRevision: String) = split(revisions, ctx, '|')

        val firstRevisionDateTime = LocalDateTime.parse(firstRevision)
        val lastRevisionDateTime = LocalDateTime.parse(lastRevision)

        return Pair(firstRevisionDateTime, lastRevisionDateTime)
    }
}