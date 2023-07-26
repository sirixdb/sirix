package io.sirix.cli.commands

import io.sirix.access.DatabaseType
import io.sirix.api.Database
import io.sirix.api.ResourceSession
import io.sirix.api.json.JsonResourceSession
import io.sirix.api.xml.XmlResourceSession
import io.sirix.cli.commands.RevisionsHelper.Companion.getRevisionsToSerialize
import io.sirix.exception.SirixException
import io.sirix.query.JsonDBSerializer
import io.sirix.query.SirixCompileChain
import io.sirix.query.SirixQueryContext
import io.sirix.query.XmlDBSerializer
import io.sirix.query.json.*
import io.sirix.query.node.BasicXmlDBStore
import io.sirix.query.node.XmlDBCollection
import io.sirix.query.node.XmlDBNode
import org.brackit.xquery.XQuery
import org.brackit.xquery.util.serialize.Serializer
import java.io.ByteArrayOutputStream
import java.io.PrintStream

class Query(options: io.sirix.cli.CliOptions, private val queryOptions: QueryOptions) : CliCommand(options) {

    override fun execute() {
        val startMillis: Long = System.currentTimeMillis()
        cliPrinter.prnLnV("Execute Query. Result is:")
        val result = if (queryOptions.hasQueryStr()) {
            xQuery()
        } else {
            serializeResource()
        }
        cliPrinter.prnLn(result)
        cliPrinter.prnLnV("Query executed (${System.currentTimeMillis() - startMillis}ms)")
    }

    private fun xQuery(): String {
        return when (databaseType()) {
            DatabaseType.XML -> xQueryXml()
            DatabaseType.JSON -> xQueryJson()
            else -> throw IllegalArgumentException("Unknown Database Type!")
        }

    }

    private fun xQueryXml(): String {
        val database = openXmlDatabase(queryOptions.user)
        database.use {
            val manager = database.beginResourceSession(queryOptions.resource)
            manager.use {
                val dbCollection =
                    XmlDBCollection(options.location, database)

                dbCollection.use {
                    val revisionNumber = RevisionsHelper.getRevisionNumber(
                        queryOptions.revision,
                        queryOptions.revisionTimestamp,
                        manager
                    )

                    val trx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    trx.use {
                        if (queryOptions.nodeId == null)
                            trx.moveToFirstChild()
                        else
                            trx.moveTo(queryOptions.nodeId)

                        val dbStore = BasicXmlDBStore.newBuilder().build()
                        dbStore.use {
                            val queryCtx = SirixQueryContext.createWithNodeStore(dbStore)

                            val node = XmlDBNode(trx, dbCollection)
                            node.let { queryCtx.contextItem = node }

                            val out = ByteArrayOutputStream()

                            PrintStream(out).use { printStream ->
                                SirixCompileChain.createWithNodeStore(dbStore).use { sirixCompileChain ->
                                    if (queryOptions.startResultSeqIndex == null) {
                                        XQuery(sirixCompileChain, queryOptions.queryStr).prettyPrint().serialize(
                                            queryCtx,
                                            XmlDBSerializer(printStream, false, true)
                                        )
                                    } else {
                                        serializePaginated(
                                            sirixCompileChain,
                                            queryCtx,
                                            XmlDBSerializer(printStream, false, true)
                                        )
                                    }
                                }
                                return out.toString()
                            }
                        }
                    }
                }
            }
        }
    }

    private fun xQueryJson(): String {
        val database = openJsonDatabase(queryOptions.user)
        database.use {
            val manager = database.beginResourceSession(queryOptions.resource)
            manager.use {
                val dbCollection = JsonDBCollection(options.location, database)
                dbCollection.use {
                    val revisionNumber = RevisionsHelper.getRevisionNumber(
                        queryOptions.revision,
                        queryOptions.revisionTimestamp,
                        manager
                    )
                    val trx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

                    trx.use {
                        if (queryOptions.nodeId == null)
                            trx.moveToFirstChild()
                        else
                            trx.moveTo(queryOptions.nodeId.toLong())

                        val jsonDBStore = BasicJsonDBStore.newBuilder().build()
                        val xmlDBStore = BasicXmlDBStore.newBuilder().build()
                        val queryCtx = SirixQueryContext.createWithJsonStoreAndNodeStoreAndCommitStrategy(
                            xmlDBStore,
                            jsonDBStore,
                            SirixQueryContext.CommitStrategy.AUTO
                        )

                        queryCtx.use {
                            val jsonItem = JsonItemFactory()
                                .getSequence(trx, dbCollection)

                            if (jsonItem != null) {
                                queryCtx.contextItem = jsonItem

                                when (jsonItem) {
                                    is AbstractJsonDBArray<*> -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.collection,
                                            jsonItem.collection.database
                                        )
                                    }
                                    is JsonDBObject -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.collection,
                                            jsonItem.collection.database
                                        )
                                    }
                                    is AtomicBooleanJsonDBItem -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.collection,
                                            jsonItem.collection.database
                                        )
                                    }
                                    is AtomicStrJsonDBItem -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.collection,
                                            jsonItem.collection.database
                                        )
                                    }
                                    is AtomicNullJsonDBItem -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.collection,
                                            jsonItem.collection.database
                                        )
                                    }
                                    is NumericJsonDBItem -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.collection,
                                            jsonItem.collection.database
                                        )
                                    }
                                    else -> throw IllegalStateException("Node type not known.")
                                }
                            }

                            val out = StringBuilder()
                            SirixCompileChain.createWithNodeAndJsonStore(xmlDBStore, jsonDBStore)
                                .use { sirixCompileChain ->
                                    if (queryOptions.startResultSeqIndex == null) {
                                        XQuery(sirixCompileChain, queryOptions.queryStr).prettyPrint()
                                            .serialize(
                                                queryCtx,
                                                JsonDBSerializer(
                                                    out,
                                                    queryOptions.prettyPrint
                                                )
                                            )
                                    } else {
                                        serializePaginated(
                                            sirixCompileChain,
                                            queryCtx,
                                            JsonDBSerializer(out, false)
                                        )
                                    }
                                }
                            return out.toString()
                        }
                    }
                }
            }
        }
    }

    private fun serializePaginated(
        sirixCompileChain: SirixCompileChain,
        queryCtx: SirixQueryContext?,
        serializer: Serializer
    ) {
        if (queryOptions.startResultSeqIndex == null) {
            throw SirixException("startResultSeqIndex can't be null!")
        }

        serializer.use {
            val sequence = XQuery(sirixCompileChain, queryOptions.queryStr).execute(queryCtx)

            if (sequence != null) {
                val itemIterator = sequence.iterate()

                for (i in 0 until queryOptions.startResultSeqIndex) {
                    itemIterator.next()
                }

                if (queryOptions.endResultSeqIndex == null) {
                    while (true) {
                        val item = itemIterator.next()

                        if (item == null)
                            break
                        else
                            serializer.serialize(item)
                    }
                } else {
                    for (i in queryOptions.startResultSeqIndex..queryOptions.endResultSeqIndex) {
                        val item = itemIterator.next()

                        if (item == null)
                            break
                        else
                            serializer.serialize(item)
                    }
                }
            }
        }
    }


    private fun serializeResource(): String {
        val database: Database<*> = openDatabase(queryOptions.user)
        database.use {
            val manager = database.beginResourceSession(queryOptions.resource)
            manager.use {
                val revisions: Array<Int> = getRevisions(manager)
                with(queryOptions) {
                    val serializerAdapter =
                        SerializerAdapter(manager, nextTopLevelNodes).revisions(revisions.toIntArray())
                            .startNodeKey(nodeId)
                            .metadata(metaData).maxLevel(maxLevel).prettyPrint(prettyPrint)

                    return serializerAdapter.serialize()
                }
            }
        }
    }


    private fun getRevisions(manager: ResourceSession<*, *>): Array<Int> {
        return when (manager) {
            is JsonResourceSession -> getRevisionsToSerialize(
                queryOptions.startRevision,
                queryOptions.endRevision,
                queryOptions.startRevisionTimestamp,
                queryOptions.endRevisionTimestamp,
                manager,
                queryOptions.revision,
                queryOptions.revisionTimestamp
            )
            is XmlResourceSession -> getRevisionsToSerialize(
                queryOptions.startRevision,
                queryOptions.endRevision,
                queryOptions.startRevisionTimestamp,
                queryOptions.endRevisionTimestamp,
                manager,
                queryOptions.revision,
                queryOptions.revisionTimestamp
            )
            else -> throw IllegalStateException("Unknown ResourceManager Type!")
        }

    }

}
