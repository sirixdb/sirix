package org.sirix.cli.commands

import org.brackit.xquery.XQuery
import org.brackit.xquery.util.serialize.Serializer
import org.sirix.access.DatabaseType
import org.sirix.api.Database
import org.sirix.api.ResourceManager
import org.sirix.api.json.JsonResourceManager
import org.sirix.api.xml.XmlNodeReadOnlyTrx
import org.sirix.api.xml.XmlResourceManager
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.RevisionsHelper.Companion.getRevisionsToSerialize
import org.sirix.exception.SirixException
import org.sirix.xquery.JsonDBSerializer
import org.sirix.xquery.SirixCompileChain
import org.sirix.xquery.SirixQueryContext
import org.sirix.xquery.XmlDBSerializer
import org.sirix.xquery.json.*
import org.sirix.xquery.node.BasicXmlDBStore
import org.sirix.xquery.node.XmlDBCollection
import org.sirix.xquery.node.XmlDBNode
import java.io.ByteArrayOutputStream
import java.io.PrintStream

class Query(options: CliOptions, private val queryOptions: QueryOptions) : CliCommand(options) {

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
            val manager = database.openResourceManager(queryOptions.resource)
            manager.use {
                val dbCollection =
                    XmlDBCollection(options.location, database)

                dbCollection.use {
                    val revisionNumber = RevisionsHelper.getRevisionNumber(
                        queryOptions.revision,
                        queryOptions.revisionTimestamp,
                        manager
                    )

                    val trx: XmlNodeReadOnlyTrx
                    trx = manager.beginNodeReadOnlyTrx(revisionNumber[0])

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
            val manager = database.openResourceManager(queryOptions.resource)
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
                            val jsonItem = JsonItemFactory().getSequence(trx, dbCollection)

                            if (jsonItem != null) {
                                queryCtx.contextItem = jsonItem

                                when (jsonItem) {
                                    is AbstractJsonDBArray<*> -> {
                                        jsonItem.getCollection().setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.getCollection(),
                                            jsonItem.getCollection().database
                                        )
                                    }
                                    is JsonDBObject -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.getCollection(),
                                            jsonItem.getCollection().database
                                        )
                                    }
                                    is AtomicJsonDBItem -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.getCollection(),
                                            jsonItem.getCollection().database
                                        )
                                    }
                                    is NumericJsonDBItem -> {
                                        jsonItem.collection.setJsonDBStore(jsonDBStore)
                                        jsonDBStore.addDatabase(
                                            jsonItem.getCollection(),
                                            jsonItem.getCollection().database
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
                                                JsonDBSerializer(out, queryOptions.prettyPrint)
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
            val manager = database.openResourceManager(queryOptions.resource)
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


    private fun getRevisions(manager: ResourceManager<*, *>): Array<Int> {
        return when (manager) {
            is JsonResourceManager -> getRevisionsToSerialize(
                queryOptions.startRevision,
                queryOptions.endRevision,
                queryOptions.startRevisionTimestamp,
                queryOptions.endRevisionTimestamp,
                manager,
                queryOptions.revision,
                queryOptions.revisionTimestamp
            )
            is XmlResourceManager -> getRevisionsToSerialize(
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
