package org.sirix.cli.commands

import org.sirix.access.DatabaseType
import org.sirix.access.DatabaseType.JSON
import org.sirix.access.DatabaseType.XML
import org.sirix.access.Databases
import org.sirix.api.Database
import org.sirix.api.ResourceManager
import org.sirix.api.json.JsonResourceManager
import org.sirix.api.xml.XmlResourceManager
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.RevisionsHelper.Companion.getRevisionsToSerialize

class Query(options: CliOptions, private val queryOptions: QueryOptions) : CliCommand(options) {

    var type: DatabaseType? = null

    override fun execute() {
        type = Databases.getDatabaseType(path())
        when (type) {
            JSON -> executeQuery(openJsonDatabase(queryOptions.user))
            XML -> executeQuery(openXmlDatabase(queryOptions.user))
            else -> throw IllegalStateException("Unknown Database Type!")
        }
    }


    private fun executeQuery(database: Database<*>) {
        database.use {
            val manager = database.openResourceManager(queryOptions.resource)
            manager.use {
                if (queryOptions.hasQueryStr()) {
                } else {
                    serializeResource(manager)
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

    private fun serializeResource(manager: ResourceManager<*, *>) {
        val revisions: Array<Int> = getRevisions(manager)
        with(queryOptions) {
            val serializerAdapter =
                QuerySerializerAdapter(manager, nextTopLevelNodes).revisions(revisions.toIntArray())
                    .startNodeKey(nodeId)
                    .metadata(metaData).maxLevel(maxLevel).prettyPrint(prettyPrint)

            cliPrinter.prnLn(serializerAdapter.serialize())
        }
    }
}
