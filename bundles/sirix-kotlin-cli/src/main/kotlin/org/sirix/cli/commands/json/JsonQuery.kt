package org.sirix.cli.commands.json

import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.QueryOptions
import org.sirix.xquery.json.JsonDBCollection

class JsonQuery(options: CliOptions, private val queryOptions: QueryOptions) :
    CliCommand(options) {

    override fun execute() {
        if (queryOptions.hasQueryStr()) {
            executeQuery()
        } else {

        }
    }

    private fun executeQuery() {

        val database: Database<JsonResourceManager> = openJsonDatabase(queryOptions.user)
        database.use {

            val manager = database.openResourceManager(queryOptions.resource)
            manager.use {
                val dbCollection = JsonDBCollection(options.location, database)

                dbCollection.use {


                }
            }
        }
    }
}
