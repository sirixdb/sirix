package org.sirix.cli.commands.json

import org.sirix.api.Database
import org.sirix.api.json.JsonResourceManager
import org.sirix.cli.CliOptions
import org.sirix.cli.commands.CliCommand
import org.sirix.cli.commands.QueryCommandOptions
import org.sirix.xquery.json.JsonDBCollection

class JsonQueryCommand(options: CliOptions, private val queryCommandOptions: QueryCommandOptions) :
    CliCommand(options) {

    override fun execute() {
        if (queryCommandOptions.hasQueryStr()) {
            executeQuery()
        } else {

        }
    }

    private fun executeQuery() {

        val database: Database<JsonResourceManager> = openJsonDatabase(queryCommandOptions.user)
        database.use {

            val manager = database.openResourceManager(queryCommandOptions.resource)
            manager.use {
                val dbCollection = JsonDBCollection(options.location, database)

                dbCollection.use {

                    
                }
            }
        }
    }
}